package common;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import join_algorithms.*;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.OrderByElement;
import operator.logical.*;
import operator.physical.*;

/**
 * The PhysicalPlanBuilder class is responsible for converting a logical operator tree into a
 * corresponding physical operator tree. It uses a visitor pattern to traverse the logical operators
 * and, based on their type and the available metadata (indexes, statistics, etc.), produces an
 * optimal or near-optimal physical execution plan.
 *
 * <p>This class:
 *
 * <ul>
 *   <li>Determines which physical operators to use (e.g., table scan vs. index scan).
 *   <li>Chooses join algorithms (e.g., BNLJ or SMJ) based on heuristics or costs.
 *   <li>Introduces sorting operators, projections, selections, etc., as needed.
 *   <li>Manages temporary directories for external operations (e.g., external sort).
 *   <li>Integrates with the DBCatalog for schema and statistics.
 * </ul>
 */
public class PhysicalPlanBuilder implements LogicalOperatorVisitor {

  /** The final physical operator produced after visiting the entire logical plan. */
  private Operator result;

  /** A map from table aliases to their actual table names, used to resolve references. */
  private final Map<String, String> tableAliases;

  /** The temporary directory for external operations such as external sorting or indexing. */
  private final String tempDir;

  /** A singleton instance of the database catalog providing metadata and statistics. */
  private DBCatalog dbCatalog;

  /** The number of buffer pages used by the Block Nested Loop Join (BNLJ). */
  private final int BNLJ_BUFFER_PAGES = 3;

  /** The number of buffer pages used by the External Sort operator. */
  private final int SORT_BUFFER_PAGES = 3;

  /**
   * Constructs a new PhysicalPlanBuilder.
   *
   * @param tableAliases A map of table aliases to their actual table names.
   * @param tempDir A temporary directory path for intermediate files.
   */
  public PhysicalPlanBuilder(Map<String, String> tableAliases, String tempDir) {
    this.tableAliases = tableAliases;
    this.tempDir = tempDir;
    this.dbCatalog = DBCatalog.getInstance();
  }

  /**
   * Visits a LogicalScanOperator, which reads a base table from disk. Converts it into a
   * ScanOperator or potentially an IndexScanOperator if beneficial.
   *
   * @param op The LogicalScanOperator.
   */
  @Override
  public void visit(LogicalScanOperator op) {
    ArrayList<Column> schema = new ArrayList<>(op.getSchema());
    String tableName = op.getTable().getName();
    result = new ScanOperator(schema, tableName);
  }

  /**
   * Visits a LogicalSelectOperator, which applies selection predicates. If an index is available
   * and beneficial, an IndexScanOperator may be used. Otherwise, a basic ScanOperator followed by a
   * SelectOperator is used.
   *
   * @param op The LogicalSelectOperator.
   */
  @Override
  public void visit(LogicalSelectOperator op) {
    if (op.getChildren().get(0) instanceof LogicalScanOperator) {
      LogicalScanOperator scanOp = (LogicalScanOperator) op.getChildren().get(0);
      String tableName = resolveTableName(scanOp.getTable().getName());

      // Attempt to find a suitable index for the selection condition
      SelectionAnalyzer analyzer = findBestIndex(tableName, op.getCondition());

      // If we found a good index-based approach
      if (analyzer != null && analyzer.hasIndexConditions()) {
        String indexColumn = analyzer.getIndexedColumn();
        boolean isClustered = isIndexClustered(tableName, indexColumn);
        result =
            new IndexScanOperator(
                new ArrayList<>(scanOp.getSchema()),
                tableName,
                tempDir + "/" + tableName + "." + indexColumn,
                isClustered,
                analyzer.getLowKey(),
                analyzer.getHighKey(),
                indexColumn);

        // Apply any remaining selection conditions that couldn't be handled by the
        // index
        List<Expression> remainingConditions = analyzer.getRemainingConditions();
        if (!remainingConditions.isEmpty()) {
          Expression remainingExpr = buildAndExpression(remainingConditions);
          result = new SelectOperator(result, remainingExpr, tableAliases);
        }
      } else {
        // No suitable index found, fall back to a regular scan + select
        op.getChildren().get(0).accept(this);
        result = new SelectOperator(result, op.getCondition(), tableAliases);
      }
    }
  }

  /**
   * Visits a LogicalJoinOperator, which combines multiple child operators based on join conditions.
   * Determines join order and join algorithms (BNLJ or SMJ) using heuristics and available
   * statistics.
   *
   * @param op The LogicalJoinOperator.
   */
  @Override
  public void visit(LogicalJoinOperator op) {
    List<LogicalOperator> children = op.getChildren();
    List<Operator> physicalChildren = new ArrayList<>();

    // Convert all child logical operators into physical operators
    for (LogicalOperator child : children) {
      child.accept(this);
      physicalChildren.add(result);
    }

    // Use the union-find and residual conditions to determine join order
    List<Expression> residualConditions = op.getResidualConditions();
    UnionFind unionFind = op.getUnionFind();

    JoinOrderOptimizer optimizer =
        new JoinOrderOptimizer(physicalChildren, residualConditions, unionFind, dbCatalog);

    // Build the final join tree from the optimized join order
    result =
        buildJoinTree(optimizer.getOptimalOrder(), optimizer.getJoinConditions(), physicalChildren);
  }

  /**
   * Visits a LogicalProjectOperator, which implements a projection of certain columns. Converts it
   * into a ProjectOperator.
   *
   * @param op The LogicalProjectOperator.
   */
  @Override
  public void visit(LogicalProjectOperator op) {
    op.getChildren().get(0).accept(this);
    result = new ProjectOperator(result, op.getSchema(), op.getSelectItems());
  }

  /**
   * Visits a LogicalSortOperator, which sorts the output of its child operator. Converts it into an
   * ExternalSort operator.
   *
   * @param op The LogicalSortOperator.
   */
  @Override
  public void visit(LogicalSortOperator op) {
    op.getChildren().get(0).accept(this);
    result =
        new ExternalSort(
            new ArrayList<>(result.getOutputSchema()),
            result,
            op.getOrderByElements(),
            SORT_BUFFER_PAGES,
            tempDir);
  }

  /**
   * Visits a LogicalDuplicateEliminationOperator, which removes duplicate tuples. Converts it into
   * a DuplicateElementEliminationOperator.
   *
   * @param op The LogicalDuplicateEliminationOperator.
   */
  @Override
  public void visit(LogicalDuplicateEliminationOperator op) {
    op.getChildren().get(0).accept(this);
    result =
        new DuplicateElementEliminationOperator(new ArrayList<>(result.getOutputSchema()), result);
  }

  /**
   * Builds a join tree from a given join order and join conditions. Tries to choose appropriate
   * join algorithms (SMJ or BNLJ) based on conditions and data sizes.
   *
   * @param joinOrder The order in which joins should be executed.
   * @param conditions The join conditions corresponding to the join sequence.
   * @param children The physical operators representing each relation in the join.
   * @return The root operator of the constructed join subtree.
   */
  private Operator buildJoinTree(
      List<Integer> joinOrder, List<Expression> conditions, List<Operator> children) {

    System.out.println("=== Join Tree Building Debug Info ===");
    System.out.println("Join order size: " + joinOrder.size());
    System.out.println("Join order: " + joinOrder);
    System.out.println("Children size: " + children.size());
    System.out.println("Conditions size: " + conditions.size());

    // Validate inputs
    if (joinOrder == null || conditions == null || children == null) {
      throw new IllegalArgumentException("Null inputs not allowed in buildJoinTree");
    }
    if (joinOrder.isEmpty()) {
      throw new IllegalArgumentException("Join order cannot be empty");
    }
    if (children.isEmpty()) {
      throw new IllegalArgumentException("Children list cannot be empty");
    }
    if (joinOrder.size() != children.size()) {
      throw new IllegalArgumentException(
          String.format(
              "Join order size (%d) must match children size (%d)",
              joinOrder.size(), children.size()));
    }

    int firstIndex = joinOrder.get(0);
    if (firstIndex >= children.size()) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid first join index: %d for children size: %d", firstIndex, children.size()));
    }

    // Start building the join tree
    Operator current = children.get(joinOrder.get(0));
    System.out.println("Initial operator schema: " + current.getOutputSchema());

    int conditionIndex = 0;
    // Iteratively join the remaining operators
    for (int i = 1; i < joinOrder.size(); i++) {
      System.out.println("Processing join " + i + " of " + (joinOrder.size() - 1));

      int rightIndex = joinOrder.get(i);
      if (rightIndex >= children.size()) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid join index at position %d: %d for children size: %d",
                i, rightIndex, children.size()));
      }

      Operator right = children.get(rightIndex);

      // Identify an applicable join condition
      Expression joinCondition = null;
      while (conditionIndex < conditions.size()) {
        Expression condition = conditions.get(conditionIndex);
        if (isConditionApplicable(condition, current, right)) {
          joinCondition = condition;
          conditionIndex++;
          break;
        }
        conditionIndex++;
      }

      System.out.println("Join condition: " + joinCondition);
      System.out.println("Right operator schema: " + right.getOutputSchema());

      // Decide whether to use Sort-Merge Join or BNLJ
      if (joinCondition != null && shouldUseSortMerge(joinCondition, current, right)) {
        List<Column> leftColumns = new ArrayList<>();
        List<Column> rightColumns = new ArrayList<>();
        extractJoinColumns(joinCondition, leftColumns, rightColumns, current, right);

        current = createSortMergeJoin(current, right, joinCondition, leftColumns, rightColumns);
      } else {
        current = new BNLJ(current, right, joinCondition, tableAliases, BNLJ_BUFFER_PAGES);
      }

      System.out.println("Resulting schema after join " + i + ": " + current.getOutputSchema());
    }

    System.out.println("=== Join Tree Building Complete ===");
    return current;
  }

  /**
   * Determines whether to use Sort-Merge Join (SMJ) based on the join condition and estimated
   * sizes.
   *
   * @param condition The join condition.
   * @param left The left child operator.
   * @param right The right child operator.
   * @return true if SMJ is preferred; false otherwise.
   */
  private boolean shouldUseSortMerge(Expression condition, Operator left, Operator right) {
    if (!isEquiJoinOnly(condition)) {
      return false;
    }

    int leftSize = estimateSize(left);
    int rightSize = estimateSize(right);

    // Heuristic: If both sides are large, SMJ might be beneficial
    if (leftSize > 1000 && rightSize > 1000) {
      return true;
    }

    // Otherwise, prefer BNLJ for small inputs
    if (leftSize < 100 || rightSize < 100) {
      return false;
    }

    return true;
  }

  /**
   * Creates a Sort-Merge Join (SMJ) operator.
   *
   * @param left The left child operator.
   * @param right The right child operator.
   * @param condition The join condition.
   * @param leftColumns The columns from the left side to sort by.
   * @param rightColumns The columns from the right side to sort by.
   * @return An SMJ operator.
   */
  private Operator createSortMergeJoin(
      Operator left,
      Operator right,
      Expression condition,
      List<Column> leftColumns,
      List<Column> rightColumns) {

    List<OrderByElement> leftOrderBy = createOrderByElements(leftColumns);
    List<OrderByElement> rightOrderBy = createOrderByElements(rightColumns);

    Operator sortedLeft =
        new ExternalSort(
            new ArrayList<>(left.getOutputSchema()), left, leftOrderBy, SORT_BUFFER_PAGES, tempDir);

    Operator sortedRight =
        new ExternalSort(
            new ArrayList<>(right.getOutputSchema()),
            right,
            rightOrderBy,
            SORT_BUFFER_PAGES,
            tempDir);

    return new SMJ(sortedLeft, sortedRight, condition, tableAliases, leftColumns, rightColumns);
  }

  /**
   * Creates a list of OrderByElement objects for the given columns.
   *
   * @param columns The columns to order by.
   * @return A list of OrderByElements.
   */
  private List<OrderByElement> createOrderByElements(List<Column> columns) {
    List<OrderByElement> orderByElements = new ArrayList<>();
    for (Column col : columns) {
      OrderByElement orderBy = new OrderByElement();
      orderBy.setExpression(col);
      orderBy.setAsc(true);
      orderByElements.add(orderBy);
    }
    return orderByElements;
  }

  /**
   * Checks if a given join condition is applicable between two operators. A condition is applicable
   * if it references tables present in both operators and not outside them.
   *
   * @param condition The join condition.
   * @param left The left operator.
   * @param right The right operator.
   * @return true if the condition is applicable; false otherwise.
   */
  private boolean isConditionApplicable(Expression condition, Operator left, Operator right) {
    Set<String> leftTables = getTablesFromSchema(left.getOutputSchema());
    Set<String> rightTables = getTablesFromSchema(right.getOutputSchema());
    Set<String> conditionTables = getTablesInCondition(condition);

    boolean usesLeft = false;
    boolean usesRight = false;
    boolean usesOther = false;

    for (String table : conditionTables) {
      if (leftTables.contains(table)) {
        usesLeft = true;
      } else if (rightTables.contains(table)) {
        usesRight = true;
      } else {
        usesOther = true;
      }
    }

    return usesLeft && usesRight && !usesOther;
  }

  /**
   * Extracts table names from a schema.
   *
   * @param schema A list of columns representing the schema.
   * @return A set of table names.
   */
  private Set<String> getTablesFromSchema(List<Column> schema) {
    Set<String> tables = new HashSet<>();
    for (Column col : schema) {
      String tableName = col.getTable().getName();
      if (tableName != null) {
        tables.add(tableName);
      }
      if (col.getTable().getAlias() != null) {
        tables.add(col.getTable().getAlias().getName());
      }
    }
    return tables;
  }

  /**
   * Extracts table names referenced in a condition expression.
   *
   * @param condition The expression representing a condition.
   * @return A set of table names referenced by the condition.
   */
  private Set<String> getTablesInCondition(Expression condition) {
    Set<String> tables = new HashSet<>();

    if (condition instanceof ComparisonOperator) {
      ComparisonOperator comp = (ComparisonOperator) condition;

      if (comp.getLeftExpression() instanceof Column) {
        Column col = (Column) comp.getLeftExpression();
        if (col.getTable() != null) {
          if (col.getTable().getName() != null) {
            tables.add(col.getTable().getName());
          }
          if (col.getTable().getAlias() != null) {
            tables.add(col.getTable().getAlias().getName());
          }
        }
      }

      if (comp.getRightExpression() instanceof Column) {
        Column col = (Column) comp.getRightExpression();
        if (col.getTable() != null) {
          if (col.getTable().getName() != null) {
            tables.add(col.getTable().getName());
          }
          if (col.getTable().getAlias() != null) {
            tables.add(col.getTable().getAlias().getName());
          }
        }
      }
    }

    return tables;
  }

  /**
   * Extracts join columns from a condition for sort-merge join.
   *
   * @param condition The join condition.
   * @param leftColumns The list to store left join columns.
   * @param rightColumns The list to store right join columns.
   * @param leftChild The left operator.
   * @param rightChild The right operator.
   */
  private void extractJoinColumns(
      Expression condition,
      List<Column> leftColumns,
      List<Column> rightColumns,
      Operator leftChild,
      Operator rightChild) {

    JoinConditionAnalyzer analyzer =
        new JoinConditionAnalyzer(
            leftChild.getOutputSchema().get(0).getTable().getName(),
            rightChild.getOutputSchema().get(0).getTable().getName());
    condition.accept(analyzer);
    leftColumns.addAll(analyzer.getLeftSortColumns());
    rightColumns.addAll(analyzer.getRightSortColumns());
  }

  /**
   * Checks if a condition represents only equi-join predicates (i.e., equality comparisons).
   *
   * @param condition The condition to check.
   * @return true if only equi-join; false otherwise.
   */
  private boolean isEquiJoinOnly(Expression condition) {
    JoinConditionAnalyzer analyzer = new JoinConditionAnalyzer("", "");
    condition.accept(analyzer);
    return analyzer.isValidEquiJoin();
  }

  /**
   * Estimates the size (number of tuples) of an operator's output. If it's a base scan, use catalog
   * statistics; else return a rough guess.
   *
   * @param op The operator.
   * @return An estimated tuple count.
   */
  private int estimateSize(Operator op) {
    if (op instanceof ScanOperator) {
      String tableName = op.getOutputSchema().get(0).getTable().getName();
      return dbCatalog.getTableTupleCount(tableName);
    }
    return 1000; // Default estimate
  }

  /**
   * Attempts to find the best index-based approach for the given selection condition on a table.
   *
   * @param tableName The name of the table.
   * @param condition The selection condition.
   * @return A SelectionAnalyzer if an index is beneficial; null otherwise.
   */
  private SelectionAnalyzer findBestIndex(String tableName, Expression condition) {
    SelectionAnalyzer bestAnalyzer = null;
    int bestScore = -1;

    ArrayList<Column> columns = dbCatalog.getColumns(tableName);
    for (Column column : columns) {
      String columnName = column.getColumnName();
      if (hasIndex(tableName, columnName)) {
        SelectionAnalyzer analyzer = new SelectionAnalyzer(tableName, columnName);
        condition.accept(analyzer);

        if (analyzer.hasIndexConditions()) {
          int score = scoreIndexUsage(analyzer, tableName, columnName);
          if (score > bestScore) {
            bestScore = score;
            bestAnalyzer = analyzer;
          }
        }
      }
    }

    return bestAnalyzer;
  }

  /**
   * Assigns a score to how beneficial using a given index is, based on equality constraints, range
   * constraints, and whether the index is clustered.
   *
   * @param analyzer The SelectionAnalyzer with extracted constraints.
   * @param tableName The table name.
   * @param columnName The column name with an index.
   * @return The index usage score.
   */
  private int scoreIndexUsage(SelectionAnalyzer analyzer, String tableName, String columnName) {
    int score = 0;

    // Equality constraint is very beneficial
    if (analyzer.getLowKey() != null && analyzer.getLowKey().equals(analyzer.getHighKey())) {
      score += 3;
    }

    // Range constraints are beneficial too
    if (analyzer.getLowKey() != null) score += 1;
    if (analyzer.getHighKey() != null) score += 1;

    // Clustered indexes are more beneficial
    if (isIndexClustered(tableName, columnName)) {
      score += 2;
    }

    return score;
  }

  /**
   * Checks if an index for a given table and column exists.
   *
   * @param tableName The table name.
   * @param columnName The column name.
   * @return true if the index file exists; false otherwise.
   */
  private boolean hasIndex(String tableName, String columnName) {
    String indexPath = String.format("%s/%s.%s", tempDir, tableName, columnName);
    return new File(indexPath).exists();
  }

  /**
   * Checks if an index is clustered. This is a placeholder implementation that always returns
   * false. In a real system, this might check catalog information or index metadata.
   *
   * @param tableName The table name.
   * @param columnName The column name.
   * @return true if the index is clustered; false otherwise.
   */
  private boolean isIndexClustered(String tableName, String columnName) {
    return false;
  }

  /**
   * Resolves a table name or alias to the actual table name.
   *
   * @param tableNameOrAlias The table name or alias.
   * @return The actual table name.
   */
  private String resolveTableName(String tableNameOrAlias) {
    return tableAliases.getOrDefault(tableNameOrAlias, tableNameOrAlias);
  }

  /**
   * Builds a conjunctive (AND) expression from a list of conditions.
   *
   * @param conditions The list of expressions to AND together.
   * @return A single expression combining all with AND, or null if empty.
   */
  private Expression buildAndExpression(List<Expression> conditions) {
    if (conditions.isEmpty()) {
      return null;
    }

    Expression result = conditions.get(0);
    for (int i = 1; i < conditions.size(); i++) {
      result = new AndExpression(result, conditions.get(i));
    }
    return result;
  }

  /**
   * Combines two schemas (lists of columns) into one.
   *
   * @param leftSchema The left schema.
   * @param rightSchema The right schema.
   * @return A combined schema containing columns from both sides.
   */
  private static ArrayList<Column> combineSchemas(
      ArrayList<Column> leftSchema, ArrayList<Column> rightSchema) {
    ArrayList<Column> combinedSchema = new ArrayList<>();

    // Add columns from left schema
    for (Column col : leftSchema) {
      Table table = new Table();
      table.setName(col.getTable().getName());
      if (col.getTable().getAlias() != null) {
        table.setAlias(new Alias(col.getTable().getAlias().getName()));
      }
      Column newCol = new Column(table, col.getColumnName());
      combinedSchema.add(newCol);
    }

    // Add columns from right schema
    for (Column col : rightSchema) {
      Table table = new Table();
      table.setName(col.getTable().getName());
      if (col.getTable().getAlias() != null) {
        table.setAlias(new Alias(col.getTable().getAlias().getName()));
      }
      Column newCol = new Column(table, col.getColumnName());
      combinedSchema.add(newCol);
    }

    System.out.println(
        "Combined schema: "
            + combinedSchema.stream()
                .map(Column::getFullyQualifiedName)
                .collect(Collectors.joining(", ")));

    return combinedSchema;
  }

  /**
   * Retrieves the resulting physical operator after the plan has been built.
   *
   * @return The resulting physical operator.
   */
  public Operator getResult() {
    return result;
  }
}
