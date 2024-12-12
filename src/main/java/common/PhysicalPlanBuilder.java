package common;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import join_algorithms.BNLJ;
import join_algorithms.SMJ;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import operator.logical.*;
import operator.physical.*;

/**
 * The PhysicalPlanBuilder class is responsible for constructing a physical
 * query execution plan
 * from a logical query plan. It translates logical operators into appropriate
 * physical operators,
 * considering optimization strategies like index usage, sort-merge joins, and
 * block nested-loop
 * joins.
 */
public class PhysicalPlanBuilder implements LogicalOperatorVisitor {
  private Operator result; // The resulting physical operator
  private final Map<String, String> tableAliases; // Map of table aliases to actual table names
  private final String tempDir; // Path to the directory for temporary files
  private DBCatalog dbCatalog; // Database catalog for accessing metadata

  // Configuration constants
  private final int BNLJ_BUFFER_PAGES = 3; // Number of buffer pages for block nested-loop join
  private final int SORT_BUFFER_PAGES = 3; // Number of buffer pages for external sort

  /**
   * Constructor for the PhysicalPlanBuilder.
   *
   * @param tableAliases Map of table aliases to table names.
   * @param tempDir      Directory path for temporary files.
   */
  public PhysicalPlanBuilder(Map<String, String> tableAliases, String tempDir) {
    this.tableAliases = tableAliases;
    this.tempDir = tempDir;
    this.dbCatalog = DBCatalog.getInstance();
  }

  /**
   * Processes a LogicalScanOperator and converts it into a physical ScanOperator.
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
   * Processes a LogicalSelectOperator and converts it into a SelectOperator. If an index is
   * available, an IndexScanOperator is used.
   *
   * @param op The LogicalSelectOperator.
   */
  @Override
  public void visit(LogicalSelectOperator op) {
    if (op.getChildren().get(0) instanceof LogicalScanOperator) {
      LogicalScanOperator scanOp = (LogicalScanOperator) op.getChildren().get(0);
      String tableName = resolveTableName(scanOp.getTable().getName());

      // Analyze the selection condition to find the best index
      SelectionAnalyzer analyzer = findBestIndex(tableName, op.getCondition());

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

        // Apply any remaining conditions not handled by the index
        List<Expression> remainingConditions = analyzer.getRemainingConditions();
        if (!remainingConditions.isEmpty()) {
          Expression remainingExpr = buildAndExpression(remainingConditions);
          result = new SelectOperator(result, remainingExpr, tableAliases);
        }

    // Default case: Create a SelectOperator without index usage
    op.getChildren().get(0).accept(this);
    result = new SelectOperator(result, op.getCondition(), tableAliases);
  }

  /**
   * Processes a LogicalJoinOperator and converts it into a tree of join
   * operators.
   *
   * @param op The LogicalJoinOperator.
   */
  @Override
  public void visit(LogicalJoinOperator op) {
    List<LogicalOperator> children = op.getChildren();
    List<Operator> physicalChildren = new ArrayList<>();

    // Convert all logical children into physical operators
    for (LogicalOperator child : children) {
      child.accept(this);
      physicalChildren.add(result);
    }

    List<Expression> residualConditions = op.getResidualConditions();
    UnionFind unionFind = op.getUnionFind();

    // Optimize the join order and build the join tree
    JoinOrderOptimizer optimizer = new JoinOrderOptimizer(physicalChildren, residualConditions, unionFind, dbCatalog);

    result = buildJoinTree(optimizer.getOptimalOrder(), optimizer.getJoinConditions(), physicalChildren);
  }

  /**
   * Processes a LogicalProjectOperator and converts it into a ProjectOperator.
   *
   * @param op The LogicalProjectOperator.
   */
  @Override
  public void visit(LogicalProjectOperator op) {
    op.getChildren().get(0).accept(this);
    result = new ProjectOperator(result, op.getSchema(), op.getSelectItems());
  }

  /**
   * Processes a LogicalSortOperator and converts it into an ExternalSort
   * operator.
   *
   * @param op The LogicalSortOperator.
   */
  @Override
  public void visit(LogicalSortOperator op) {
    op.getChildren().get(0).accept(this);
    result = new ExternalSort(
        new ArrayList<>(result.getOutputSchema()),
        result,
        op.getOrderByElements(),
        SORT_BUFFER_PAGES,
        tempDir);
  }

  /**
   * Processes a LogicalDuplicateEliminationOperator and converts it into a
   * DuplicateElementEliminationOperator.
   *
   * @param op The LogicalDuplicateEliminationOperator.
   */
  @Override
  public void visit(LogicalDuplicateEliminationOperator op) {
    op.getChildren().get(0).accept(this);
    result = new DuplicateElementEliminationOperator(new ArrayList<>(result.getOutputSchema()), result);
  }

  /**
   * Builds a tree of join operators based on the specified join order and
   * conditions.
   *
   * @param joinOrder  List of indices representing the join order.
   * @param conditions List of join conditions.
   * @param children   List of physical child operators.
   * @return The root of the join tree.
   */
  private Operator buildJoinTree(
      List<Integer> joinOrder, List<Expression> conditions, List<Operator> children) {
    Operator current = children.get(joinOrder.get(0));

    for (int i = 1; i < joinOrder.size(); i++) {
      Operator right = children.get(joinOrder.get(i));
      Expression condition = conditions.get(i - 1);

      // Use sort-merge join for equi-joins with large datasets
      if (shouldUseSortMerge(condition, current, right)) {
        List<Column> leftColumns = new ArrayList<>();
        List<Column> rightColumns = new ArrayList<>();
        extractJoinColumns(condition, leftColumns, rightColumns, current, right);

        // Sort both sides before performing the join
        List<OrderByElement> leftOrderBy = createOrderByElements(leftColumns);
        List<OrderByElement> rightOrderBy = createOrderByElements(rightColumns);

        Operator sortedLeft = new ExternalSort(
            new ArrayList<>(current.getOutputSchema()),
            current,
            leftOrderBy,
            SORT_BUFFER_PAGES,
            tempDir);

        Operator sortedRight = new ExternalSort(
            new ArrayList<>(right.getOutputSchema()),
            right,
            rightOrderBy,
            SORT_BUFFER_PAGES,
            tempDir);

        current = new SMJ(sortedLeft, sortedRight, condition, tableAliases, leftColumns, rightColumns);
      } else {
        // Use block nested-loop join for smaller datasets
        current = new BNLJ(current, right, condition, tableAliases, BNLJ_BUFFER_PAGES);
      }
    }

    return current;
  }

  /**
   * Extracts join columns for sort-merge join from the join condition.
   *
   * @param condition    The join condition.
   * @param leftColumns  List to store left-side join columns.
   * @param rightColumns List to store right-side join columns.
   * @param leftChild    The left child operator.
   * @param rightChild   The right child operator.
   */
  private void extractJoinColumns(
      Expression condition,
      List<Column> leftColumns,
      List<Column> rightColumns,
      Operator leftChild,
      Operator rightChild) {
    JoinConditionAnalyzer analyzer = new JoinConditionAnalyzer(
        leftChild.getOutputSchema().get(0).getTable().getName(),
        rightChild.getOutputSchema().get(0).getTable().getName());
    condition.accept(analyzer);
    leftColumns.addAll(analyzer.getLeftSortColumns());
    rightColumns.addAll(analyzer.getRightSortColumns());
  }

  /**
   * Creates a list of OrderByElements from a list of columns.
   *
   * @param columns The columns for which to create OrderByElements.
   * @return The list of OrderByElements.
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
   * Determines whether to use sort-merge join based on the join condition and
   * dataset sizes.
   *
   * @param condition The join condition.
   * @param left      The left operator.
   * @param right     The right operator.
   * @return True if sort-merge join should be used, false otherwise.
   */
  private boolean shouldUseSortMerge(Expression condition, Operator left, Operator right) {
    if (!isEquiJoinOnly(condition)) {
      return false;
    }

    int leftSize = estimateSize(left);
    int rightSize = estimateSize(right);

    return leftSize > 1000 && rightSize > 1000;
  }

  /**
   * Checks whether a join condition is an equi-join.
   *
   * @param condition The join condition.
   * @return True if the condition is an equi-join, false otherwise.
   */
  private boolean isEquiJoinOnly(Expression condition) {
    JoinConditionAnalyzer analyzer = new JoinConditionAnalyzer("", "");
    condition.accept(analyzer);
    return analyzer.isValidEquiJoin();
  }

  /**
   * Estimates the size of a dataset represented by an operator.
   *
   * @param op The operator.
   * @return The estimated size of the dataset.
   */
  private int estimateSize(Operator op) {
    if (op instanceof ScanOperator) {
      String tableName = op.getOutputSchema().get(0).getTable().getName();
      return dbCatalog.getTableTupleCount(tableName);
    }
    return 1000; // Default size estimate
  }

  /**
   * Finds the best index for a selection condition on a table.
   *
   * @param tableName The table name.
   * @param condition The selection condition.
   * @return The best SelectionAnalyzer, or null if no suitable index is found.
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
   * Scores the usage of an index based on its characteristics.
   *
   * @param analyzer   The SelectionAnalyzer for the index.
   * @param tableName  The table name.
   * @param columnName The column name.
   * @return The score for the index.
   */
  private int scoreIndexUsage(SelectionAnalyzer analyzer, String tableName, String columnName) {
    int score = 0;

    if (analyzer.getLowKey() != null && analyzer.getLowKey().equals(analyzer.getHighKey())) {
      score += 3; // Equality condition
    }

    if (analyzer.getLowKey() != null)
      score += 1; // Lower bound exists
    if (analyzer.getHighKey() != null)
      score += 1; // Upper bound exists

    if (isIndexClustered(tableName, columnName)) {
      score += 2; // Prefer clustered indexes
    }

    return score;
  }

  /**
   * Checks if a table has an index on a specific column.
   *
   * @param tableName  The table name.
   * @param columnName The column name.
   * @return True if an index exists, false otherwise.
   */
  private boolean hasIndex(String tableName, String columnName) {
    String indexPath = String.format("%s/%s.%s", tempDir, tableName, columnName);
    return new File(indexPath).exists();
  }

  /**
   * Determines if an index is clustered.
   *
   * @param tableName  The table name.
   * @param columnName The column name.
   * @return True if the index is clustered, false otherwise.
   */
  private boolean isIndexClustered(String tableName, String columnName) {
    // For now, returning false as default. Actual check can be implemented later.
    return false;
  }

  /**
   * Resolves a table name or alias to its actual table name.
   *
   * @param tableNameOrAlias The table name or alias.
   * @return The resolved table name.
   */
  private String resolveTableName(String tableNameOrAlias) {
    return tableAliases.getOrDefault(tableNameOrAlias, tableNameOrAlias);
  }

  /**
   * Combines a list of conditions using AND.
   *
   * @param conditions The list of conditions.
   * @return The combined condition.
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
   * Retrieves the resulting physical operator after the plan has been built.
   *
   * @return The resulting physical operator.
   */
  public Operator getResult() {
    return result;
  }
}
