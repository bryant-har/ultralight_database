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

public class PhysicalPlanBuilder implements LogicalOperatorVisitor {
  private Operator result;
  private final Map<String, String> tableAliases;
  private final String tempDir;
  private DBCatalog dbCatalog;
  private final int BNLJ_BUFFER_PAGES = 3;
  private final int SORT_BUFFER_PAGES = 3;

  public PhysicalPlanBuilder(Map<String, String> tableAliases, String tempDir) {
    this.tableAliases = tableAliases;
    this.tempDir = tempDir;
    this.dbCatalog = DBCatalog.getInstance();
  }

  @Override
  public void visit(LogicalScanOperator op) {
    ArrayList<Column> schema = new ArrayList<>(op.getSchema());
    String tableName = op.getTable().getName();
    result = new ScanOperator(schema, tableName);
  }

  @Override
  public void visit(LogicalSelectOperator op) {
    if (op.getChildren().get(0) instanceof LogicalScanOperator) {
      LogicalScanOperator scanOp = (LogicalScanOperator) op.getChildren().get(0);
      String tableName = resolveTableName(scanOp.getTable().getName());

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

        List<Expression> remainingConditions = analyzer.getRemainingConditions();
        if (!remainingConditions.isEmpty()) {
          Expression remainingExpr = buildAndExpression(remainingConditions);
          result = new SelectOperator(result, remainingExpr, tableAliases);
        }
      } else {
        op.getChildren().get(0).accept(this);
        result = new SelectOperator(result, op.getCondition(), tableAliases);
      }
    }
  }

  @Override
  public void visit(LogicalJoinOperator op) {
    List<LogicalOperator> children = op.getChildren();
    List<Operator> physicalChildren = new ArrayList<>();

    for (LogicalOperator child : children) {
      child.accept(this);
      physicalChildren.add(result);
    }

    List<Expression> residualConditions = op.getResidualConditions();
    UnionFind unionFind = op.getUnionFind();

    JoinOrderOptimizer optimizer =
        new JoinOrderOptimizer(physicalChildren, residualConditions, unionFind, dbCatalog);

    result =
        buildJoinTree(optimizer.getOptimalOrder(), optimizer.getJoinConditions(), physicalChildren);
  }

  @Override
  public void visit(LogicalProjectOperator op) {
    op.getChildren().get(0).accept(this);
    result = new ProjectOperator(result, op.getSchema(), op.getSelectItems());
  }

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

  @Override
  public void visit(LogicalDuplicateEliminationOperator op) {
    op.getChildren().get(0).accept(this);
    result =
        new DuplicateElementEliminationOperator(new ArrayList<>(result.getOutputSchema()), result);
  }

  private Operator buildJoinTree(
      List<Integer> joinOrder, List<Expression> conditions, List<Operator> children) {

    System.out.println("=== Join Tree Building Debug Info ===");
    System.out.println("Join order size: " + joinOrder.size());
    System.out.println("Join order: " + joinOrder);
    System.out.println("Children size: " + children.size());
    System.out.println("Conditions size: " + conditions.size());

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

    Operator current = children.get(joinOrder.get(0));
    System.out.println("Initial operator schema: " + current.getOutputSchema());

    int conditionIndex = 0;
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

  private boolean shouldUseSortMerge(Expression condition, Operator left, Operator right) {
    if (!isEquiJoinOnly(condition)) {
      return false;
    }

    int leftSize = estimateSize(left);
    int rightSize = estimateSize(right);

    if (leftSize > 1000 && rightSize > 1000) {
      return true;
    }

    if (leftSize < 100 || rightSize < 100) {
      return false;
    }

    return true;
  }

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

  private boolean isEquiJoinOnly(Expression condition) {
    JoinConditionAnalyzer analyzer = new JoinConditionAnalyzer("", "");
    condition.accept(analyzer);
    return analyzer.isValidEquiJoin();
  }

  private int estimateSize(Operator op) {
    if (op instanceof ScanOperator) {
      String tableName = op.getOutputSchema().get(0).getTable().getName();
      return dbCatalog.getTableTupleCount(tableName);
    }
    return 1000;
  }

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

  private int scoreIndexUsage(SelectionAnalyzer analyzer, String tableName, String columnName) {
    int score = 0;

    if (analyzer.getLowKey() != null && analyzer.getLowKey().equals(analyzer.getHighKey())) {
      score += 3;
    }

    if (analyzer.getLowKey() != null) score += 1;
    if (analyzer.getHighKey() != null) score += 1;

    if (isIndexClustered(tableName, columnName)) {
      score += 2;
    }

    return score;
  }

  private boolean hasIndex(String tableName, String columnName) {
    String indexPath = String.format("%s/%s.%s", tempDir, tableName, columnName);
    return new File(indexPath).exists();
  }

  private boolean isIndexClustered(String tableName, String columnName) {
    return false;
  }

  private String resolveTableName(String tableNameOrAlias) {
    return tableAliases.getOrDefault(tableNameOrAlias, tableNameOrAlias);
  }

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
