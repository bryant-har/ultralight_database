package common;

import join_algorithms.BNLJ;
import join_algorithms.SMJ;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.OrderByElement;
import operator.logical.*;
import operator.physical.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PhysicalPlanBuilder implements LogicalOperatorVisitor {
  private Operator result;
  private final Map<String, String> tableAliases;
  private final String tempDir;
  private DBCatalog dbCatalog;

  // Configuration values
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

        result = new IndexScanOperator(
            new ArrayList<>(scanOp.getSchema()),
            tableName,
            tempDir + "/" + tableName + "." + indexColumn,
            isClustered,
            analyzer.getLowKey(),
            analyzer.getHighKey());

        List<Expression> remainingConditions = analyzer.getRemainingConditions();
        if (!remainingConditions.isEmpty()) {
          Expression remainingExpr = buildAndExpression(remainingConditions);
          result = new SelectOperator(result, remainingExpr, tableAliases);
        }
        return;
      }
    }

    op.getChildren().get(0).accept(this);
    result = new SelectOperator(result, op.getCondition(), tableAliases);
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

    JoinOrderOptimizer optimizer = new JoinOrderOptimizer(
        physicalChildren,
        residualConditions,
        unionFind,
        dbCatalog);

    result = buildJoinTree(optimizer.getOptimalOrder(),
        optimizer.getJoinConditions(),
        physicalChildren);
  }

  @Override
  public void visit(LogicalProjectOperator op) {
    op.getChildren().get(0).accept(this);
    result = new ProjectOperator(result, op.getSelectItems());
  }

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

  @Override
  public void visit(LogicalDuplicateEliminationOperator op) {
    op.getChildren().get(0).accept(this);
    result = new DuplicateElementEliminationOperator(
        new ArrayList<>(result.getOutputSchema()),
        result);
  }

  private Operator buildJoinTree(List<Integer> joinOrder,
      List<Expression> conditions,
      List<Operator> children) {
    Operator current = children.get(joinOrder.get(0));

    for (int i = 1; i < joinOrder.size(); i++) {
      Operator right = children.get(joinOrder.get(i));
      Expression condition = conditions.get(i - 1);

      if (shouldUseSortMerge(condition, current, right)) {
        List<Column> leftColumns = new ArrayList<>();
        List<Column> rightColumns = new ArrayList<>();
        extractJoinColumns(condition, leftColumns, rightColumns, current, right);

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

        current = new SMJ(sortedLeft, sortedRight, condition,
            tableAliases, leftColumns, rightColumns);
      } else {
        current = new BNLJ(current, right, condition,
            tableAliases, BNLJ_BUFFER_PAGES);
      }
    }

    return current;
  }

  private void extractJoinColumns(Expression condition,
      List<Column> leftColumns,
      List<Column> rightColumns,
      Operator leftChild,
      Operator rightChild) {
    JoinConditionAnalyzer analyzer = new JoinConditionAnalyzer(
        leftChild.getOutputSchema().get(0).getTable().getName(),
        rightChild.getOutputSchema().get(0).getTable().getName());
    condition.accept(analyzer);
    leftColumns.addAll(analyzer.getLeftSortColumns()); // Changed from getLeftColumns()
    rightColumns.addAll(analyzer.getRightSortColumns()); // Changed from getRightColumns()
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

  private boolean shouldUseSortMerge(Expression condition,
      Operator left,
      Operator right) {
    if (!isEquiJoinOnly(condition)) {
      return false;
    }

    int leftSize = estimateSize(left);
    int rightSize = estimateSize(right);

    return leftSize > 1000 && rightSize > 1000;
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

  private boolean hasIndex(String tableName, String columnName) {
    String indexPath = String.format("%s/%s.%s", tempDir, tableName, columnName);
    return new File(indexPath).exists();
  }

  private boolean isIndexClustered(String tableName, String columnName) {
    // For now returning false as default - would need to check index_info.txt
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

  public Operator getResult() {
    return result;
  }
}