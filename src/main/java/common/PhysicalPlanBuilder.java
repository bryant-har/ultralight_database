package common;

import java.io.File;
import java.util.ArrayList;
import java.util.Map;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import operator.logical.*;
import operator.physical.*;
import java.util.List;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;

/**
 * The PhysicalPlanBuilder class is responsible for converting a logical query
 * plan into a physical
 * query plan. It implements the Visitor pattern to traverse the logical
 * operator tree and create
 * corresponding physical operators.
 */
public class PhysicalPlanBuilder implements LogicalOperatorVisitor {
  private Operator result;
  private Map<String, String> tableAliases;
  private DBCatalog dbCatalog;
  private String tempDir; // Directory containing index files

  /**
   * Constructs a new PhysicalPlanBuilder.
   *
   * @param tableAliases A map of table aliases to their actual table names.
   * @param tempDir      Directory containing index files
   */
  public PhysicalPlanBuilder(Map<String, String> tableAliases, String tempDir) {
    this.tableAliases = tableAliases;
    this.dbCatalog = DBCatalog.getInstance();
    this.tempDir = tempDir;
  }

  /**
   * Gets the index of a column in a table's schema.
   * 
   * @param tableName  The name of the table
   * @param columnName The name of the column
   * @return The index of the column in the table's schema, or -1 if not found
   */
  private int getColumnIndex(String tableName, String columnName) {
    ArrayList<Column> columns = dbCatalog.getColumns(tableName);
    for (int i = 0; i < columns.size(); i++) {
      if (columns.get(i).getColumnName().equals(columnName)) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Checks if an index exists for the given table and column
   */
  private boolean hasIndex(String tableName, String columnName) {
    String indexPath = String.format("%s/%s.%s", tempDir, tableName, columnName);
    return new File(indexPath).exists();
  }

  /**
   * Gets the actual table name from a possible alias
   */
  private String resolveTableName(String tableNameOrAlias) {
    return tableAliases.getOrDefault(tableNameOrAlias, tableNameOrAlias);
  }

  /**
   * Retrieves the result of the most recent visit operation.
   */
  public Operator getResult() {
    return result;
  }

  /**
   * Visits a LogicalScanOperator and creates a corresponding physical operator.
   * May create either a regular ScanOperator or an IndexScanOperator depending on
   * available indexes and query conditions.
   */
  @Override
  public void visit(LogicalScanOperator op) {
    ArrayList<Column> schema = new ArrayList<>(op.getSchema());
    String tableName = op.getTable().getName();

    // For now, create a regular scan operator
    // Index selection will be handled in the SelectOperator visit
    result = new ScanOperator(schema, tableName);
  }

  /**
   * Visits a LogicalSelectOperator and creates a corresponding physical operator.
   * May use an IndexScanOperator if appropriate indexes exist.
   */
  @Override
  public void visit(LogicalSelectOperator op) {
    // First check if we can use an index scan
    if (op.getChildren().get(0) instanceof LogicalScanOperator) {
      LogicalScanOperator scanOp = (LogicalScanOperator) op.getChildren().get(0);
      String tableName = resolveTableName(scanOp.getTable().getName());

      // Analyze the selection condition for potential index usage
      SelectionAnalyzer analyzer = findBestIndex(tableName, op.getCondition());

      if (analyzer != null && analyzer.hasIndexConditions()) {
        // We found an index we can use
        String indexColumn = analyzer.getIndexedColumn();
        boolean isClustered = isIndexClustered(tableName, indexColumn);

        // Create an IndexScanOperator
        result = new IndexScanOperator(
            new ArrayList<>(scanOp.getSchema()),
            tableName,
            tempDir + "/" + tableName + "." + indexColumn,
            isClustered,
            analyzer.getLowKey(),
            analyzer.getHighKey());

        // If there are remaining conditions, add a SelectOperator on top
        List<Expression> remainingConditions = analyzer.getRemainingConditions();
        if (!remainingConditions.isEmpty()) {
          Expression remainingExpr = buildAndExpression(remainingConditions);
          result = new SelectOperator(result, remainingExpr, tableAliases);
        }
        return;
      }
    }

    // If we can't use an index, fall back to regular selection
    op.getChildren().get(0).accept(this);
    Operator child = result;
    result = new SelectOperator(child, op.getCondition(), tableAliases);
  }

  /**
   * Analyzes selection conditions to find the best index to use
   */
  private SelectionAnalyzer findBestIndex(String tableName, Expression condition) {
    SelectionAnalyzer bestAnalyzer = null;
    int bestScore = -1;

    // Get all columns from the schema
    ArrayList<Column> columns = dbCatalog.getColumns(tableName);

    // Check each column that has an index
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
   * Scores how beneficial it would be to use a particular index
   */
  private int scoreIndexUsage(SelectionAnalyzer analyzer, String tableName, String columnName) {
    int score = 0;

    // Prefer equality conditions
    if (analyzer.getLowKey() != null && analyzer.getLowKey().equals(analyzer.getHighKey())) {
      score += 3;
    }

    // Prefer range conditions over unbounded scans
    if (analyzer.getLowKey() != null)
      score += 1;
    if (analyzer.getHighKey() != null)
      score += 1;

    // Prefer clustered indexes
    if (isIndexClustered(tableName, columnName)) {
      score += 2;
    }

    return score;
  }

  /**
   * Checks if an index is clustered
   */
  private boolean isIndexClustered(String tableName, String columnName) {
    // This information should come from index_info.txt
    // For now, returning false as default
    return false;
  }

  /**
   * Combines multiple conditions with AND
   */
  private Expression buildAndExpression(List<Expression> conditions) {
    if (conditions.isEmpty())
      return null;
    if (conditions.size() == 1)
      return conditions.get(0);

    Expression result = conditions.get(0);
    for (int i = 1; i < conditions.size(); i++) {
      result = new AndExpression(result, conditions.get(i));
    }
    return result;
  }

  @Override
  public void visit(LogicalProjectOperator op) {
    op.getChildren().get(0).accept(this);
    Operator child = result;
    result = new ProjectOperator(child, op.getSelectItems());
  }

  @Override
  public void visit(LogicalJoinOperator op) {
    op.getChildren().get(0).accept(this);
    Operator leftChild = result;
    op.getChildren().get(1).accept(this);
    Operator rightChild = result;
    result = new JoinOperator(leftChild, rightChild, op.getCondition(), tableAliases);
  }

  @Override
  public void visit(LogicalSortOperator op) {
    op.getChildren().get(0).accept(this);
    Operator child = result;
    ArrayList<Column> schema = new ArrayList<>(op.getSchema());
    result = new SortOperator(schema, child, op.getOrderByElements());
  }

  @Override
  public void visit(LogicalDuplicateEliminationOperator op) {
    op.getChildren().get(0).accept(this);
    Operator child = result;
    ArrayList<Column> schema = new ArrayList<>(op.getSchema());
    result = new DuplicateElementEliminationOperator(schema, child);
  }
}