package common;

import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JoinConditionAnalyzer extends ExpressionVisitorAdapter {
  private static final Logger logger = LogManager.getLogger(JoinConditionAnalyzer.class);

  private List<Column> leftColumns;
  private List<Column> rightColumns;
  private final String leftTableName;
  private final String rightTableName;

  public JoinConditionAnalyzer(String leftTableName, String rightTableName) {
    this.leftTableName = leftTableName;
    this.rightTableName = rightTableName;
    this.leftColumns = new ArrayList<>();
    this.rightColumns = new ArrayList<>();

    logger.info(
        "Creating analyzer for left table '{}' and right table '{}'",
        leftTableName,
        rightTableName);
  }

  @Override
  public void visit(AndExpression expr) {
    logger.debug("Processing AND expression");
    expr.getLeftExpression().accept(this);
    expr.getRightExpression().accept(this);
  }

  @Override
  public void visit(EqualsTo expr) {
    if (!(expr.getLeftExpression() instanceof Column)
        || !(expr.getRightExpression() instanceof Column)) {
      logger.debug("Skipping non-column equality");
      return;
    }

    Column leftCol = (Column) expr.getLeftExpression();
    Column rightCol = (Column) expr.getRightExpression();

    // Log full column details
    logColumnDetails("Left", leftCol);
    logColumnDetails("Right", rightCol);

    // Get normalized table names
    String leftTable = normalizeTableName(leftCol.getTable());
    String rightTable = normalizeTableName(rightCol.getTable());

    logger.info(
        "Analyzing equality condition: {}.{} = {}.{}",
        leftTable,
        leftCol.getColumnName(),
        rightTable,
        rightCol.getColumnName());

    if (isMatchingPair(leftTable, rightTable)) {
      leftColumns.add(leftCol);
      rightColumns.add(rightCol);
      logger.info("Added columns to join condition (normal order)");
    } else if (isMatchingPair(rightTable, leftTable)) {
      leftColumns.add(rightCol);
      rightColumns.add(leftCol);
      logger.info("Added columns to join condition (reversed order)");
    } else {
      logger.info("Tables do not match current join pair - likely part of another join condition");
    }
  }

  private void logColumnDetails(String side, Column col) {
    Table table = col.getTable();
    String tableName = table != null ? table.getName() : "null";
    String alias = table != null && table.getAlias() != null ? table.getAlias().getName() : "null";
    logger.debug(
        "{} column details - Table: {}, Alias: {}, Column: {}",
        side,
        tableName,
        alias,
        col.getColumnName());
  }

  private String normalizeTableName(Table table) {
    if (table == null) return "";
    return table.getAlias() != null ? table.getAlias().getName() : table.getName();
  }

  private boolean isMatchingPair(String table1, String table2) {
    // Convert to uppercase for case-insensitive comparison
    String t1 = table1.toUpperCase();
    String t2 = table2.toUpperCase();
    String left = leftTableName.toUpperCase();
    String right = rightTableName.toUpperCase();

    boolean matches = (t1.equals(left) && t2.equals(right));
    logger.debug("Comparing tables: {}={} and {}={} : {}", t1, left, t2, right, matches);
    return matches;
  }

  public List<Column> getLeftSortColumns() {
    logger.info("Returning {} left columns", leftColumns.size());
    leftColumns.forEach(
        col ->
            logger.debug(
                "Left column: {}.{}", normalizeTableName(col.getTable()), col.getColumnName()));
    return leftColumns;
  }

  public List<Column> getRightSortColumns() {
    logger.info("Returning {} right columns", rightColumns.size());
    rightColumns.forEach(
        col ->
            logger.debug(
                "Right column: {}.{}", normalizeTableName(col.getTable()), col.getColumnName()));
    return rightColumns;
  }

  public boolean isValidEquiJoin() {
    boolean isValid = !leftColumns.isEmpty() && leftColumns.size() == rightColumns.size();
    logger.info(
        "Checking equijoin validity: {} left columns, {} right columns, valid: {}",
        leftColumns.size(),
        rightColumns.size(),
        isValid);
    return isValid;
  }
}
