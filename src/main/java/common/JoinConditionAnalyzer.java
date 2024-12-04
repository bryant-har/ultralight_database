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

/**
 * The JoinConditionAnalyzer class is responsible for analyzing join conditions
 * in a SQL query, specifically handling equi-joins (conditions of the form
 * `table1.column1 = table2.column2`). It extracts the relevant columns from
 * join conditions and validates the equi-join structure.
 */
public class JoinConditionAnalyzer extends ExpressionVisitorAdapter {
  private static final Logger logger = LogManager.getLogger(JoinConditionAnalyzer.class);

  private List<Column> leftColumns; // Stores left-side join columns
  private List<Column> rightColumns; // Stores right-side join columns
  private final String leftTableName; // Name of the left table in the join
  private final String rightTableName; // Name of the right table in the join

  /**
   * Constructs a JoinConditionAnalyzer for a specific pair of tables.
   *
   * @param leftTableName  The name of the left table in the join.
   * @param rightTableName The name of the right table in the join.
   */
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

  /**
   * Visits an AND expression and processes its left and right expressions.
   *
   * @param expr The AND expression to process.
   */
  @Override
  public void visit(AndExpression expr) {
    logger.debug("Processing AND expression");
    expr.getLeftExpression().accept(this); // Process left part of the AND
    expr.getRightExpression().accept(this); // Process right part of the AND
  }

  /**
   * Visits an EQUALS expression and analyzes it for potential join conditions.
   *
   * @param expr The EQUALS expression to analyze.
   */
  @Override
  public void visit(EqualsTo expr) {
    // Skip expressions that are not column-to-column comparisons
    if (!(expr.getLeftExpression() instanceof Column)
        || !(expr.getRightExpression() instanceof Column)) {
      logger.debug("Skipping non-column equality");
      return;
    }

    Column leftCol = (Column) expr.getLeftExpression(); // Left column in the condition
    Column rightCol = (Column) expr.getRightExpression(); // Right column in the condition

    // Log detailed information about the columns involved
    logColumnDetails("Left", leftCol);
    logColumnDetails("Right", rightCol);

    // Normalize table names (use alias if available)
    String leftTable = normalizeTableName(leftCol.getTable());
    String rightTable = normalizeTableName(rightCol.getTable());

    logger.info(
        "Analyzing equality condition: {}.{} = {}.{}",
        leftTable,
        leftCol.getColumnName(),
        rightTable,
        rightCol.getColumnName());

    // Check if the condition matches the left and right tables
    if (isMatchingPair(leftTable, rightTable)) {
      leftColumns.add(leftCol); // Add the left column
      rightColumns.add(rightCol); // Add the right column
      logger.info("Added columns to join condition (normal order)");
    } else if (isMatchingPair(rightTable, leftTable)) {
      // Reverse the order if the tables are swapped
      leftColumns.add(rightCol);
      rightColumns.add(leftCol);
      logger.info("Added columns to join condition (reversed order)");
    } else {
      logger.info("Tables do not match current join pair - likely part of another join condition");
    }
  }

  /**
   * Logs details about a column, including its table and alias.
   *
   * @param side The side of the join ("Left" or "Right").
   * @param col  The column to log.
   */
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

  /**
   * Normalizes a table name, preferring its alias if available.
   *
   * @param table The table to normalize.
   * @return The normalized table name.
   */
  private String normalizeTableName(Table table) {
    if (table == null)
      return ""; // Return an empty string if the table is null
    return table.getAlias() != null ? table.getAlias().getName() : table.getName();
  }

  /**
   * Checks if two table names match the left and right tables for this analyzer.
   *
   * @param table1 The first table name.
   * @param table2 The second table name.
   * @return True if the tables match the left and right tables, false otherwise.
   */
  private boolean isMatchingPair(String table1, String table2) {
    String t1 = table1.toUpperCase(); // Convert to uppercase for case-insensitive comparison
    String t2 = table2.toUpperCase();
    String left = leftTableName.toUpperCase();
    String right = rightTableName.toUpperCase();

    boolean matches = (t1.equals(left) && t2.equals(right));
    logger.debug("Comparing tables: {}={} and {}={} : {}", t1, left, t2, right, matches);
    return matches;
  }

  /**
   * Retrieves the list of left columns involved in the join condition.
   *
   * @return A list of left columns.
   */
  public List<Column> getLeftSortColumns() {
    logger.info("Returning {} left columns", leftColumns.size());
    leftColumns.forEach(
        col -> logger.debug(
            "Left column: {}.{}", normalizeTableName(col.getTable()), col.getColumnName()));
    return leftColumns;
  }

  /**
   * Retrieves the list of right columns involved in the join condition.
   *
   * @return A list of right columns.
   */
  public List<Column> getRightSortColumns() {
    logger.info("Returning {} right columns", rightColumns.size());
    rightColumns.forEach(
        col -> logger.debug(
            "Right column: {}.{}", normalizeTableName(col.getTable()), col.getColumnName()));
    return rightColumns;
  }

  /**
   * Checks if the analyzed join condition is a valid equi-join.
   *
   * @return True if the join condition is valid, false otherwise.
   */
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
