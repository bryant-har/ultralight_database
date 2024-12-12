package common;

import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;

/**
 * The SelectionAnalyzer class analyzes a selection condition to determine if and how it can use an
 * index on a particular table and column. It tries to extract low and high keys from the selection
 * conditions to perform index scans. Any conditions that cannot be translated into index-based
 * constraints are collected as remaining conditions.
 *
 * <p>The analysis focuses on extracting integer bounds from comparison operators (e.g., "=", ">",
 * ">=", "<", "<=") involving the indexed column. It keeps track of the most restrictive lower and
 * upper bounds found.
 *
 * <p>Example: If the indexed column is "S.A" and the condition is:
 *
 * <pre>
 *   S.A >= 10 AND S.A < 20 AND R.C = 5
 * </pre>
 *
 * Then the analyzer sets lowKey = 10 and highKey = 19 (assuming non-inclusive "<"), and leaves "R.C
 * = 5" as a remaining condition.
 */
public class SelectionAnalyzer extends ExpressionVisitorAdapter {
  /** The table name on which indexing is considered. */
  private String indexedTable;

  /** The column name on which indexing is considered. */
  private String indexedColumn;

  /** The extracted lower bound key for indexing, or null if none found. */
  private Integer lowKey = null;

  /** The extracted upper bound key for indexing, or null if none found. */
  private Integer highKey = null;

  /** A list of conditions that cannot be translated into index constraints. */
  private List<Expression> remainingConditions;

  /**
   * Indicates if the current expression uses the indexed column (helps in logic for combining AND
   * conditions).
   */
  private boolean currentExpressionUsesIndex = false;

  /**
   * Constructs a SelectionAnalyzer for a specific table and column.
   *
   * @param indexedTable The name of the table with a potential index.
   * @param indexedColumn The name of the column with a potential index.
   */
  public SelectionAnalyzer(String indexedTable, String indexedColumn) {
    this.indexedTable = indexedTable;
    this.indexedColumn = indexedColumn;
    this.remainingConditions = new ArrayList<>();
  }

  /**
   * Visits an AND expression. Splits the analysis into left and right parts, merges their results,
   * and updates the index bounds accordingly.
   *
   * @param andExpression The AND expression to visit.
   */
  @Override
  public void visit(AndExpression andExpression) {
    // Save current state
    List<Expression> tempRemaining = new ArrayList<>(remainingConditions);
    Integer tempLow = lowKey;
    Integer tempHigh = highKey;

    // Process left side
    andExpression.getLeftExpression().accept(this);
    Integer leftLow = lowKey;
    Integer leftHigh = highKey;
    List<Expression> leftRemaining = new ArrayList<>(remainingConditions);

    // Restore state for right side
    lowKey = tempLow;
    highKey = tempHigh;
    remainingConditions = new ArrayList<>(tempRemaining);

    // Process right side
    andExpression.getRightExpression().accept(this);

    // Merge left and right results: pick the more restrictive bounds
    if (leftLow != null && (lowKey == null || leftLow > lowKey)) {
      lowKey = leftLow;
    }
    if (leftHigh != null && (highKey == null || leftHigh < highKey)) {
      highKey = leftHigh;
    }

    // Combine remaining conditions
    remainingConditions.addAll(leftRemaining);
  }

  /**
   * Visits an EqualsTo expression. Equalities can set both lowKey and highKey if on the indexed
   * column.
   *
   * @param expr The equals expression.
   */
  @Override
  public void visit(EqualsTo expr) {
    processComparisonOperation(expr, true, true);
  }

  /**
   * Visits a GreaterThan expression. Sets or updates lowKey.
   *
   * @param expr The greater-than expression.
   */
  @Override
  public void visit(GreaterThan expr) {
    processComparisonOperation(expr, true, false);
  }

  /**
   * Visits a GreaterThanEquals expression. Sets or updates lowKey (inclusive).
   *
   * @param expr The greater-than-or-equals expression.
   */
  @Override
  public void visit(GreaterThanEquals expr) {
    processComparisonOperation(expr, true, true);
  }

  /**
   * Visits a MinorThan (less-than) expression. Sets or updates highKey.
   *
   * @param expr The less-than expression.
   */
  @Override
  public void visit(MinorThan expr) {
    processComparisonOperation(expr, false, false);
  }

  /**
   * Visits a MinorThanEquals expression. Sets or updates highKey (inclusive).
   *
   * @param expr The less-than-or-equals expression.
   */
  @Override
  public void visit(MinorThanEquals expr) {
    processComparisonOperation(expr, false, true);
  }

  /**
   * Processes a comparison operator, extracting integer keys if the operator involves the indexed
   * column.
   *
   * @param expr The comparison expression.
   * @param isLower A boolean indicating if this comparison sets a lower bound (true) or upper bound
   *     (false).
   * @param inclusive Whether this bound is inclusive or exclusive.
   */
  private void processComparisonOperation(
      ComparisonOperator expr, boolean isLower, boolean inclusive) {
    Expression left = expr.getLeftExpression();
    Expression right = expr.getRightExpression();

    boolean leftIsIndexed = isIndexedColumn(left);
    boolean rightIsIndexed = isIndexedColumn(right);

    // Check if this expression involves the indexed column
    if (leftIsIndexed || rightIsIndexed) {
      Integer value = leftIsIndexed ? getValueFromExpression(right) : getValueFromExpression(left);

      if (value != null) {
        currentExpressionUsesIndex = true;
        // If the indexed column is on the left side, isLower applies directly.
        // If on the right side, invert the logic since column op value is reversed.
        boolean effectiveIsLower = leftIsIndexed ? isLower : !isLower;

        if (effectiveIsLower) {
          // Update lowKey
          if (inclusive) {
            if (lowKey == null || value > lowKey) {
              lowKey = value;
            }
          } else {
            // Non-inclusive means next integer greater
            if (lowKey == null || value + 1 > lowKey) {
              lowKey = value + 1;
            }
          }
        } else {
          // Update highKey
          if (inclusive) {
            if (highKey == null || value < highKey) {
              highKey = value;
            }
          } else {
            // Non-inclusive means one less than the value
            if (highKey == null || value - 1 < highKey) {
              highKey = value - 1;
            }
          }
        }
        return;
      }
    }

    // If we cannot derive a key constraint, add the expression to remaining
    // conditions
    remainingConditions.add(expr);
  }

  /**
   * Checks if the given expression is a column reference to the indexed column.
   *
   * @param expr The expression.
   * @return true if the expression references the indexed column; false otherwise.
   */
  private boolean isIndexedColumn(Expression expr) {
    if (expr instanceof Column) {
      Column col = (Column) expr;
      String tableName = col.getTable().getName();
      if (tableName != null && !tableName.equals(indexedTable)) {
        return false;
      }
      return col.getColumnName().equals(indexedColumn);
    }
    return false;
  }

  /**
   * Extracts an integer value from an expression if it's a LongValue.
   *
   * @param expr The expression.
   * @return The integer value if available; null otherwise.
   */
  private Integer getValueFromExpression(Expression expr) {
    if (expr instanceof LongValue) {
      return (int) ((LongValue) expr).getValue();
    }
    return null;
  }

  /**
   * @return The extracted lower key for indexing, or null if none.
   */
  public Integer getLowKey() {
    return lowKey;
  }

  /**
   * @return The extracted upper key for indexing, or null if none.
   */
  public Integer getHighKey() {
    return highKey;
  }

  /**
   * @return The name of the indexed column being analyzed.
   */
  public String getIndexedColumn() {
    return indexedColumn;
  }

  /**
   * @return The name of the indexed table being analyzed.
   */
  public String getIndexedTable() {
    return indexedTable;
  }

  /**
   * @return A list of remaining conditions that cannot be turned into index constraints.
   */
  public List<Expression> getRemainingConditions() {
    return remainingConditions;
  }

  /**
   * Checks if there are any index conditions extracted (lowKey or highKey).
   *
   * @return true if at least one index-bound condition was found; false otherwise.
   */
  public boolean hasIndexConditions() {
    return lowKey != null || highKey != null;
  }

  /**
   * Indicates if the current (most recently visited) expression used the indexed column.
   *
   * @return true if the current expression uses the index; false otherwise.
   */
  public boolean getCurrentExpressionUsesIndex() {
    return currentExpressionUsesIndex;
  }
}
