package common;

import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;

/**
 * SelectionAnalyzer analyzes SQL selection conditions to determine which parts can be handled using
 * an index and which parts must be processed through regular selection.
 *
 * <p>This class analyzes expressions to: - Identify conditions that can use an index (e.g., column
 * comparisons with constants) - Determine index scan ranges (lowKey and highKey) - Track remaining
 * conditions that must be handled by regular selection
 *
 * <p>The analyzer handles various comparison operations (=, >, >=, <, <=) and combines multiple
 * conditions through AND operations to determine optimal index usage.
 */
public class SelectionAnalyzer extends ExpressionVisitorAdapter {
  /** The table being indexed */
  private String indexedTable;

  /** The column being indexed */
  private String indexedColumn;

  /** Lower bound for index scan, null if unbounded */
  private Integer lowKey = null;

  /** Upper bound for index scan, null if unbounded */
  private Integer highKey = null;

  /** Conditions that cannot be handled by the index */
  private List<Expression> remainingConditions;

  /** Flag indicating if current expression can use the index */
  private boolean currentExpressionUsesIndex = false;

  /**
   * Constructs a SelectionAnalyzer for a specific indexed table and column.
   *
   * @param indexedTable The name of the table containing the indexed column
   * @param indexedColumn The name of the indexed column
   */
  public SelectionAnalyzer(String indexedTable, String indexedColumn) {
    this.indexedTable = indexedTable;
    this.indexedColumn = indexedColumn;
    this.remainingConditions = new ArrayList<>();
  }

  /**
   * Processes AND expressions by combining the results of both operands. For index conditions, this
   * means taking the more restrictive bounds (higher low key and lower high key).
   *
   * @param andExpression The AND expression to analyze
   */
  @Override
  public void visit(AndExpression andExpression) {
    // Store current state
    List<Expression> tempRemaining = new ArrayList<>(remainingConditions);
    Integer tempLow = lowKey;
    Integer tempHigh = highKey;

    // Process left expression
    andExpression.getLeftExpression().accept(this);

    // Store results from left side
    Integer leftLow = lowKey;
    Integer leftHigh = highKey;
    List<Expression> leftRemaining = new ArrayList<>(remainingConditions);

    // Reset state for right expression
    lowKey = tempLow;
    highKey = tempHigh;
    remainingConditions = new ArrayList<>(tempRemaining);

    // Process right expression
    andExpression.getRightExpression().accept(this);

    // Combine results
    if (leftLow != null && lowKey == null) {
      lowKey = leftLow;
    } else if (leftLow != null && lowKey != null) {
      lowKey = Math.max(leftLow, lowKey);
    }

    if (leftHigh != null && highKey == null) {
      highKey = leftHigh;
    } else if (leftHigh != null && highKey != null) {
      highKey = Math.min(leftHigh, highKey);
    }

    remainingConditions.addAll(leftRemaining);
  }

  /**
   * Checks if an expression refers to the indexed column.
   *
   * @param expr The expression to check
   * @return true if the expression refers to the indexed column
   */
  private boolean isIndexedColumn(Expression expr) {
    if (expr instanceof Column) {
      Column col = (Column) expr;
      String tableName = col.getTable().getName();
      if (tableName == null || tableName.equals(indexedTable)) {
        return col.getColumnName().equals(indexedColumn);
      }
    }
    return false;
  }

  /**
   * Extracts an integer value from an expression if possible.
   *
   * @param expr The expression to evaluate
   * @return The integer value, or null if the expression is not a constant integer
   */
  private Integer getValueFromExpression(Expression expr) {
    if (expr instanceof LongValue) {
      return (int) ((LongValue) expr).getValue();
    }
    return null;
  }

  /**
   * Processes equality comparisons.
   *
   * @param expr The equals comparison to analyze
   */
  @Override
  public void visit(EqualsTo expr) {
    processComparisonOperation(expr, true, true);
  }

  /**
   * Processes greater-than comparisons.
   *
   * @param expr The greater-than comparison to analyze
   */
  @Override
  public void visit(GreaterThan expr) {
    processComparisonOperation(expr, true, false);
  }

  /**
   * Processes greater-than-or-equals comparisons.
   *
   * @param expr The greater-than-or-equals comparison to analyze
   */
  @Override
  public void visit(GreaterThanEquals expr) {
    processComparisonOperation(expr, true, true);
  }

  /**
   * Processes less-than comparisons.
   *
   * @param expr The less-than comparison to analyze
   */
  @Override
  public void visit(MinorThan expr) {
    processComparisonOperation(expr, false, false);
  }

  /**
   * Processes less-than-or-equals comparisons.
   *
   * @param expr The less-than-or-equals comparison to analyze
   */
  @Override
  public void visit(MinorThanEquals expr) {
    processComparisonOperation(expr, false, true);
  }

  /**
   * Processes comparison operations to determine if and how they can use the index. Updates lowKey
   * and highKey based on the comparison, or adds the condition to remainingConditions if it cannot
   * use the index.
   *
   * @param expr The comparison operation to process
   * @param isLower true if this establishes a lower bound, false for upper bound
   * @param inclusive true if the comparison is inclusive (>=, <=), false for exclusive (>, <)
   */
  private void processComparisonOperation(
      ComparisonOperator expr, boolean isLower, boolean inclusive) {
    Expression left = expr.getLeftExpression();
    Expression right = expr.getRightExpression();

    // Check if either side uses the indexed column
    if (isIndexedColumn(left)) {
      Integer value = getValueFromExpression(right);
      if (value != null) {
        currentExpressionUsesIndex = true;
        if (isLower) {
          if (lowKey == null || value > lowKey) {
            lowKey = value;
          }
        } else {
          if (highKey == null || value < highKey) {
            highKey = value;
          }
        }
        return;
      }
    } else if (isIndexedColumn(right)) {
      Integer value = getValueFromExpression(left);
      if (value != null) {
        currentExpressionUsesIndex = true;
        if (!isLower) { // Flip the comparison since column is on right
          if (lowKey == null || value > lowKey) {
            lowKey = value;
          }
        } else {
          if (highKey == null || value < highKey) {
            highKey = value;
          }
        }
        return;
      }
    }

    // If we get here, this condition can't use the index
    remainingConditions.add(expr);
  }

  /**
   * Returns the lower bound for index scan.
   *
   * @return The lower bound value, or null if unbounded
   */
  public Integer getLowKey() {
    return lowKey;
  }

  /**
   * Returns the upper bound for index scan.
   *
   * @return The upper bound value, or null if unbounded
   */
  public Integer getHighKey() {
    return highKey;
  }

  /**
   * Returns the name of the indexed column.
   *
   * @return The indexed column name
   */
  public String getIndexedColumn() {
    return indexedColumn;
  }

  /**
   * Returns the name of the indexed table.
   *
   * @return The indexed table name
   */
  public String getIndexedTable() {
    return indexedTable;
  }

  /**
   * Returns the list of conditions that cannot be handled by the index.
   *
   * @return List of remaining conditions requiring regular selection
   */
  public List<Expression> getRemainingConditions() {
    return remainingConditions;
  }

  /**
   * Checks if any conditions can use the index.
   *
   * @return true if there are conditions that can use the index
   */
  public boolean hasIndexConditions() {
    return lowKey != null || highKey != null;
  }

  /**
   * Checks if the current expression being processed can use the index.
   *
   * @return true if the current expression can use the index
   */
  public boolean getCurrentExpressionUsesIndex() {
    return currentExpressionUsesIndex;
  }
}
