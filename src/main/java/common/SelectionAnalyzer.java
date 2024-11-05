package common;

import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;

public class SelectionAnalyzer extends ExpressionVisitorAdapter {
  private String indexedColumn;
  private String indexedTable;
  private Integer lowKey = null;
  private Integer highKey = null;
  private List<Expression> remainingConditions;
  private boolean currentExpressionUsesIndex = false;

  public SelectionAnalyzer(String indexedTable, String indexedColumn) {
    this.indexedTable = indexedTable;
    this.indexedColumn = indexedColumn;
    this.remainingConditions = new ArrayList<>();
  }

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

  private Integer getValueFromExpression(Expression expr) {
    if (expr instanceof LongValue) {
      return (int) ((LongValue) expr).getValue();
    }
    return null;
  }

  @Override
  public void visit(EqualsTo expr) {
    processComparisonOperation(expr, true, true);
  }

  @Override
  public void visit(GreaterThan expr) {
    processComparisonOperation(expr, true, false);
  }

  @Override
  public void visit(GreaterThanEquals expr) {
    processComparisonOperation(expr, true, true);
  }

  @Override
  public void visit(MinorThan expr) {
    processComparisonOperation(expr, false, false);
  }

  @Override
  public void visit(MinorThanEquals expr) {
    processComparisonOperation(expr, false, true);
  }

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

  public Integer getLowKey() {
    return lowKey;
  }

  public Integer getHighKey() {
    return highKey;
  }

  public List<Expression> getRemainingConditions() {
    return remainingConditions;
  }

  public boolean hasIndexConditions() {
    return lowKey != null || highKey != null;
  }
}
