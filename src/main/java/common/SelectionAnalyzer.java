package common;

import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;

public class SelectionAnalyzer extends ExpressionVisitorAdapter {
  private String indexedTable;
  private String indexedColumn;
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
    List<Expression> tempRemaining = new ArrayList<>(remainingConditions);
    Integer tempLow = lowKey;
    Integer tempHigh = highKey;

    andExpression.getLeftExpression().accept(this);

    Integer leftLow = lowKey;
    Integer leftHigh = highKey;
    List<Expression> leftRemaining = new ArrayList<>(remainingConditions);

    lowKey = tempLow;
    highKey = tempHigh;
    remainingConditions = new ArrayList<>(tempRemaining);

    andExpression.getRightExpression().accept(this);

    if (leftLow != null && (lowKey == null || leftLow > lowKey)) {
      lowKey = leftLow;
    }

    if (leftHigh != null && (highKey == null || leftHigh < highKey)) {
      highKey = leftHigh;
    }

    remainingConditions.addAll(leftRemaining);
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

    boolean leftIsIndexed = isIndexedColumn(left);
    boolean rightIsIndexed = isIndexedColumn(right);

    if (leftIsIndexed || rightIsIndexed) {
      Integer value = leftIsIndexed ? getValueFromExpression(right) : getValueFromExpression(left);

      if (value != null) {
        currentExpressionUsesIndex = true;
        boolean effectiveIsLower = leftIsIndexed ? isLower : !isLower;

        if (effectiveIsLower) {
          if (inclusive) {
            if (lowKey == null || value > lowKey) {
              lowKey = value;
            }
          } else {
            if (lowKey == null || value + 1 > lowKey) {
              lowKey = value + 1;
            }
          }
        } else {
          if (inclusive) {
            if (highKey == null || value < highKey) {
              highKey = value;
            }
          } else {
            if (highKey == null || value - 1 < highKey) {
              highKey = value - 1;
            }
          }
        }
        return;
      }
    }
    remainingConditions.add(expr);
  }

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

  private Integer getValueFromExpression(Expression expr) {
    if (expr instanceof LongValue) {
      return (int) ((LongValue) expr).getValue();
    }
    return null;
  }

  public Integer getLowKey() {
    return lowKey;
  }

  public Integer getHighKey() {
    return highKey;
  }

  public String getIndexedColumn() {
    return indexedColumn;
  }

  public String getIndexedTable() {
    return indexedTable;
  }

  public List<Expression> getRemainingConditions() {
    return remainingConditions;
  }

  public boolean hasIndexConditions() {
    return lowKey != null || highKey != null;
  }

  public boolean getCurrentExpressionUsesIndex() {
    return currentExpressionUsesIndex;
  }
}
