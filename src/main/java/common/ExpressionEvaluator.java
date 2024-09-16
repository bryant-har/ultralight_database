package common;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import java.util.List;
import java.util.Map;

public class ExpressionEvaluator extends ExpressionVisitorAdapter {
  private Tuple tuple;
  private boolean result;
  private int tempValue;
  private List<Column> schema;
  private Map<String, String> tableAliases;

  public ExpressionEvaluator(Map<String, String> tableAliases) {
    this.tableAliases = tableAliases;
  }

  public boolean evaluate(Expression expr, Tuple tuple, List<Column> schema) {
    this.tuple = tuple;
    this.schema = schema;
    expr.accept(this);
    return result;
  }

  @Override
  public void visit(AndExpression andExpression) {
    andExpression.getLeftExpression().accept(this);
    boolean leftResult = result;
    andExpression.getRightExpression().accept(this);
    result = leftResult && result;
  }

  @Override
  public void visit(Column column) {
    String tableName = column.getTable().getName();
    String actualTableName = tableAliases.getOrDefault(tableName, tableName);
    String columnName = column.getColumnName();

    int index = getColumnIndex(actualTableName, columnName);
    if (index != -1) {
      tempValue = tuple.getElementAtIndex(index);
    } else {
      throw new IllegalArgumentException("Column not found: " + column.getFullyQualifiedName());
    }
  }

  @Override
  public void visit(LongValue longValue) {
    tempValue = (int) longValue.getValue();
  }

  @Override
  public void visit(EqualsTo equalsTo) {
    equalsTo.getLeftExpression().accept(this);
    int left = tempValue;
    equalsTo.getRightExpression().accept(this);
    result = left == tempValue;
  }

  @Override
  public void visit(NotEqualsTo notEqualsTo) {
    notEqualsTo.getLeftExpression().accept(this);
    int left = tempValue;
    notEqualsTo.getRightExpression().accept(this);
    result = left != tempValue;
  }

  @Override
  public void visit(GreaterThan greaterThan) {
    greaterThan.getLeftExpression().accept(this);
    int left = tempValue;
    greaterThan.getRightExpression().accept(this);
    result = left > tempValue;
  }

  @Override
  public void visit(GreaterThanEquals greaterThanEquals) {
    greaterThanEquals.getLeftExpression().accept(this);
    int left = tempValue;
    greaterThanEquals.getRightExpression().accept(this);
    result = left >= tempValue;
  }

  @Override
  public void visit(MinorThan minorThan) {
    minorThan.getLeftExpression().accept(this);
    int left = tempValue;
    minorThan.getRightExpression().accept(this);
    result = left < tempValue;
  }

  @Override
  public void visit(MinorThanEquals minorThanEquals) {
    minorThanEquals.getLeftExpression().accept(this);
    int left = tempValue;
    minorThanEquals.getRightExpression().accept(this);
    result = left <= tempValue;
  }

  private int getColumnIndex(String tableName, String columnName) {
    for (int i = 0; i < schema.size(); i++) {
      Column schemaColumn = schema.get(i);
      if (schemaColumn.getColumnName().equals(columnName) &&
          (tableName == null || tableName.equals(schemaColumn.getTable().getName()))) {
        return i;
      }
    }
    return -1;
  }
}