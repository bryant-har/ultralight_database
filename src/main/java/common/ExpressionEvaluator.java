package common;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.expression.operators.relational.*;

import java.util.List;
import java.util.Map;

public class ExpressionEvaluator extends ExpressionVisitorAdapter {
  private Tuple tuple;
  private boolean resultBool = true;
  private long resultLong = 0;
  private List<Column> schema;
  private Map<String, String> tableAliases;

  public ExpressionEvaluator(Map<String, String> tableAliases) {
    this.tableAliases = tableAliases;
  }

  public boolean evaluate(Expression expr, Tuple tuple, List<Column> schema) {
    this.tuple = tuple;
    this.schema = schema;
    expr.accept(this);
    return resultBool;
  }

  @Override
  public void visit(Column column) {
    String tableNameOrAlias = column.getTable().getName();
    String actualTableName = tableAliases.getOrDefault(tableNameOrAlias, tableNameOrAlias);
    String columnName = column.getColumnName();

    int index = getColumnIndex(actualTableName, columnName);
    if (index != -1) {
      resultLong = tuple.getElementAtIndex(index);
      resultBool = resultLong != 0;
    } else {
      throw new IllegalArgumentException("Column not found: " + column.getFullyQualifiedName());
    }
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

  // Add other comparison methods
  @Override
  public void visit(GreaterThan expr) {
    expr.getLeftExpression().accept(this);
    long lval = this.resultLong;
    expr.getRightExpression().accept(this);
    long rval = this.resultLong;
    resultBool = lval > rval;
  }

  @Override
  public void visit(GreaterThanEquals expr) {
    expr.getLeftExpression().accept(this);
    long lval = this.resultLong;
    expr.getRightExpression().accept(this);
    long rval = this.resultLong;
    resultBool = lval >= rval;
  }

  @Override
  public void visit(MinorThan expr) {
    expr.getLeftExpression().accept(this);
    long lval = this.resultLong;
    expr.getRightExpression().accept(this);
    long rval = this.resultLong;
    resultBool = lval < rval;
  }

  @Override
  public void visit(MinorThanEquals expr) {
    expr.getLeftExpression().accept(this);
    long lval = this.resultLong;
    expr.getRightExpression().accept(this);
    long rval = this.resultLong;
    resultBool = lval <= rval;
  }

  @Override
  public void visit(NotEqualsTo expr) {
    expr.getLeftExpression().accept(this);
    long lval = this.resultLong;
    expr.getRightExpression().accept(this);
    long rval = this.resultLong;
    resultBool = lval != rval;
  }

  public boolean getResult() {
    return resultBool;
  }
}