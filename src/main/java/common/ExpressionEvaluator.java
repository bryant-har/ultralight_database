package common;

import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

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
    String columnName = column.getColumnName();
    String tableAlias = null;
    if (column.getTable() != null) {
      Table table = column.getTable();
      tableAlias = table.getAlias() != null ? table.getAlias().getName() : table.getName();
    }

    int index = getColumnIndex(tableAlias, columnName);
    if (index != -1) {
      tempValue = tuple.getElementAtIndex(index);
    } else {
      throw new IllegalArgumentException("Column not found: " + column + " in schema: " + schema);
    }
  }

  private int getColumnIndex(String tableAlias, String columnName) {
    for (int i = 0; i < schema.size(); i++) {
      Column schemaColumn = schema.get(i);
      String schemaColumnName = schemaColumn.getColumnName();
      Table schemaTable = schemaColumn.getTable();
      String schemaTableName = schemaTable.getName();
      String schemaTableAlias =
          schemaTable.getAlias() != null ? schemaTable.getAlias().getName() : schemaTableName;

      if (schemaColumnName.equals(columnName)) {
        if (tableAlias == null || tableAlias.equals(schemaTableAlias)) {
          return i;
        }
      }
    }
    return -1;
  }

  @Override
  public void visit(LongValue longValue) {
    tempValue = (int) longValue.getValue();
  }

  @Override
  public void visit(DoubleValue doubleValue) {
    tempValue = (int) doubleValue.getValue();
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

  static Expression pruneWhereCondition(
      Expression whereCondition, List<String> availableTableNames) {
    if (whereCondition instanceof AndExpression) {
      AndExpression andExpression = (AndExpression) whereCondition;
      Expression prunedRight =
          pruneWhereCondition(andExpression.getRightExpression(), availableTableNames);
      Expression prunedLeft =
          pruneWhereCondition(andExpression.getLeftExpression(), availableTableNames);
      if (prunedRight == null || prunedLeft == null) {
        return (prunedRight == null) ? prunedLeft : prunedRight;
      }
      return new AndExpression(prunedLeft, prunedRight);
    } else if (whereCondition instanceof ComparisonOperator) {
      ComparisonOperator comparisonOperator = (ComparisonOperator) whereCondition;
      Expression prunedLeft =
          pruneWhereCondition(comparisonOperator.getLeftExpression(), availableTableNames);
      Expression prunedRight =
          pruneWhereCondition(comparisonOperator.getRightExpression(), availableTableNames);
      if (prunedLeft == null || prunedRight == null) {
        return null;
      }
    } else if (whereCondition instanceof Column) {
      Column column = (Column) whereCondition;
      Table table = column.getTable();
      String tableName = table.getAlias() != null ? table.getAlias().getName() : table.getName();
      boolean available = availableTableNames.contains(tableName);
      if (!available) {
        return null;
      }
    }
    return whereCondition;
  }
}
