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
    System.out.println("Created ExpressionEvaluator with aliases: " + tableAliases);
  }

  public boolean evaluate(Expression expr, Tuple tuple, List<Column> schema) {
    this.tuple = tuple;
    this.schema = schema;
    System.out.println("Evaluating expression: " + expr + " on tuple: " + tuple);
    System.out.println("Using schema: " + schema.stream()
        .map(col -> col.getTable().getName() + "." + col.getColumnName())
        .reduce("", (a, b) -> a + ", " + b));

    expr.accept(this);
    System.out.println("Expression evaluation result: " + result);
    return result;
  }

  @Override
  public void visit(AndExpression andExpression) {
    System.out.println("Evaluating AND expression: " + andExpression);
    andExpression.getLeftExpression().accept(this);
    boolean leftResult = result;
    andExpression.getRightExpression().accept(this);
    result = leftResult && result;
    System.out.println("AND result: " + leftResult + " && " + result + " = " + (leftResult && result));
  }

  @Override
  public void visit(Column column) {
    String columnName = column.getColumnName();
    String tableAlias = null;

    if (column.getTable() != null) {
      Table table = column.getTable();
      tableAlias = table.getAlias() != null ? table.getAlias().getName() : table.getName();
      System.out.println("Looking up column: " + columnName + " with alias/table: " + tableAlias);
    } else {
      System.out.println("Looking up unqualified column: " + columnName);
    }

    int index = getColumnIndex(tableAlias, columnName);
    if (index != -1) {
      tempValue = tuple.getElementAtIndex(index);
      System.out.println("Found column " + columnName + " at index " + index + " with value " + tempValue);
    } else {
      throw new IllegalArgumentException("Column not found: " + column + " in schema: " + schema);
    }
  }

  private int getColumnIndex(String tableAlias, String columnName) {
    System.out.println("Searching for column " + columnName + " with alias " + tableAlias);
    System.out.println("Current schema size: " + schema.size());

    for (int i = 0; i < schema.size(); i++) {
      Column schemaColumn = schema.get(i);
      String schemaColumnName = schemaColumn.getColumnName();
      Table schemaTable = schemaColumn.getTable();
      String schemaTableName = schemaTable.getName();
      String schemaTableAlias = schemaTable.getAlias() != null ? schemaTable.getAlias().getName() : schemaTableName;

      System.out.println("Checking schema column: " + schemaTableName + "." + schemaColumnName +
          " (alias: " + schemaTableAlias + ")");

      if (schemaColumnName.equals(columnName)) {
        if (tableAlias == null || tableAlias.equals(schemaTableAlias)) {
          System.out.println("Found match at index " + i);
          return i;
        }
      }
    }

    System.out.println("No matching column found");
    return -1;
  }

  @Override
  public void visit(LongValue longValue) {
    tempValue = (int) longValue.getValue();
    System.out.println("Processing long value: " + tempValue);
  }

  @Override
  public void visit(DoubleValue doubleValue) {
    tempValue = (int) doubleValue.getValue();
    System.out.println("Processing double value: " + tempValue);
  }

  @Override
  public void visit(EqualsTo equalsTo) {
    System.out.println("Evaluating equals expression");
    equalsTo.getLeftExpression().accept(this);
    int left = tempValue;
    equalsTo.getRightExpression().accept(this);
    result = left == tempValue;
    System.out.println("Equals comparison: " + left + " == " + tempValue + " = " + result);
  }

  @Override
  public void visit(NotEqualsTo notEqualsTo) {
    System.out.println("Evaluating not equals expression");
    notEqualsTo.getLeftExpression().accept(this);
    int left = tempValue;
    notEqualsTo.getRightExpression().accept(this);
    result = left != tempValue;
    System.out.println("Not equals comparison: " + left + " != " + tempValue + " = " + result);
  }

  @Override
  public void visit(GreaterThan greaterThan) {
    System.out.println("Evaluating greater than expression");
    greaterThan.getLeftExpression().accept(this);
    int left = tempValue;
    greaterThan.getRightExpression().accept(this);
    result = left > tempValue;
    System.out.println("Greater than comparison: " + left + " > " + tempValue + " = " + result);
  }

  @Override
  public void visit(GreaterThanEquals greaterThanEquals) {
    System.out.println("Evaluating greater than equals expression");
    greaterThanEquals.getLeftExpression().accept(this);
    int left = tempValue;
    greaterThanEquals.getRightExpression().accept(this);
    result = left >= tempValue;
    System.out.println("Greater than equals comparison: " + left + " >= " + tempValue + " = " + result);
  }

  @Override
  public void visit(MinorThan minorThan) {
    System.out.println("Evaluating less than expression");
    minorThan.getLeftExpression().accept(this);
    int left = tempValue;
    minorThan.getRightExpression().accept(this);
    result = left < tempValue;
    System.out.println("Less than comparison: " + left + " < " + tempValue + " = " + result);
  }

  @Override
  public void visit(MinorThanEquals minorThanEquals) {
    System.out.println("Evaluating less than equals expression");
    minorThanEquals.getLeftExpression().accept(this);
    int left = tempValue;
    minorThanEquals.getRightExpression().accept(this);
    result = left <= tempValue;
    System.out.println("Less than equals comparison: " + left + " <= " + tempValue + " = " + result);
  }

  static Expression pruneWhereCondition(Expression whereCondition, List<String> availableTableNames) {
    System.out.println("Pruning condition: " + whereCondition +
        " with available tables: " + availableTableNames);

    if (whereCondition instanceof AndExpression) {
      AndExpression andExpression = (AndExpression) whereCondition;
      Expression prunedRight = pruneWhereCondition(andExpression.getRightExpression(), availableTableNames);
      Expression prunedLeft = pruneWhereCondition(andExpression.getLeftExpression(), availableTableNames);
      if (prunedRight == null || prunedLeft == null) {
        Expression result = (prunedRight == null) ? prunedLeft : prunedRight;
        System.out.println("Pruned AND expression to: " + result);
        return result;
      }
      return new AndExpression(prunedLeft, prunedRight);
    } else if (whereCondition instanceof ComparisonOperator) {
      ComparisonOperator comparisonOperator = (ComparisonOperator) whereCondition;
      Expression prunedLeft = pruneWhereCondition(comparisonOperator.getLeftExpression(), availableTableNames);
      Expression prunedRight = pruneWhereCondition(comparisonOperator.getRightExpression(), availableTableNames);
      if (prunedLeft == null || prunedRight == null) {
        System.out.println("Pruned comparison to null - one side references unavailable table");
        return null;
      }
    } else if (whereCondition instanceof Column) {
      Column column = (Column) whereCondition;
      Table table = column.getTable();
      String tableName = table.getAlias() != null ? table.getAlias().getName() : table.getName();
      boolean available = availableTableNames.contains(tableName);
      System.out.println("Checking column " + column + " with table " + tableName +
          " - available: " + available);
      if (!available) {
        return null;
      }
    }

    System.out.println("Keeping condition unchanged: " + whereCondition);
    return whereCondition;
  }
}