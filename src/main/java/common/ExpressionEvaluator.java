package common;

import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;

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
    expr.accept(this); // Visit the expression and evaluate it
    return result;
  }

  @Override
  public void visit(AndExpression andExpression) {
    andExpression.getLeftExpression().accept(this);
    boolean leftResult = result; // Store the result of the left expression
    andExpression.getRightExpression().accept(this); // Evaluate the right expression
    result = leftResult && result; // Combine results using AND logic
  }

  @Override
  public void visit(Column column) {
    String columnName = column.getColumnName();
    String tableAlias = null;

    // Determine if the column is qualified with an alias
    if (column.getTable() != null) {
      tableAlias =
          column.getTable().getAlias() != null
              ? column.getTable().getAlias().getName()
              : column.getTable().getName(); // Use actual table name if alias is absent
    }

    // Get the index of the column in the tuple schema
    int index = getColumnIndex(tableAlias, columnName);
    if (index != -1) {
      tempValue = tuple.getElementAtIndex(index); // Fetch the value from the tuple
    } else {
      throw new IllegalArgumentException("Column not found: " + column.toString());
    }
  }

  @Override
  public void visit(LongValue longValue) {
    tempValue = (int) longValue.getValue(); // Handle long value
  }

  @Override
  public void visit(DoubleValue doubleValue) {
    tempValue = (int) doubleValue.getValue(); // Handle double value (truncated to int)
  }

  // You can add similar methods to handle other types of values (e.g.,
  // StringValue, BooleanValue)
  @Override
  public void visit(StringValue stringValue) {
    // If needed, you can handle String values here
    // tempValue = Integer.parseInt(stringValue.getValue()); // Example of handling
    // string as integer
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

  /**
   * Finds the index of the column in the schema based on the alias and column name.
   *
   * @param tableAlias the alias of the table or null if none
   * @param columnName the name of the column
   * @return the index of the column in the schema, or -1 if not found
   */
  private int getColumnIndex(String tableAlias, String columnName) {
    for (int i = 0; i < schema.size(); i++) {
      Column schemaColumn = schema.get(i);
      String schemaColumnName = schemaColumn.getColumnName();
      String schemaTableAlias =
          schemaColumn.getTable().getAlias() != null
              ? schemaColumn.getTable().getAlias().getName()
              : schemaColumn
                  .getTable()
                  .getName(); // Handle self-joins by checking both alias and real table name

      // Prioritize alias match over table name match
      if (schemaColumnName.equals(columnName)
          && (tableAlias != null && tableAlias.equals(schemaTableAlias))) {
        return i;
      }
    }

    // If no match is found, print an error for debugging
    return -1; // Return -1 if the column was not found
  }
}
