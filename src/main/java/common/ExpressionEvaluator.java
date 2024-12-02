package common;

import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

/**
 * ExpressionEvaluator handles the evaluation of SQL expressions within the query processing system.
 * It extends ExpressionVisitorAdapter to visit and evaluate different types of SQL expressions. The
 * evaluator supports basic comparison operations, logical AND operations, and handles column
 * references with proper table alias resolution.
 *
 * <p>This class is primarily used for evaluating WHERE clause conditions and JOIN conditions in the
 * context of query execution.
 */
public class ExpressionEvaluator extends ExpressionVisitorAdapter {
  /** Current tuple being evaluated */
  private Tuple tuple;

  /** Result of the current expression evaluation */
  private boolean result;

  /** Temporary storage for intermediate numeric values */
  private int tempValue;

  /** Schema of the relation being processed */
  private List<Column> schema;

  /** Mapping of table aliases to actual table names */
  private Map<String, String> tableAliases;

  /**
   * Prunes a WHERE condition based on available tables. Removes conditions that reference tables
   * not available in the current context. This is particularly useful for implementing join
   * operations where conditions need to be split between different operators.
   *
   * @param whereCondition The WHERE condition expression to prune
   * @param availableTableNames List of table names that are available in the current context
   * @return The pruned expression, or null if the entire expression should be removed
   */
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
      // left and right expressions should represent columns or constants
      if (prunedLeft == null || prunedRight == null) {
        return null;
      }
    } else if (whereCondition instanceof Column) {
      Column column = (Column) whereCondition;
      Table table = column.getTable();
      if (!availableTableNames.contains(
          table.getAlias() != null ? table.getAlias().getName() : table.getName())) {
        return null;
      }
    }
    return whereCondition; // return unchanged expression
  }

  /**
   * Constructs an ExpressionEvaluator with the given table alias mappings.
   *
   * @param tableAliases Map of table aliases to their actual table names
   */
  public ExpressionEvaluator(Map<String, String> tableAliases) {
    this.tableAliases = tableAliases;
  }

  /**
   * Evaluates an expression against a given tuple and schema. This is the main entry point for
   * expression evaluation.
   *
   * @param expr The expression to evaluate
   * @param tuple The tuple to evaluate against
   * @param schema The schema defining the structure of the tuple
   * @return The boolean result of the expression evaluation
   */
  public boolean evaluate(Expression expr, Tuple tuple, List<Column> schema) {
    this.tuple = tuple;
    this.schema = schema;
    expr.accept(this); // Visit the expression and evaluate it
    return result;
  }

  /**
   * Visits and evaluates an AND expression by evaluating both operands.
   *
   * @param andExpression The AND expression to evaluate
   */
  @Override
  public void visit(AndExpression andExpression) {
    andExpression.getLeftExpression().accept(this);
    boolean leftResult = result; // Store the result of the left expression
    andExpression.getRightExpression().accept(this); // Evaluate the right expression
    result = leftResult && result; // Combine results using AND logic
  }

  /**
   * Visits and evaluates a column reference by looking up its value in the current tuple.
   *
   * @param column The column reference to evaluate
   * @throws IllegalArgumentException if the column is not found in the schema
   */
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

  /**
   * Visits and evaluates a long literal value.
   *
   * @param longValue The long value to evaluate
   */
  @Override
  public void visit(LongValue longValue) {
    tempValue = (int) longValue.getValue(); // Handle long value
  }

  /**
   * Visits and evaluates a double literal value.
   *
   * @param doubleValue The double value to evaluate
   */
  @Override
  public void visit(DoubleValue doubleValue) {
    tempValue = (int) doubleValue.getValue(); // Handle double value (truncated to int)
  }

  /**
   * Visits and evaluates an equals comparison.
   *
   * @param equalsTo The equals comparison to evaluate
   */
  @Override
  public void visit(EqualsTo equalsTo) {
    equalsTo.getLeftExpression().accept(this);
    int left = tempValue;
    equalsTo.getRightExpression().accept(this);
    result = left == tempValue;
  }

  /**
   * Visits and evaluates a not-equals comparison.
   *
   * @param notEqualsTo The not-equals comparison to evaluate
   */
  @Override
  public void visit(NotEqualsTo notEqualsTo) {
    notEqualsTo.getLeftExpression().accept(this);
    int left = tempValue;
    notEqualsTo.getRightExpression().accept(this);
    result = left != tempValue;
  }

  /**
   * Visits and evaluates a greater-than comparison.
   *
   * @param greaterThan The greater-than comparison to evaluate
   */
  @Override
  public void visit(GreaterThan greaterThan) {
    greaterThan.getLeftExpression().accept(this);
    int left = tempValue;
    greaterThan.getRightExpression().accept(this);
    result = left > tempValue;
  }

  /**
   * Visits and evaluates a greater-than-or-equals comparison.
   *
   * @param greaterThanEquals The greater-than-or-equals comparison to evaluate
   */
  @Override
  public void visit(GreaterThanEquals greaterThanEquals) {
    greaterThanEquals.getLeftExpression().accept(this);
    int left = tempValue;
    greaterThanEquals.getRightExpression().accept(this);
    result = left >= tempValue;
  }

  /**
   * Visits and evaluates a less-than comparison.
   *
   * @param minorThan The less-than comparison to evaluate
   */
  @Override
  public void visit(MinorThan minorThan) {
    minorThan.getLeftExpression().accept(this);
    int left = tempValue;
    minorThan.getRightExpression().accept(this);
    result = left < tempValue;
  }

  /**
   * Visits and evaluates a less-than-or-equals comparison.
   *
   * @param minorThanEquals The less-than-or-equals comparison to evaluate
   */
  @Override
  public void visit(MinorThanEquals minorThanEquals) {
    minorThanEquals.getLeftExpression().accept(this);
    int left = tempValue;
    minorThanEquals.getRightExpression().accept(this);
    result = left <= tempValue;
  }

  /**
   * Finds the index of a column in the schema based on the alias and column name. This method
   * handles table aliases properly to support self-joins.
   *
   * @param tableAlias The alias of the table or null if none
   * @param columnName The name of the column
   * @return The index of the column in the schema, or -1 if not found
   */
  private int getColumnIndex(String tableAlias, String columnName) {
    for (int i = 0; i < schema.size(); i++) {
      Column schemaColumn = schema.get(i);
      String schemaColumnName = schemaColumn.getColumnName();
      String schemaTableAlias =
          schemaColumn.getTable().getAlias() != null
              ? schemaColumn.getTable().getAlias().getName()
              : schemaColumn.getTable().getName();

      // Prioritize alias match over table name match
      if (schemaColumnName.equals(columnName)
          && (tableAlias != null && tableAlias.equals(schemaTableAlias))) {
        return i;
      }
    }

    return -1; // Return -1 if the column was not found
  }
}
