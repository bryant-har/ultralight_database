package operator.logical;

import common.LogicalOperatorVisitor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

/**
 * Represents a logical join operator in a query plan. This operator combines two child operators
 * (left and right) based on a join condition.
 */
public class LogicalJoinOperator extends LogicalOperator {
  private LogicalOperator leftChild;
  private LogicalOperator rightChild;
  private Expression condition;

  /**
   * Constructs a new LogicalJoinOperator.
   *
   * @param leftChild The left child operator of the join.
   * @param rightChild The right child operator of the join.
   * @param condition The join condition expression. Can be null for cross product.
   */
  public LogicalJoinOperator(
      LogicalOperator leftChild, LogicalOperator rightChild, Expression condition) {
    super(combineAndQualifySchemas(leftChild, rightChild));
    this.leftChild = leftChild;
    this.rightChild = rightChild;
    this.condition = condition;
  }

  /**
   * Combines and qualifies the schemas of the left and right child operators.
   *
   * @param left The left child operator.
   * @param right The right child operator.
   * @return A combined list of qualified columns from both child operators.
   */
  private static List<Column> combineAndQualifySchemas(
      LogicalOperator left, LogicalOperator right) {
    List<Column> combinedSchema = new ArrayList<>();
    addQualifiedColumns(combinedSchema, left);
    addQualifiedColumns(combinedSchema, right);
    return combinedSchema;
  }

  /**
   * Adds qualified columns from an operator to the given schema.
   *
   * @param schema The schema to add columns to.
   * @param operator The operator whose columns are to be added.
   */
  private static void addQualifiedColumns(List<Column> schema, LogicalOperator operator) {
    for (Column col : operator.getSchema()) {
      Table table = col.getTable();
      if (table == null || table.getName() == null) {
        // If the column doesn't have a table, use the operator's name or alias
        table = new Table(operator.getClass().getSimpleName());
      }
      schema.add(new Column(table, col.getColumnName()));
    }
  }

  /**
   * Gets the join condition expression.
   *
   * @return The join condition expression.
   */
  public Expression getCondition() {
    return condition;
  }

  /**
   * Accepts a LogicalOperatorVisitor to visit this operator.
   *
   * @param visitor The visitor to accept.
   */
  @Override
  public void accept(LogicalOperatorVisitor visitor) {
    visitor.visit(this);
  }

  /**
   * Gets the child operators of this join operator.
   *
   * @return A list containing the left and right child operators.
   */
  @Override
  public List<LogicalOperator> getChildren() {
    return Arrays.asList(leftChild, rightChild);
  }

  /**
   * Returns a string representation of this join operator.
   *
   * @return A string describing this join operator and its condition.
   */
  @Override
  public String toString() {
    return "LogicalJoinOperator: " + (condition != null ? condition.toString() : "Cross Product");
  }
}
