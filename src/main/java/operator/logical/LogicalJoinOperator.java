package operator.logical;

import common.LogicalOperatorVisitor;
import java.util.Arrays;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

/**
 * Represents a logical JOIN operation in a query plan.
 * This operator combines rows from two child operators based on a join
 * condition.
 * If no condition is provided, it performs a cross product.
 */
public class LogicalJoinOperator extends LogicalOperator {

  /** The left child operator of the join. */
  private LogicalOperator leftChild;

  /** The right child operator of the join. */
  private LogicalOperator rightChild;

  /** The join condition. Can be null for a cross product. */
  private Expression condition;

  /**
   * Constructs a new LogicalJoinOperator.
   *
   * @param leftChild  The left child logical operator.
   * @param rightChild The right child logical operator.
   * @param condition  The join condition. Can be null for a cross product.
   * @param schema     The schema of the resulting joined relation.
   */
  public LogicalJoinOperator(
      LogicalOperator leftChild,
      LogicalOperator rightChild,
      Expression condition,
      List<Column> schema) {
    super(schema);
    this.leftChild = leftChild;
    this.rightChild = rightChild;
    this.condition = condition;
  }

  /**
   * Gets the join condition.
   *
   * @return The join condition expression, or null if this is a cross product.
   */
  public Expression getCondition() {
    return condition;
  }

  /**
   * Accepts a visitor, allowing the visitor to perform operations on this
   * operator.
   *
   * @param visitor The LogicalOperatorVisitor visiting this operator.
   */
  @Override
  public void accept(LogicalOperatorVisitor visitor) {
    visitor.visit(this);
  }

  /**
   * Returns a list containing this operator's left and right child operators.
   *
   * @return A list containing the left and right child operators.
   */
  @Override
  public List<LogicalOperator> getChildren() {
    return Arrays.asList(leftChild, rightChild);
  }

  /**
   * Returns a string representation of this operator.
   *
   * @return A string describing this join operator, including the join condition
   *         if present.
   */
  @Override
  public String toString() {
    return "LogicalJoinOperator: " + (condition != null ? condition.toString() : "Cross Product");
  }
}