/**
 * Represents a logical SELECT operation in a query plan.
 * This operator filters rows from its child operator based on a specified condition.
 */
package operator.logical;

import common.LogicalOperatorVisitor;
import java.util.Arrays;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;

public class LogicalSelectOperator extends LogicalOperator {

  /** The child operator whose output this operator filters. */
  private LogicalOperator child;

  /** The condition used to filter rows. */
  private Expression condition;

  /**
   * Constructs a new LogicalSelectOperator.
   *
   * @param child     The child logical operator.
   * @param condition The condition used to filter rows.
   */
  public LogicalSelectOperator(LogicalOperator child, Expression condition) {
    super(child.getSchema());
    this.child = child;
    this.condition = condition;
  }

  /**
   * Gets the condition used to filter rows.
   *
   * @return The filtering condition.
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
   * Returns a list containing this operator's single child.
   *
   * @return A list containing the child operator.
   */
  @Override
  public List<LogicalOperator> getChildren() {
    return Arrays.asList(child);
  }

  /**
   * Returns a string representation of this operator.
   *
   * @return A string describing this select operator, including the filtering
   *         condition.
   */
  @Override
  public String toString() {
    return "LogicalSelectOperator: " + condition.toString();
  }
}