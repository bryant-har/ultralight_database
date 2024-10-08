/**
 * Represents a logical SORT operation in a query plan. This operator sorts the rows from its child
 * operator based on specified ordering elements.
 */
package operator.logical;

import common.LogicalOperatorVisitor;
import java.util.Arrays;
import java.util.List;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class LogicalSortOperator extends LogicalOperator {

  /** The child operator whose output this operator sorts. */
  private LogicalOperator child;

  /** The list of elements specifying the sort order. */
  private List<OrderByElement> orderByElements;

  /**
   * Constructs a new LogicalSortOperator.
   *
   * @param child The child logical operator.
   * @param orderByElements The list of elements specifying the sort order.
   */
  public LogicalSortOperator(LogicalOperator child, List<OrderByElement> orderByElements) {
    super(child.getSchema());
    this.child = child;
    this.orderByElements = orderByElements;
  }

  /**
   * Gets the list of elements specifying the sort order.
   *
   * @return The list of OrderByElements.
   */
  public List<OrderByElement> getOrderByElements() {
    return orderByElements;
  }

  /**
   * Accepts a visitor, allowing the visitor to perform operations on this operator.
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
   * @return A string describing this sort operator, including the ordering elements.
   */
  @Override
  public String toString() {
    return "LogicalSortOperator: " + orderByElements.toString();
  }
}
