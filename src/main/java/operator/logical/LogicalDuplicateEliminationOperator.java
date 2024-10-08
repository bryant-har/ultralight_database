package operator.logical;

import common.LogicalOperatorVisitor;
import java.util.Arrays;
import java.util.List;

/**
 * Represents a logical DISTINCT operation in a query plan. This operator eliminates duplicate
 * tuples from its child operator's output.
 */
public class LogicalDuplicateEliminationOperator extends LogicalOperator {

  /** The child operator whose output this operator eliminates duplicates from. */
  private LogicalOperator child;

  /**
   * Constructs a new LogicalDuplicateEliminationOperator.
   *
   * @param child The child logical operator.
   */
  public LogicalDuplicateEliminationOperator(LogicalOperator child) {
    super(child.getSchema());
    this.child = child;
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
   * Returns a list containing this operator's child.
   *
   * @return A list containing the single child operator.
   */
  @Override
  public List<LogicalOperator> getChildren() {
    return Arrays.asList(child);
  }

  /**
   * Returns a string representation of this operator.
   *
   * @return The string "LogicalDuplicateEliminationOperator".
   */
  @Override
  public String toString() {
    return "LogicalDuplicateEliminationOperator";
  }
}
