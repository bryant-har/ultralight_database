package operator.logical;

import common.LogicalOperatorVisitor;
import java.util.Arrays;
import java.util.List;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectItem;

/**
 * Represents a logical PROJECT operation in a query plan. This operator selects specific columns or
 * expressions from its child operator's output, potentially renaming them as specified in the
 * SELECT clause of a SQL query.
 */
public class LogicalProjectOperator extends LogicalOperator {

  /** The child operator whose output this operator projects. */
  private LogicalOperator child;

  /** The list of items to be projected, as specified in the SELECT clause. */
  private List<SelectItem> selectItems;

  /**
   * Constructs a new LogicalProjectOperator.
   *
   * @param child The child logical operator.
   * @param selectItems The list of items to be projected.
   * @param schema The schema of the resulting projected relation.
   */
  public LogicalProjectOperator(
      LogicalOperator child, List<SelectItem> selectItems, List<Column> schema) {
    super(schema);
    this.child = child;
    this.selectItems = selectItems;
  }

  /**
   * Gets the list of items to be projected.
   *
   * @return The list of SelectItems specifying the projection.
   */
  public List<SelectItem> getSelectItems() {
    return selectItems;
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
   * @return A string describing this project operator, including the items being projected.
   */
  @Override
  public String toString() {
    return "LogicalProjectOperator: " + selectItems.toString();
  }
}
