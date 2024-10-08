package operator.logical;

import common.LogicalOperatorVisitor;
import java.util.List;
import net.sf.jsqlparser.schema.Column;

/**
 * Abstract base class for all logical operators in the query plan. This class defines the common
 * structure and behavior for logical operators.
 */
public abstract class LogicalOperator {

  /** The schema (list of columns) for this operator's output. */
  protected List<Column> schema;

  /**
   * Constructor for LogicalOperator.
   *
   * @param schema The schema (list of columns) for this operator's output.
   */
  public LogicalOperator(List<Column> schema) {
    this.schema = schema;
  }

  /**
   * Get the schema for this operator's output.
   *
   * @return The list of columns in the output schema.
   */
  public List<Column> getSchema() {
    return schema;
  }

  /**
   * Accept method for the visitor pattern. This method should be implemented by all subclasses to
   * allow for the visitor to visit them.
   *
   * @param visitor The visitor that will visit this operator.
   */
  public abstract void accept(LogicalOperatorVisitor visitor);

  /**
   * Get the children of this operator. This method should be implemented by all subclasses to
   * return their child operators. For leaf operators like LogicalScanOperator, this method should
   * return an empty list.
   *
   * @return A list of child LogicalOperators.
   */
  public abstract List<LogicalOperator> getChildren();

  /**
   * Print a representation of this operator. This method can be overridden by subclasses to provide
   * a more specific representation.
   *
   * @return A string representation of this operator.
   */
  @Override
  public String toString() {
    return getClass().getSimpleName() + " - Schema: " + schema;
  }
}
