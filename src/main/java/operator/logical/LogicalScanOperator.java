package operator.logical;

import common.LogicalOperatorVisitor;
import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

/**
 * Represents a logical SCAN operation in a query plan.
 * This operator is responsible for reading data from a base table.
 * It is always a leaf node in the logical operator tree as it doesn't have any
 * child operators.
 */
public class LogicalScanOperator extends LogicalOperator {

  /** The table to be scanned. */
  private Table table;

  /**
   * Constructs a new LogicalScanOperator.
   *
   * @param table  The table to be scanned.
   * @param schema The schema of the table being scanned.
   */
  public LogicalScanOperator(Table table, List<Column> schema) {
    super(schema);
    this.table = table;
  }

  /**
   * Gets the table being scanned by this operator.
   *
   * @return The Table object representing the scanned table.
   */
  public Table getTable() {
    return table;
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
   * Returns an empty list as this operator is a leaf node and has no children.
   *
   * @return An empty list of LogicalOperators.
   */
  @Override
  public List<LogicalOperator> getChildren() {
    return new ArrayList<>(); // Scan is a leaf node, so it has no children
  }

  /**
   * Returns a string representation of this operator.
   *
   * @return A string describing this scan operator, including the name of the
   *         table being scanned.
   */
  @Override
  public String toString() {
    return "LogicalScanOperator: " + table.getName();
  }
}