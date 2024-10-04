package operator.logical;

import common.LogicalOperatorVisitor;
import java.util.Arrays;
import java.util.List;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class LogicalSortOperator extends LogicalOperator {
  private LogicalOperator child;
  private List<OrderByElement> orderByElements;

  public LogicalSortOperator(LogicalOperator child, List<OrderByElement> orderByElements) {
    super(child.getSchema());
    this.child = child;
    this.orderByElements = orderByElements;
  }

  public List<OrderByElement> getOrderByElements() {
    return orderByElements;
  }

  @Override
  public void accept(LogicalOperatorVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public List<LogicalOperator> getChildren() {
    return Arrays.asList(child);
  }

  @Override
  public String toString() {
    return "LogicalSortOperator: " + orderByElements.toString();
  }
}
