package operator.logical;

import common.LogicalOperatorVisitor;
import java.util.Arrays;
import java.util.List;

public class LogicalDuplicateEliminationOperator extends LogicalOperator {
  private LogicalOperator child;

  public LogicalDuplicateEliminationOperator(LogicalOperator child) {
    super(child.getSchema());
    this.child = child;
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
    return "LogicalDuplicateEliminationOperator";
  }
}
