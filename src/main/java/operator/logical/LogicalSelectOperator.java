package operator.logical;

import common.LogicalOperatorVisitor;
import java.util.Arrays;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;

public class LogicalSelectOperator extends LogicalOperator {
  private LogicalOperator child;
  private Expression condition;

  public LogicalSelectOperator(LogicalOperator child, Expression condition) {
    super(child.getSchema());
    this.child = child;
    this.condition = condition;
  }

  public Expression getCondition() {
    return condition;
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
    return "LogicalSelectOperator: " + condition.toString();
  }
}
