package operator.logical;

import common.LogicalOperatorVisitor;
import java.util.Arrays;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

public class LogicalJoinOperator extends LogicalOperator {
  private LogicalOperator leftChild;
  private LogicalOperator rightChild;
  private Expression condition;

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

  public Expression getCondition() {
    return condition;
  }

  @Override
  public void accept(LogicalOperatorVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public List<LogicalOperator> getChildren() {
    return Arrays.asList(leftChild, rightChild);
  }

  @Override
  public String toString() {
    return "LogicalJoinOperator: " + (condition != null ? condition.toString() : "Cross Product");
  }
}
