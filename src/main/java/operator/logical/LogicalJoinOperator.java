package operator.logical;

import common.LogicalOperatorVisitor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

/**
 * Represents a logical join operator in a query plan. This operator combines
 * two child operators
 * (left and right) based on a join condition.
 */
public class LogicalJoinOperator extends LogicalOperator {
  private List<LogicalOperator> children;
  private List<Expression> residualConditions;
  private UnionFind unionFind;

  public LogicalJoinOperator(List<LogicalOperator> children,
      List<Expression> residualConditions,
      UnionFind unionFind) {
    super(combineSchemas(children));
    this.children = children;
    this.residualConditions = residualConditions;
    this.unionFind = unionFind;
  }

  private static List<Column> combineSchemas(List<LogicalOperator> children) {
    List<Column> combined = new ArrayList<>();
    for (LogicalOperator child : children) {
      combined.addAll(child.getSchema());
    }
    return combined;
  }

  @Override
  public void accept(LogicalOperatorVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public List<LogicalOperator> getChildren() {
    return children;
  }

  public List<Expression> getResidualConditions() {
    return residualConditions;
  }

  public UnionFind getUnionFind() {
    return unionFind;
  }
}