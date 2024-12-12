package operator.logical;

import common.LogicalOperatorVisitor;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

/**
 * Represents a logical join operator in a query plan. This operator combines multiple child
 * operators based on given join conditions and maintains a union-find structure to handle equality
 * constraints and propagate statistics.
 *
 * <p>The union-find structure allows the system to keep track of columns that are known to be
 * equal, as well as their associated constraints (equality values, lower and upper bounds). The
 * residual conditions are those that cannot be directly handled by equality propagation alone.
 *
 * <p>During query optimization, these union-find and residual conditions help determine the optimal
 * join order and potentially reduce the search space.
 */
public class LogicalJoinOperator extends LogicalOperator {
  /** The child operators to be joined together. */
  private List<LogicalOperator> children;

  /** The residual join conditions that are not fully handled by the union-find structure. */
  private List<Expression> residualConditions;

  /** The union-find structure capturing equality constraints and bounds among attributes. */
  private UnionFind unionFind;

  /**
   * Constructs a new LogicalJoinOperator.
   *
   * @param children The logical operators to be joined.
   * @param residualConditions Any leftover conditions that the union-find couldn't absorb.
   * @param unionFind The union-find structure with equality classes and constraints.
   */
  public LogicalJoinOperator(
      List<LogicalOperator> children, List<Expression> residualConditions, UnionFind unionFind) {
    super(combineSchemas(children));
    this.children = children;
    this.residualConditions = residualConditions;
    this.unionFind = unionFind;
  }

  /**
   * Combines the schemas of all child operators into a single schema.
   *
   * @param children The child operators.
   * @return A list of columns representing the combined schema.
   */
  private static List<Column> combineSchemas(List<LogicalOperator> children) {
    List<Column> combined = new ArrayList<>();
    for (LogicalOperator child : children) {
      combined.addAll(child.getSchema());
    }
    return combined;
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
   * Returns the list of child operators that this operator joins together.
   *
   * @return The list of child operators.
   */
  @Override
  public List<LogicalOperator> getChildren() {
    return children;
  }

  /**
   * Returns the residual conditions that cannot be fully handled by the union-find.
   *
   * @return A list of residual conditions.
   */
  public List<Expression> getResidualConditions() {
    return residualConditions;
  }

  /**
   * Returns the union-find structure containing equality classes and constraints.
   *
   * @return The union-find structure.
   */
  public UnionFind getUnionFind() {
    return unionFind;
  }

  /**
   * Returns a string representation of the join condition. If multiple residual conditions exist,
   * they are combined with " AND ".
   *
   * @return A string representing the join condition.
   */
  public String getJoinCondition() {
    if (residualConditions == null || residualConditions.isEmpty()) {
      return "No Join Condition";
    }
    return residualConditions.stream()
        .map(Expression::toString)
        .collect(Collectors.joining(" AND "));
  }

  /**
   * Returns a list of statistics strings derived from the union-find structure. Each string might
   * look like:
   *
   * <pre>
   * [[S.B, R.G], equals null, min null, max null]
   * [[S.A, B.D], equals null, min null, max null]
   * [[R.H], equals null, min null, max 99]
   * </pre>
   *
   * These statistics provide insights into the equality classes, minimum/maximum values, and
   * equality constraints discovered during optimization.
   *
   * @return A list of formatted statistics strings.
   */
  public List<String> getStats() {
    if (unionFind == null) {
      return new ArrayList<>();
    }
    return unionFind.getStatsStrings();
  }

  /**
   * Returns a string representation of this operator, including the join condition.
   *
   * @return A string describing this join operator.
   */
  @Override
  public String toString() {
    return "LogicalJoinOperator: " + getJoinCondition();
  }
}
