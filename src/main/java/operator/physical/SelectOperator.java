package operator.physical;

import common.ExpressionEvaluator;
import common.Tuple;
import java.util.ArrayList;
import java.util.Map;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

/**
 * The {@code SelectOperator} class applies a selection (WHERE) predicate to the tuples produced by
 * its child operator. It evaluates each incoming tuple against the provided WHERE expression,
 * returning only those that satisfy the condition.
 *
 * <p>This operator:
 *
 * <ul>
 *   <li>Fetches tuples from its child operator.
 *   <li>Evaluates the WHERE expression on each tuple.
 *   <li>Returns tuples that pass the condition and discards those that do not.
 * </ul>
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * Operator child = ...; // Some operator producing tuples
 * Expression where = ...; // A parsed SQL expression (e.g., "S.A > 50")
 * Map<String, String> tableAliases = ...;
 *
 * SelectOperator selectOp = new SelectOperator(child, where, tableAliases);
 * Tuple tuple;
 * while ((tuple = selectOp.getNextTuple()) != null) {
 *   System.out.println(tuple);
 * }
 * }</pre>
 */
public class SelectOperator extends Operator {
  /** The child operator supplying tuples to be filtered. */
  private Operator child;

  /** The WHERE expression defining the selection predicate. */
  private Expression whereExpression;

  /** The evaluator used to apply the WHERE expression to tuples. */
  private ExpressionEvaluator evaluator;

  /**
   * Constructs a SelectOperator.
   *
   * @param child The child operator producing tuples to be filtered.
   * @param whereExpression The WHERE expression to evaluate on each tuple.
   * @param tableAliases A map of table aliases to their real table names, for expression
   *     evaluation.
   */
  public SelectOperator(
      Operator child, Expression whereExpression, Map<String, String> tableAliases) {
    super(new ArrayList<>(child.getOutputSchema()));
    this.child = child;
    this.whereExpression = whereExpression;
    this.evaluator = new ExpressionEvaluator(tableAliases);
    System.out.println("Created SelectOperator with condition: " + whereExpression); // Debug
  }

  /** Resets the operator and its child to the initial state. */
  @Override
  public void reset() {
    child.reset();
  }

  /**
   * Retrieves the next tuple that satisfies the selection condition.
   *
   * @return The next tuple passing the WHERE predicate, or null if no more tuples.
   */
  @Override
  public Tuple getNextTuple() {
    while (true) {
      Tuple nextTuple = child.getNextTuple();
      System.out.println("SelectOperator received tuple: " + nextTuple); // Debug
      if (nextTuple == null) {
        return null; // No more tuples
      }

      boolean passes = evaluator.evaluate(whereExpression, nextTuple, getOutputSchema());
      System.out.println("Condition evaluation: " + passes); // Debug
      if (passes) {
        return nextTuple; // Return the tuple that satisfies the condition
      }
      // Otherwise, continue fetching the next tuple
    }
  }

  /**
   * Returns the output schema of this operator, which is the same as the child's schema.
   *
   * @return The list of columns in the output schema.
   */
  @Override
  public ArrayList<Column> getOutputSchema() {
    return child.getOutputSchema();
  }

  /**
   * Returns the selection condition expression.
   *
   * @return The WHERE expression.
   */
  public Expression getCondition() {
    return whereExpression;
  }

  /**
   * Returns the child operator of this select operator.
   *
   * @return The child operator.
   */
  @Override
  public Operator getChild() {
    return child;
  }
}
