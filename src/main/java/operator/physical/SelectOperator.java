package operator.physical;

import common.ExpressionEvaluator;
import common.Tuple;
import java.util.ArrayList;
import java.util.Map;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

public class SelectOperator extends Operator {
  private Operator child;
  private Expression whereExpression;
  private ExpressionEvaluator evaluator;

  public SelectOperator(
      Operator child, Expression whereExpression, Map<String, String> tableAliases) {
    super(new ArrayList<>(child.getOutputSchema()));
    this.child = child;
    this.whereExpression = whereExpression;
    this.evaluator = new ExpressionEvaluator(tableAliases);
    System.out.println("Created SelectOperator with condition: " + whereExpression);
  }

  @Override
  public void reset() {
    child.reset();
  }

  @Override
  public Tuple getNextTuple() {
    while (true) {
      Tuple nextTuple = child.getNextTuple();
      System.out.println("SelectOperator received tuple: " + nextTuple); // Debug
      if (nextTuple == null) {
        return null;
      }
      boolean passes = evaluator.evaluate(whereExpression, nextTuple, getOutputSchema());
      System.out.println("Condition evaluation: " + passes); // Debug
      if (passes) {
        return nextTuple;
      }
    }
  }

  @Override
  public ArrayList<Column> getOutputSchema() {
    return child.getOutputSchema();
  }

  public Expression getCondition() {
    return whereExpression;
  }

  @Override
  public Operator getChild() {
    return child;
  }
}
