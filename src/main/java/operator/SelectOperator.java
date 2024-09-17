package operator;

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
  }

  @Override
  public void reset() {
    child.reset();
  }

  @Override
  public Tuple getNextTuple() {
    while (true) {
      Tuple nextTuple = child.getNextTuple();
      if (nextTuple == null) {
        return null;
      }
      if (evaluator.evaluate(whereExpression, nextTuple, getOutputSchema())) {
        return nextTuple;
      }
    }
  }

  @Override
  public ArrayList<Column> getOutputSchema() {
    return child.getOutputSchema();
  }
}
