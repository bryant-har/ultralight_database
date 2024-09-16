package operator;

import common.Tuple;
import common.ExpressionEvaluator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.Map;

public class SelectOperator extends Operator {
  private Operator child;
  private Expression whereExpression;
  private ExpressionEvaluator evaluator;
  private Map<String, String> tableAliases;

  public SelectOperator(Operator child, Expression whereExpression, Map<String, String> tableAliases) {
    super(new ArrayList<>(child.getOutputSchema()));
    this.child = child;
    this.whereExpression = whereExpression;
    this.tableAliases = tableAliases;
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