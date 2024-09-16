package operator;

import common.Tuple;
import common.ExpressionEvaluator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.Map;

public class JoinOperator extends Operator {
  private Operator leftChild;
  private Operator rightChild;
  private Expression joinCondition;
  private Tuple leftTuple;
  private ExpressionEvaluator evaluator;
  private Map<String, String> tableAliases;

  public JoinOperator(Operator leftChild, Operator rightChild, Expression joinCondition,
      Map<String, String> tableAliases) {
    super(new ArrayList<>());
    this.leftChild = leftChild;
    this.rightChild = rightChild;
    this.joinCondition = joinCondition;
    this.tableAliases = tableAliases;
    this.evaluator = new ExpressionEvaluator(tableAliases);
    setupOutputSchema();
  }

  private void setupOutputSchema() {
    this.outputSchema = new ArrayList<>();
    this.outputSchema.addAll(leftChild.getOutputSchema());
    this.outputSchema.addAll(rightChild.getOutputSchema());
  }

  @Override
  public void reset() {
    leftChild.reset();
    rightChild.reset();
    leftTuple = null;
  }

  @Override
  public Tuple getNextTuple() {
    while (true) {
      if (leftTuple == null) {
        leftTuple = leftChild.getNextTuple();
        if (leftTuple == null) {
          return null; // No more tuples from left child
        }
        rightChild.reset();
      }

      Tuple rightTuple = rightChild.getNextTuple();
      if (rightTuple == null) {
        leftTuple = null; // Move to next left tuple
        continue;
      }

      Tuple joinedTuple = joinTuples(leftTuple, rightTuple);
      if (joinCondition == null || evaluator.evaluate(joinCondition, joinedTuple, outputSchema)) {
        return joinedTuple;
      }
    }
  }

  private Tuple joinTuples(Tuple left, Tuple right) {
    ArrayList<Integer> values = new ArrayList<>(left.getAllElements());
    values.addAll(right.getAllElements());
    return new Tuple(values);
  }
}