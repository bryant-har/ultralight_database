package operator;

import common.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.expression.Alias;
import common.ExpressionEvaluator;

import java.util.ArrayList;
import java.util.Map;

public class JoinOperator extends Operator {
  private Operator leftChild;
  private Operator rightChild;
  private Expression joinCondition;
  private Map<String, String> tableAliases;

  private Tuple leftTuple;
  private Tuple rightTuple;

  private ExpressionEvaluator expressionEvaluator;

  public JoinOperator(Operator leftChild, Operator rightChild, Expression joinCondition,
      Map<String, String> tableAliases) {
    // Combine schemas of the left and right children and set them as the output
    // schema
    super(combineSchemas(leftChild.getOutputSchema(), rightChild.getOutputSchema()));

    this.leftChild = leftChild;
    this.rightChild = rightChild;
    this.joinCondition = joinCondition;
    this.tableAliases = tableAliases;

    // Initialize the expression evaluator with the table aliases
    this.expressionEvaluator = new ExpressionEvaluator(tableAliases);

    // Get the first tuple from the left and right child
    this.leftTuple = leftChild.getNextTuple();
    this.rightTuple = rightChild.getNextTuple();
  }

  @Override
  public Tuple getNextTuple() {
    while (leftTuple != null) { // Outer loop for left child
      while (rightTuple != null) { // Inner loop for right child
        Tuple joinedTuple = joinTuples(leftTuple, rightTuple); // Join the left and right tuples

        // Move to the next right tuple for the next iteration
        rightTuple = rightChild.getNextTuple();

        // Evaluate the join condition (if it exists) or return the Cartesian product
        if (joinCondition == null || evaluateJoinCondition(joinedTuple)) {
          return joinedTuple; // Return the joined tuple if the condition is met
        }
      }

      // Reset the right child to start over for the next left tuple
      rightChild.reset();
      rightTuple = rightChild.getNextTuple(); // Get the first right tuple again

      // Move to the next left tuple
      leftTuple = leftChild.getNextTuple();
    }

    return null; // No more tuples
  }

  @Override
  public void reset() {
    leftChild.reset();
    rightChild.reset();
    leftTuple = leftChild.getNextTuple();
    rightTuple = rightChild.getNextTuple();
  }

  private static ArrayList<Column> combineSchemas(ArrayList<Column> leftSchema, ArrayList<Column> rightSchema) {
    ArrayList<Column> combinedSchema = new ArrayList<>();

    // Process the columns from the left schema
    for (Column col : leftSchema) {
      Table table = new Table();
      table.setName(col.getTable().getName());

      if (col.getTable().getAlias() != null) {
        table.setAlias(new Alias(col.getTable().getAlias().getName())); // Keep alias if present
      } else {
        table.setAlias(new Alias(col.getTable().getName())); // Use table name as alias if none exists
      }

      Column newCol = new Column(table, col.getColumnName());
      combinedSchema.add(newCol);
    }

    // Process the columns from the right schema
    for (Column col : rightSchema) {
      Table table = new Table();
      table.setName(col.getTable().getName());

      if (col.getTable().getAlias() != null) {
        table.setAlias(new Alias(col.getTable().getAlias().getName())); // Keep alias if present
      } else {
        table.setAlias(new Alias(col.getTable().getName())); // Use table name as alias if none exists
      }

      Column newCol = new Column(table, col.getColumnName());
      combinedSchema.add(newCol);
    }

    return combinedSchema;
  }

  private Tuple joinTuples(Tuple left, Tuple right) {
    ArrayList<Integer> combinedData = new ArrayList<>(left.getAllElements());
    combinedData.addAll(right.getAllElements());
    return new Tuple(combinedData);
  }

  private boolean evaluateJoinCondition(Tuple tuple) {
    return expressionEvaluator.evaluate(joinCondition, tuple, this.outputSchema);
  }
}
