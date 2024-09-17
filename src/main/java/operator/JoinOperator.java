package operator;

import common.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.expression.Alias;
import common.ExpressionEvaluator;

import java.util.ArrayList;
import java.util.Map;

/**
 * JoinOperator performs a tuple nested loop join between two child operators.
 * It can handle join conditions (e.g., R.A = S.B) or perform a cross product if
 * the condition is null.
 */
public class JoinOperator extends Operator {
  private Operator leftChild;
  private Operator rightChild;
  private Expression joinCondition;
  private Map<String, String> tableAliases;

  private Tuple leftTuple;
  private Tuple rightTuple;

  // For evaluating expressions
  private ExpressionEvaluator expressionEvaluator;

  public JoinOperator(Operator leftChild, Operator rightChild, Expression joinCondition,
      Map<String, String> tableAliases) {
    super(combineSchemas(leftChild.getOutputSchema(), rightChild.getOutputSchema()));
    this.leftChild = leftChild;
    this.rightChild = rightChild;
    this.joinCondition = joinCondition;
    this.tableAliases = tableAliases;

    // Initialize the expression evaluator with the table aliases
    this.expressionEvaluator = new ExpressionEvaluator(tableAliases);

    // Get the first tuple from the left child
    this.leftTuple = leftChild.getNextTuple();
    // Initialize right tuple to null; it will be set in getNextTuple()
    this.rightTuple = null;
  }

  @Override
  public Tuple getNextTuple() {
    while (leftTuple != null) {
      if (rightTuple == null) {
        // Reset right child and get the first right tuple
        rightChild.reset();
        rightTuple = rightChild.getNextTuple();
      }
      while (rightTuple != null) {
        Tuple joinedTuple = joinTuples(leftTuple, rightTuple);

        // Store the current right tuple
        Tuple currentRightTuple = rightTuple;
        // Move to the next right tuple for the next iteration
        rightTuple = rightChild.getNextTuple();

        if (joinCondition == null || evaluateJoinCondition(joinedTuple)) {
          return joinedTuple;
        }
      }
      // Reset right tuple to null to trigger reset in next iteration
      rightTuple = null;
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
    rightTuple = null;
  }

  /**
   * Combines the schemas of the left and right children, ensuring proper
   * aliasing.
   */
  private static ArrayList<Column> combineSchemas(ArrayList<Column> leftSchema, ArrayList<Column> rightSchema) {
    ArrayList<Column> combinedSchema = new ArrayList<>();

    // Process the columns from the left schema
    for (Column col : leftSchema) {
      // Create a new Table object and set its name and alias
      Table table = new Table();
      table.setName(col.getTable().getName());

      // Check if the column has an alias; if not, use the table name as the alias
      if (col.getTable().getAlias() != null) {
        table.setAlias(new Alias(col.getTable().getAlias().getName())); // Keep alias if present
      } else {
        table.setAlias(new Alias(col.getTable().getName())); // Use the table name as alias if none exists
      }

      // Create a new Column with the new Table and column name
      Column newCol = new Column(table, col.getColumnName());
      combinedSchema.add(newCol);
    }

    // Process the columns from the right schema
    for (Column col : rightSchema) {
      // Create a new Table object and set its name and alias
      Table table = new Table();
      table.setName(col.getTable().getName());

      // Check if the column has an alias; if not, use the table name as the alias
      if (col.getTable().getAlias() != null) {
        table.setAlias(new Alias(col.getTable().getAlias().getName())); // Keep alias if present
      } else {
        table.setAlias(new Alias(col.getTable().getName())); // Use the table name as alias if none exists
      }

      // Create a new Column with the new Table and column name
      Column newCol = new Column(table, col.getColumnName());
      combinedSchema.add(newCol);
    }

    return combinedSchema;
  }

  /**
   * Joins two tuples by concatenating their data.
   */
  private Tuple joinTuples(Tuple left, Tuple right) {
    ArrayList<Integer> combinedData = new ArrayList<>(left.getAllElements());
    combinedData.addAll(right.getAllElements());
    return new Tuple(combinedData);
  }

  /**
   * Evaluates the join condition on the joined tuple.
   */
  private boolean evaluateJoinCondition(Tuple tuple) {
    return expressionEvaluator.evaluate(joinCondition, tuple, this.outputSchema);
  }

  @Override
  public ArrayList<Column> getOutputSchema() {
    return outputSchema;
  }
}
