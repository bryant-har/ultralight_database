package operator;

import common.ExpressionEvaluator;
import common.Tuple;
import java.util.ArrayList;
import java.util.Map;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

/**
 * The JoinOperator class implements a relational join operation. It takes two child operators and
 * returns a result based on the join condition. If no join condition is provided, it performs a
 * Cartesian product.
 */
public class JoinOperator extends Operator {
  // Left and right child operators that provide tuples for the join
  private Operator leftChild;
  private Operator rightChild;

  // The condition to evaluate for the join (e.g., ON clause in SQL)
  private Expression joinCondition;

  // A map of table aliases used for expression evaluation
  private Map<String, String> tableAliases;

  // Current tuples being processed from the left and right child operators
  private Tuple leftTuple;
  private Tuple rightTuple;

  // Expression evaluator used to evaluate the join condition
  private ExpressionEvaluator expressionEvaluator;

  /**
   * Constructs a JoinOperator.
   *
   * @param leftChild The left child operator for the join.
   * @param rightChild The right child operator for the join.
   * @param joinCondition The join condition (can be null for Cartesian product).
   * @param tableAliases A map of table aliases to their actual names.
   */
  public JoinOperator(
      Operator leftChild,
      Operator rightChild,
      Expression joinCondition,
      Map<String, String> tableAliases) {
    // Combine schemas of both left and right children to create the output schema
    super(combineSchemas(leftChild.getOutputSchema(), rightChild.getOutputSchema()));

    this.leftChild = leftChild;
    this.rightChild = rightChild;
    this.joinCondition = joinCondition;
    this.tableAliases = tableAliases;

    // Initialize the expression evaluator for evaluating join conditions
    this.expressionEvaluator = new ExpressionEvaluator(tableAliases);

    // Fetch the first tuple from both the left and right child operators
    this.leftTuple = leftChild.getNextTuple();
    this.rightTuple = rightChild.getNextTuple();
  }

  /**
   * Retrieves the next tuple from the join operation by combining tuples from the left and right
   * child operators.
   *
   * @return The next joined tuple, or null if no more tuples are available.
   */
  @Override
  public Tuple getNextTuple() {
    while (leftTuple != null) { // Iterate through the left child tuples
      while (rightTuple != null) { // Iterate through the right child tuples
        // Combine the current left and right tuples into a joined tuple
        Tuple joinedTuple = joinTuples(leftTuple, rightTuple);

        // Fetch the next tuple from the right child for the next iteration
        rightTuple = rightChild.getNextTuple();

        // If no join condition exists, or if the condition is satisfied, return the
        // joined tuple
        if (joinCondition == null || evaluateJoinCondition(joinedTuple)) {
          return joinedTuple;
        }
      }

      // If we've exhausted the right child, reset it and move to the next left tuple
      rightChild.reset();
      rightTuple = rightChild.getNextTuple(); // Re-fetch the first right tuple

      // Fetch the next tuple from the left child
      leftTuple = leftChild.getNextTuple();
    }

    return null; // No more tuples to process
  }

  /**
   * Resets the join operation, allowing both child operators to be re-executed from the beginning.
   */
  @Override
  public void reset() {
    // Reset both child operators
    leftChild.reset();
    rightChild.reset();

    // Re-fetch the first tuple from both the left and right children
    leftTuple = leftChild.getNextTuple();
    rightTuple = rightChild.getNextTuple();
  }

  /**
   * Combines the schemas of the left and right child operators into a single schema. This is used
   * to produce the output schema for the join operation.
   *
   * @param leftSchema The schema of the left child.
   * @param rightSchema The schema of the right child.
   * @return The combined schema of both children.
   */
  private static ArrayList<Column> combineSchemas(
      ArrayList<Column> leftSchema, ArrayList<Column> rightSchema) {
    ArrayList<Column> combinedSchema = new ArrayList<>();

    // Process columns from the left schema and add them to the combined schema
    for (Column col : leftSchema) {
      Table table = new Table();
      table.setName(col.getTable().getName());

      // Retain table alias if present, or use the table name as the alias if not
      if (col.getTable().getAlias() != null) {
        table.setAlias(new Alias(col.getTable().getAlias().getName()));
      } else {
        table.setAlias(new Alias(col.getTable().getName()));
      }

      // Create a new column for the combined schema
      Column newCol = new Column(table, col.getColumnName());
      combinedSchema.add(newCol);
    }

    // Process columns from the right schema and add them to the combined schema
    for (Column col : rightSchema) {
      Table table = new Table();
      table.setName(col.getTable().getName());

      // Retain table alias if present, or use the table name as the alias if not
      if (col.getTable().getAlias() != null) {
        table.setAlias(new Alias(col.getTable().getAlias().getName()));
      } else {
        table.setAlias(new Alias(col.getTable().getName()));
      }

      // Create a new column for the combined schema
      Column newCol = new Column(table, col.getColumnName());
      combinedSchema.add(newCol);
    }

    return combinedSchema;
  }

  /**
   * Combines a left tuple and a right tuple into a single tuple by concatenating their elements.
   *
   * @param left The tuple from the left child.
   * @param right The tuple from the right child.
   * @return A new tuple combining elements from both the left and right tuples.
   */
  private Tuple joinTuples(Tuple left, Tuple right) {
    // Create a new list by concatenating the elements of both tuples
    ArrayList<Integer> combinedData = new ArrayList<>(left.getAllElements());
    combinedData.addAll(right.getAllElements());
    return new Tuple(combinedData); // Return the joined tuple
  }

  /**
   * Evaluates the join condition for a given tuple using the ExpressionEvaluator.
   *
   * @param tuple The tuple for which the condition is evaluated.
   * @return True if the join condition is satisfied, false otherwise.
   */
  private boolean evaluateJoinCondition(Tuple tuple) {
    return expressionEvaluator.evaluate(joinCondition, tuple, this.outputSchema);
  }
}
