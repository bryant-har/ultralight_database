package join_algorithms;

import common.ExpressionEvaluator;
import common.Tuple;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import operator.physical.Operator;

/**
 * The SMJ (Sort-Merge Join) class is an implementation of the Sort-Merge Join algorithm, which
 * joins two sorted input streams based on a join condition.
 */
public class SMJ extends Operator {
  // Left and right child operators
  private final Operator leftChild;
  private final Operator rightChild;

  // Join condition to evaluate for each pair of tuples
  private final Expression joinCondition;

  // Map of table aliases (used by the expression evaluator)
  private final Map<String, String> tableAliases;

  // Evaluator to handle join condition expressions
  private final ExpressionEvaluator expressionEvaluator;

  // Lists of indices that indicate the join key positions in the left and right
  // schemas
  private final List<Integer> leftOrder;
  private final List<Integer> rightOrder;

  // Tuples currently being processed from the left and right child operators
  private Tuple leftTuple;
  private Tuple rightTuple;

  // Buffer to store all matching tuples from the right child for a given left
  // tuple
  private List<Tuple> rightBuffer;
  private int rightBufferIndex; // Tracks position in the right buffer
  private boolean needToLoadRightBuffer; // Flag indicating if we need to load the right buffer

  // Comparator to compare tuples from the left and right children based on join
  // keys
  private final TupleComparator comparator;

  /**
   * Constructs the Sort-Merge Join (SMJ) operator.
   *
   * @param leftChild the left sorted input operator
   * @param rightChild the right sorted input operator
   * @param joinCondition the join condition that defines how to join the tuples
   * @param tableAliases map of table aliases for expression evaluation
   * @param leftSortColumns columns used to sort the left child
   * @param rightSortColumns columns used to sort the right child
   */
  public SMJ(
      Operator leftChild,
      Operator rightChild,
      Expression joinCondition,
      Map<String, String> tableAliases,
      List<Column> leftSortColumns,
      List<Column> rightSortColumns) {

    // Combine the output schemas of the left and right children into a single
    // schema
    super(combineSchemas(leftChild.getOutputSchema(), rightChild.getOutputSchema()));
    System.out.println(super.getOutputSchema()); // Print combined schema for debugging

    this.leftChild = leftChild;
    this.rightChild = rightChild;
    this.joinCondition = joinCondition;
    this.tableAliases = tableAliases;
    this.expressionEvaluator = new ExpressionEvaluator(tableAliases);

    // Initialize lists to store index positions for sorting
    this.leftOrder = new ArrayList<>();
    this.rightOrder = new ArrayList<>();

    // Compute sort indices for both left and right children based on the sort
    // columns
    computeSortIndices(leftSortColumns, rightSortColumns);

    // Initialize tuple comparator based on the sort indices
    this.comparator = new TupleComparator(leftOrder, rightOrder);

    // Load the first tuple from both the left and right children
    this.leftTuple = leftChild.getNextTuple();
    this.rightTuple = rightChild.getNextTuple();

    // Initialize the right buffer to store matching tuples
    this.rightBuffer = new ArrayList<>();
    this.rightBufferIndex = 0;
    this.needToLoadRightBuffer = true;
  }

  /**
   * Computes the indices for sorting based on the columns specified for the left and right
   * children.
   *
   * @param leftCols columns used to sort the left child
   * @param rightCols columns used to sort the right child
   */
  private void computeSortIndices(List<Column> leftCols, List<Column> rightCols) {
    // Compute the sort indices for the left child
    for (Column col : leftCols) {
      String colName = col.getColumnName();
      String tableName = col.getTable().getName();
      for (int i = 0; i < leftChild.getOutputSchema().size(); i++) {
        Column schemaCol = leftChild.getOutputSchema().get(i);
        if (schemaCol.getColumnName().equals(colName)
            && schemaCol.getTable().getName().equals(tableName)) {
          leftOrder.add(i); // Add the index of the column
          break;
        }
      }
    }

    // Compute the sort indices for the right child
    for (Column col : rightCols) {
      String colName = col.getColumnName();
      String tableName = col.getTable().getName();
      for (int i = 0; i < rightChild.getOutputSchema().size(); i++) {
        Column schemaCol = rightChild.getOutputSchema().get(i);
        if (schemaCol.getColumnName().equals(colName)
            && schemaCol.getTable().getName().equals(tableName)) {
          rightOrder.add(i); // Add the index of the column
          break;
        }
      }
    }
  }

  /**
   * Inner class that compares tuples from the left and right children based on the sort indices.
   */
  private class TupleComparator implements Comparator<Tuple> {
    private final List<Integer> leftIndices; // Indices of join keys in the left tuples
    private final List<Integer> rightIndices; // Indices of join keys in the right tuples

    /**
     * Constructs the TupleComparator.
     *
     * @param leftIndices list of sort indices for the left tuples
     * @param rightIndices list of sort indices for the right tuples
     */
    public TupleComparator(List<Integer> leftIndices, List<Integer> rightIndices) {
      this.leftIndices = leftIndices;
      this.rightIndices = rightIndices;
    }

    /**
     * Compares two tuples (one from the left and one from the right) based on the sort order.
     *
     * @param left the tuple from the left child
     * @param right the tuple from the right child
     * @return comparison result (-1 if left < right, 1 if left > right, 0 if equal)
     */
    @Override
    public int compare(Tuple left, Tuple right) {
      for (int i = 0; i < leftIndices.size(); i++) {
        int leftVal = (Integer) left.getElementAtIndex(leftIndices.get(i));
        int rightVal = (Integer) right.getElementAtIndex(rightIndices.get(i));

        int cmp = Integer.compare(leftVal, rightVal);
        if (cmp != 0) {
          return cmp; // Return the comparison result if the values differ
        }
      }
      return 0; // Return 0 if all values are equal
    }
  }

  /**
   * Retrieves the next tuple from the join result.
   *
   * @return the next joined tuple, or null if there are no more tuples
   */
  @Override
  public Tuple getNextTuple() {
    while (leftTuple != null) {
      // Load matching tuples from the right child into the buffer if needed
      if (needToLoadRightBuffer) {
        rightBuffer.clear(); // Clear previous buffer
        rightBufferIndex = 0; // Reset buffer index
        Tuple tempRightTuple = rightTuple;

        // Save matching tuples from the right child
        List<Tuple> savedRightTuples = new ArrayList<>();

        // Read all matching right tuples for the current left tuple
        while (tempRightTuple != null) {
          int cmp = comparator.compare(leftTuple, tempRightTuple);
          if (cmp == 0) {
            rightBuffer.add(tempRightTuple); // Add matching tuple to the buffer
            savedRightTuples.add(tempRightTuple); // Save for resetting
            tempRightTuple = rightChild.getNextTuple(); // Advance right tuple
          } else if (cmp < 0) {
            // If left tuple is smaller, stop loading right tuples
            break;
          } else { // cmp > 0
            // If right tuple is smaller, move to the next right tuple
            tempRightTuple = rightChild.getNextTuple();
          }
        }

        needToLoadRightBuffer = false; // Right buffer is now loaded
        rightTuple = tempRightTuple; // Update right tuple

        // Reset the right child to the position after the matching partition
        rightChild.reset();
        for (Tuple t : savedRightTuples) {
          rightChild.getNextTuple(); // Skip saved tuples
        }
      }

      // If the right buffer is not empty, produce the next join result
      if (rightBufferIndex < rightBuffer.size()) {
        Tuple rightBufTuple = rightBuffer.get(rightBufferIndex);
        rightBufferIndex++;

        // Check if the join condition is satisfied
        if (joinCondition == null || evaluateJoinCondition(leftTuple, rightBufTuple)) {
          return joinTuples(leftTuple, rightBufTuple); // Return joined tuple
        } else {
          continue; // Skip tuple if condition not satisfied
        }
      } else {
        // Move to the next left tuple and reset the right buffer
        leftTuple = leftChild.getNextTuple();
        rightBufferIndex = 0;
        needToLoadRightBuffer = true;

        // Reset right child and get the next right tuple
        rightChild.reset();
        rightTuple = rightChild.getNextTuple();
      }
    }
    return null; // No more tuples to join
  }

  /** Resets the SMJ operator, allowing it to be run again from the start. */
  @Override
  public void reset() {
    leftChild.reset();
    rightChild.reset();
    leftTuple = leftChild.getNextTuple();
    rightTuple = rightChild.getNextTuple();
    rightBuffer.clear(); // Clear the buffer
    rightBufferIndex = 0;
    needToLoadRightBuffer = true; // Indicate that the right buffer needs reloading
  }

  /**
   * Joins a left tuple and a right tuple into a single combined tuple.
   *
   * @param left the tuple from the left child
   * @param right the tuple from the right child
   * @return a new tuple containing elements from both the left and right tuples
   */
  private Tuple joinTuples(Tuple left, Tuple right) {
    ArrayList<Integer> combinedData = new ArrayList<>(left.getAllElements());
    combinedData.addAll(right.getAllElements()); // Combine left and right tuples
    return new Tuple(combinedData);
  }

  /**
   * Evaluates the join condition for a pair of tuples.
   *
   * @param leftTuple the tuple from the left child
   * @param rightTuple the tuple from the right child
   * @return true if the join condition is satisfied, false otherwise
   */
  private boolean evaluateJoinCondition(Tuple leftTuple, Tuple rightTuple) {
    Tuple combinedTuple = joinTuples(leftTuple, rightTuple); // Combine tuples
    return expressionEvaluator.evaluate(joinCondition, combinedTuple, this.outputSchema);
  }

  /**
   * Combines the schemas of the left and right children into a single output schema.
   *
   * @param leftSchema the schema of the left child
   * @param rightSchema the schema of the right child
   * @return a combined schema containing columns from both the left and right children
   */
  private static ArrayList<Column> combineSchemas(
      ArrayList<Column> leftSchema, ArrayList<Column> rightSchema) {
    ArrayList<Column> combinedSchema = new ArrayList<>();
    combinedSchema.addAll(leftSchema); // Add columns from the left schema
    combinedSchema.addAll(rightSchema); // Add columns from the right schema
    return combinedSchema;
  }
}
