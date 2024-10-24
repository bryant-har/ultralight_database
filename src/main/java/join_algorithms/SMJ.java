package join_algorithms;

import common.ExpressionEvaluator;
import common.Tuple;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import operator.physical.Operator;

public class SMJ extends Operator {
  private final Operator leftChild;
  private final Operator rightChild;
  private final Expression joinCondition;
  private final Map<String, String> tableAliases;
  private final ExpressionEvaluator expressionEvaluator;
  private final List<Integer> leftOrder;
  private final List<Integer> rightOrder;

  private Tuple leftTuple;
  private Tuple rightTuple;
  private List<Tuple> rightBuffer; // Buffer to store matching right tuples
  private int rightBufferIndex;
  private boolean needToLoadRightBuffer;

  private final TupleComparator comparator;

  public SMJ(
      Operator leftChild,
      Operator rightChild,
      Expression joinCondition,
      Map<String, String> tableAliases,
      List<Column> leftSortColumns,
      List<Column> rightSortColumns) {

    super(combineSchemas(leftChild.getOutputSchema(), rightChild.getOutputSchema()));
    System.out.println(super.getOutputSchema());

    this.leftChild = leftChild;
    this.rightChild = rightChild;
    this.joinCondition = joinCondition;
    this.tableAliases = tableAliases;
    this.expressionEvaluator = new ExpressionEvaluator(tableAliases);

    // Convert column lists to index lists
    this.leftOrder = new ArrayList<>();
    this.rightOrder = new ArrayList<>();
    computeSortIndices(leftSortColumns, rightSortColumns);

    // Initialize state
    this.comparator = new TupleComparator(leftOrder, rightOrder);

    // Initialize tuples
    this.leftTuple = leftChild.getNextTuple();
    this.rightTuple = rightChild.getNextTuple();

    // Initialize right buffer
    this.rightBuffer = new ArrayList<>();
    this.rightBufferIndex = 0;
    this.needToLoadRightBuffer = true;
  }

  private void computeSortIndices(List<Column> leftCols, List<Column> rightCols) {
    for (Column col : leftCols) {
      String colName = col.getColumnName();
      String tableName = col.getTable().getName();
      for (int i = 0; i < leftChild.getOutputSchema().size(); i++) {
        Column schemaCol = leftChild.getOutputSchema().get(i);
        if (schemaCol.getColumnName().equals(colName) && schemaCol.getTable().getName().equals(tableName)) {
          leftOrder.add(i);
          break;
        }
      }
    }

    for (Column col : rightCols) {
      String colName = col.getColumnName();
      String tableName = col.getTable().getName();
      for (int i = 0; i < rightChild.getOutputSchema().size(); i++) {
        Column schemaCol = rightChild.getOutputSchema().get(i);
        if (schemaCol.getColumnName().equals(colName) && schemaCol.getTable().getName().equals(tableName)) {
          rightOrder.add(i);
          break;
        }
      }
    }
  }

  private class TupleComparator implements Comparator<Tuple> {
    private final List<Integer> leftIndices;
    private final List<Integer> rightIndices;

    public TupleComparator(List<Integer> leftIndices, List<Integer> rightIndices) {
      this.leftIndices = leftIndices;
      this.rightIndices = rightIndices;
    }

    @Override
    public int compare(Tuple left, Tuple right) {
      for (int i = 0; i < leftIndices.size(); i++) {
        // Compare the correct values based on the computed sort indices
        int leftVal = (Integer) left.getElementAtIndex(leftIndices.get(i));
        int rightVal = (Integer) right.getElementAtIndex(rightIndices.get(i));

        int cmp = Integer.compare(leftVal, rightVal);
        if (cmp != 0) {
          return cmp;
        }
      }
      return 0;
    }

  }

  @Override
  public Tuple getNextTuple() {
    while (leftTuple != null) {
      // Load right buffer if needed
      if (needToLoadRightBuffer) {
        rightBuffer.clear();
        rightBufferIndex = 0;
        Tuple tempRightTuple = rightTuple;

        // Save the current position of the right child
        List<Tuple> savedRightTuples = new ArrayList<>();

        // Read all matching right tuples
        while (tempRightTuple != null) {
          int cmp = comparator.compare(leftTuple, tempRightTuple);
          if (cmp == 0) {
            rightBuffer.add(tempRightTuple);
            savedRightTuples.add(tempRightTuple);
            tempRightTuple = rightChild.getNextTuple();
          } else if (cmp < 0) {
            // Left tuple is smaller, no more matching tuples
            break;
          } else { // cmp > 0
            // Right tuple is smaller, advance right tuple
            tempRightTuple = rightChild.getNextTuple();
          }
        }

        needToLoadRightBuffer = false;
        rightTuple = tempRightTuple;

        // Reset right child to the position after the matching partition
        rightChild.reset();
        for (Tuple t : savedRightTuples) {
          rightChild.getNextTuple(); // Advance rightChild to skip saved tuples
        }
      }

      // If right buffer is not empty, produce join tuples
      if (rightBufferIndex < rightBuffer.size()) {
        Tuple rightBufTuple = rightBuffer.get(rightBufferIndex);
        rightBufferIndex++;

        // Evaluate join condition if it exists
        if (joinCondition == null || evaluateJoinCondition(leftTuple, rightBufTuple)) {
          return joinTuples(leftTuple, rightBufTuple);
        } else {
          continue;
        }
      } else {
        // Move to next left tuple and reset right buffer
        leftTuple = leftChild.getNextTuple();
        rightBufferIndex = 0;
        needToLoadRightBuffer = true;

        // Reset right child to start from previous position
        rightChild.reset();
        rightTuple = rightChild.getNextTuple();
      }
    }
    return null; // No more tuples
  }

  @Override
  public void reset() {
    leftChild.reset();
    rightChild.reset();
    leftTuple = leftChild.getNextTuple();
    rightTuple = rightChild.getNextTuple();
    rightBuffer.clear();
    rightBufferIndex = 0;
    needToLoadRightBuffer = true;
  }

  private Tuple joinTuples(Tuple left, Tuple right) {
    ArrayList<Integer> combinedData = new ArrayList<>(left.getAllElements());
    combinedData.addAll(right.getAllElements());
    return new Tuple(combinedData);
  }

  private boolean evaluateJoinCondition(Tuple leftTuple, Tuple rightTuple) {
    Tuple combinedTuple = joinTuples(leftTuple, rightTuple);
    return expressionEvaluator.evaluate(joinCondition, combinedTuple, this.outputSchema);
  }

  private static ArrayList<Column> combineSchemas(
      ArrayList<Column> leftSchema, ArrayList<Column> rightSchema) {
    ArrayList<Column> combinedSchema = new ArrayList<>();

    combinedSchema.addAll(leftSchema);
    combinedSchema.addAll(rightSchema);

    return combinedSchema;
  }
}
