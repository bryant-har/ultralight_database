package operator;

import common.Tuple;
import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.schema.Column;

/** Operator that eliminates duplicate tuples from a sorted input. */
public class DuplicateElementEliminationOperator extends Operator {
  private Operator childOperator;
  private Tuple lastTuple;

  /**
   * Constructor for DuplicateElementEliminationOperator.
   *
   * @param outputSchema The output schema for this operator
   * @param childOperator The child operator providing input tuples
   */
  public DuplicateElementEliminationOperator(
      ArrayList<Column> outputSchema, Operator childOperator) {
    super(outputSchema);
    this.childOperator = childOperator;
    this.lastTuple = null;
  }

  /** Resets the operator state. */
  @Override
  public void reset() {
    childOperator.reset();
    lastTuple = null;
  }

  /**
   * Gets the next non-duplicate tuple.
   *
   * @return The next unique tuple, or null if there are no more unique tuples
   */
  @Override
  public Tuple getNextTuple() {
    Tuple currentTuple;
    while ((currentTuple = childOperator.getNextTuple()) != null) {
      if (lastTuple == null || !currentTuple.equals(lastTuple)) {
        lastTuple = currentTuple;
        return currentTuple;
      }
    }
    return null;
  }

  /**
   * Gets all unique tuples from the child operator.
   *
   * @return A list of all unique tuples
   */
  @Override
  public List<Tuple> getAllTuples() {
    List<Tuple> uniqueTuples = new ArrayList<>();
    Tuple tuple;
    while ((tuple = getNextTuple()) != null) {
      uniqueTuples.add(tuple);
    }
    return uniqueTuples;
  }
}
