package operator.physical;

import common.Tuple;
import file_management.TupleWriter;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.schema.Column;

/**
 * Abstract class to represent relational operators. Every operator has a reference to an
 * outputSchema which represents the schema of the output tuples from the operator.
 */
public abstract class Operator {

  protected ArrayList<Column> outputSchema;
  protected int currentIndex; // Track current tuple index

  public Operator(ArrayList<Column> outputSchema) {
    this.outputSchema = outputSchema;
    this.currentIndex = 0;
  }

  public ArrayList<Column> getOutputSchema() {
    return outputSchema;
  }

  /** Resets cursor on the operator to the beginning */
  public abstract void reset();

  /**
   * Resets the operator to a specific tuple index. Default implementation throws
   * UnsupportedOperationException. Should be overridden by operators that support index-based reset
   * (e.g. Sort).
   *
   * @param index The tuple index to reset to
   */
  public void reset(int index) {
    throw new UnsupportedOperationException("This operator does not support index-based reset");
  }

  /**
   * Get next tuple from operator
   *
   * @return next Tuple, or null if we are at the end
   */
  public abstract Tuple getNextTuple();

  /**
   * Gets the current index in the tuple stream.
   *
   * @return The current tuple index
   */
  public int getCurrentIndex() {
    return currentIndex;
  }

  /**
   * Collects all tuples of this operator.
   *
   * @return A list of Tuples.
   */
  public List<Tuple> getAllTuples() {
    Tuple t;
    List<Tuple> tuples = new ArrayList<>();
    while ((t = getNextTuple()) != null) {
      tuples.add(t);
    }
    return tuples;
  }

  public void dump(PrintStream printStream) {
    Tuple t;
    while ((t = this.getNextTuple()) != null) {
      printStream.println(t);
    }
  }

  public void dump(TupleWriter tw) throws Exception {
    Tuple t;
    while ((t = this.getNextTuple()) != null) {
      tw.writeTuple(t.toIntArray());
    }
  }
}
