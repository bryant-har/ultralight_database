package operator.physical;

import common.Tuple;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

/**
 * SortOperator class implements sorting functionality for database operations. It sorts tuples
 * based on specified ORDER BY elements.
 */
public class SortOperator extends Operator {
  private final Operator childOperator;
  private final List<OrderByElement> orderByElements;
  private final ArrayList<Column> outputSchema;
  private List<Tuple> sortedTuples;
  private int currentIndex;

  /**
   * Constructor for SortOperator.
   *
   * @param outputSchema The schema of the output tuples.
   * @param childOperator The child operator providing input tuples.
   * @param orderByElements The list of ORDER BY elements for sorting.
   */
  public SortOperator(
      ArrayList<Column> outputSchema,
      Operator childOperator,
      List<OrderByElement> orderByElements) {
    super(outputSchema);
    this.childOperator = childOperator;
    this.orderByElements = orderByElements;
    this.outputSchema = outputSchema;
    this.sortedTuples = null;
    this.currentIndex = 0;
  }

  /**
   * Retrieves the next tuple in the sorted order.
   *
   * @return The next Tuple, or null if no more tuples are available.
   */
  @Override
  public Tuple getNextTuple() {
    if (sortedTuples == null) {
      bufferAndSortTuples();
    }
    if (currentIndex < sortedTuples.size()) {
      return sortedTuples.get(currentIndex++);
    }
    return null;
  }

  /** Buffers all tuples from the child operator and sorts them. */
  private void bufferAndSortTuples() {
    sortedTuples = new ArrayList<>();
    Tuple tuple;
    while ((tuple = childOperator.getNextTuple()) != null) {
      sortedTuples.add(tuple);
    }
    Collections.sort(sortedTuples, new TupleComparator());
  }

  /**
   * Inner class implementing Comparator for Tuple objects. Used for sorting tuples based on ORDER
   * BY elements.
   */
  private class TupleComparator implements Comparator<Tuple> {
    @Override
    public int compare(Tuple t1, Tuple t2) {
      for (OrderByElement orderByElement : orderByElements) {
        Expression expr = orderByElement.getExpression();
        if (expr instanceof Column) {
          Column column = (Column) expr;
          int columnIndex = findColumnIndex(column);
          if (columnIndex != -1) {
            int value1 = t1.getElementAtIndex(columnIndex);
            int value2 = t2.getElementAtIndex(columnIndex);
            int comparison = Integer.compare(value1, value2);
            if (comparison != 0) {
              return orderByElement.isAsc() ? comparison : -comparison;
            }
          }
        }
      }
      return 0;
    }

    /**
     * Finds the index of a column in the output schema.
     *
     * @param column The column to find.
     * @return The index of the column, or -1 if not found.
     */
    private int findColumnIndex(Column column) {
      String columnName = column.getColumnName();
      for (int i = 0; i < outputSchema.size(); i++) {
        if (outputSchema.get(i).getColumnName().equalsIgnoreCase(columnName)) {
          return i;
        }
      }
      return -1; // Column not found
    }
  }

  /** Resets the operator to its initial state. */
  @Override
  public void reset() {
    childOperator.reset();
    sortedTuples = null;
    currentIndex = 0;
  }

  /**
   * Retrieves all tuples in sorted order.
   *
   * @return A list of all tuples, sorted according to ORDER BY elements.
   */
  @Override
  public List<Tuple> getAllTuples() {
    if (sortedTuples == null) {
      bufferAndSortTuples();
    }
    return new ArrayList<>(sortedTuples);
  }

  /**
   * Dumps all tuples to the specified PrintStream.
   *
   * @param printStream The PrintStream to dump the tuples to.
   */
  @Override
  public void dump(PrintStream printStream) {
    Tuple t;
    while ((t = this.getNextTuple()) != null) {
      printStream.println(t);
    }
  }
}
