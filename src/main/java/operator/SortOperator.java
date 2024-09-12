package operator;

import common.Tuple;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class SortOperator extends Operator {
  private final Operator childOperator;
  private final List<OrderByElement> orderByElements;
  private final ArrayList<Column> outputSchema;
  private List<Tuple> sortedTuples;
  private int currentIndex;

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

  private void bufferAndSortTuples() {
    sortedTuples = new ArrayList<>();
    Tuple tuple;
    while ((tuple = childOperator.getNextTuple()) != null) {
      sortedTuples.add(tuple);
    }
    Collections.sort(sortedTuples, new TupleComparator());
  }

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

  @Override
  public void reset() {
    childOperator.reset();
    sortedTuples = null;
    currentIndex = 0;
  }

  @Override
  public List<Tuple> getAllTuples() {
    if (sortedTuples == null) {
      bufferAndSortTuples();
    }
    return new ArrayList<>(sortedTuples);
  }

  @Override
  public void dump(PrintStream printStream) {
    Tuple t;
    while ((t = this.getNextTuple()) != null) {
      printStream.println(t);
    }
  }
}
