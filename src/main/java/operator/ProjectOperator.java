package operator;

import common.Tuple;
import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class ProjectOperator extends Operator {
  private Operator child;
  private List<SelectItem> selectItems;
  private List<Integer> projectionIndexes;

  public ProjectOperator(Operator child, List<SelectItem> selectItems) {
    super(new ArrayList<>());
    this.child = child;
    this.selectItems = selectItems;
    setupProjection();
  }

  private void setupProjection() {
    projectionIndexes = new ArrayList<>();
    ArrayList<Column> childSchema = child.getOutputSchema();
    ArrayList<Column> newSchema = new ArrayList<>();

    for (SelectItem item : selectItems) {
      if (item instanceof AllColumns) {
        for (int i = 0; i < childSchema.size(); i++) {
          projectionIndexes.add(i);
          newSchema.add(childSchema.get(i));
        }
      } else if (item instanceof SelectExpressionItem) {
        SelectExpressionItem sei = (SelectExpressionItem) item;
        if (sei.getExpression() instanceof Column) {
          Column col = (Column) sei.getExpression();
          String columnName = col.getColumnName();
          boolean found = false;
          for (int i = 0; i < childSchema.size(); i++) {
            if (childSchema.get(i).getColumnName().equals(columnName)) {
              projectionIndexes.add(i);
              newSchema.add(childSchema.get(i));
              found = true;
              break;
            }
          }
          if (!found) {
            throw new IllegalArgumentException("Column " + columnName + " not found in schema");
          }
        } else {
          throw new IllegalArgumentException("Only column expressions are supported in SELECT");
        }
      }
    }
    this.outputSchema = newSchema;
  }

  @Override
  public void reset() {
    child.reset();
  }

  @Override
  public Tuple getNextTuple() {
    Tuple childTuple = child.getNextTuple();
    if (childTuple == null) {
      return null;
    }

    ArrayList<Integer> projectedValues = new ArrayList<>();
    for (int index : projectionIndexes) {
      if (index >= 0 && index < childTuple.getAllElements().size()) {
        projectedValues.add(childTuple.getElementAtIndex(index));
      } else {
        throw new IndexOutOfBoundsException("Invalid projection index: " + index);
      }
    }
    return new Tuple(projectedValues);
  }
}
