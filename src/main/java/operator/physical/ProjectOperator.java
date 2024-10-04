package operator.physical;

import common.Tuple;
import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

/**
 * The ProjectOperator class implements the projection operation in relational algebra. It selects
 * specific columns from the input tuples (as provided by the child operator) and returns tuples
 * with only those selected columns.
 */
public class ProjectOperator extends Operator {
  // The child operator which provides the input tuples
  private Operator child;

  // The list of items to project (these represent the columns that should be
  // selected)
  private List<SelectItem> selectItems;

  // A list of indexes corresponding to the selected columns in the child schema
  private List<Integer> projectionIndexes;

  /**
   * Constructs a ProjectOperator.
   *
   * @param child The input operator that provides the tuples to be projected
   * @param selectItems The list of columns or expressions to select
   */
  public ProjectOperator(Operator child, List<SelectItem> selectItems) {
    super(new ArrayList<>());
    this.child = child;
    this.selectItems = selectItems;
    // Set up the projection mapping between the input schema and the projected
    // schema
    setupProjection();
  }

  /**
   * Initializes the projection indexes based on the select items and the child schema. For each
   * selected column, it finds its corresponding index in the child schema. If all columns are
   * selected, it includes all columns from the child schema.
   */
  private void setupProjection() {
    projectionIndexes = new ArrayList<>();
    ArrayList<Column> childSchema = child.getOutputSchema();
    ArrayList<Column> newSchema = new ArrayList<>();

    // Iterate through each select item (representing a column or expression)
    for (SelectItem item : selectItems) {
      // If the select item represents all columns (SELECT *), add all columns from
      // the child schema
      if (item instanceof AllColumns) {
        for (int i = 0; i < childSchema.size(); i++) {
          projectionIndexes.add(i);
          newSchema.add(childSchema.get(i));
        }
      }
      // If the select item is a specific column
      else if (item instanceof SelectExpressionItem) {
        SelectExpressionItem sei = (SelectExpressionItem) item;
        if (sei.getExpression() instanceof Column) {
          Column col = (Column) sei.getExpression();
          String columnName = col.getColumnName();
          boolean found = false;
          // Find the index of the column in the child schema
          for (int i = 0; i < childSchema.size(); i++) {
            if (childSchema.get(i).getColumnName().equals(columnName)) {
              projectionIndexes.add(i);
              newSchema.add(childSchema.get(i));
              found = true;
              break;
            }
          }
          // Throw an error if the column is not found in the child schema
          if (!found) {
            throw new IllegalArgumentException("Column " + columnName + " not found in schema");
          }
        } else {
          // Throw an error if a non-column expression is encountered (e.g., aggregate
          // functions)
          throw new IllegalArgumentException("Only column expressions are supported in SELECT");
        }
      }
    }
    // Set the output schema to the newly constructed projected schema
    this.outputSchema = newSchema;
  }

  /** Resets the operator, allowing the child operator to be re-executed. */
  @Override
  public void reset() {
    child.reset();
  }

  /**
   * Retrieves the next tuple from the child operator, applies the projection, and returns a tuple
   * with only the selected columns.
   *
   * @return The projected tuple, or null if no more tuples are available.
   */
  @Override
  public Tuple getNextTuple() {
    // Get the next tuple from the child operator
    Tuple childTuple = child.getNextTuple();
    if (childTuple == null) {
      return null; // No more tuples available
    }

    // Create a new list to hold the projected values
    ArrayList<Integer> projectedValues = new ArrayList<>();
    for (int index : projectionIndexes) {
      // Ensure the index is valid, then add the value from the corresponding column
      if (index >= 0 && index < childTuple.getAllElements().size()) {
        projectedValues.add(childTuple.getElementAtIndex(index));
      } else {
        // Throw an error if the projection index is invalid (out of bounds)
        throw new IndexOutOfBoundsException("Invalid projection index: " + index);
      }
    }
    // Return the tuple with only the projected columns
    return new Tuple(projectedValues);
  }
}
