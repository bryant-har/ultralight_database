package operator.physical;

import common.Tuple;
import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class ProjectOperator extends Operator {
  private final Operator child;
  private final List<SelectItem> selectItems;
  private final List<Integer> projectionIndexes;

  public ProjectOperator(
      Operator child, List<Column> canonicalSchema, List<SelectItem> selectItems) {
    super(new ArrayList<>());
    this.child = child;
    this.selectItems = selectItems;
    this.projectionIndexes = new ArrayList<>();
    setupProjection(canonicalSchema);
  }

  private void setupProjection(List<Column> canonicalSchema) {
    ArrayList<Column> childSchema = child.getOutputSchema();
    ArrayList<Column> newSchema = new ArrayList<>();

    for (SelectItem item : selectItems) {
      if (item instanceof AllColumns) {
        for (int i = 0; i < canonicalSchema.size(); i++) {
          Column canonicalCol = canonicalSchema.get(i);
          boolean found = false;
          for (int j = 0; j < childSchema.size(); j++) {
            Column childCol = childSchema.get(j);
            if (matchesColumn(childCol, canonicalCol)) {
              projectionIndexes.add(j);
              newSchema.add(childCol);
              found = true;
              break;
            }
          }
          if (!found) {
            throw new IllegalArgumentException(
                "Column " + canonicalCol.getFullyQualifiedName() + " not found in schema");
          }
        }
      } else if (item instanceof SelectExpressionItem) {
        SelectExpressionItem sei = (SelectExpressionItem) item;
        if (sei.getExpression() instanceof Column) {
          Column targetCol = (Column) sei.getExpression();
          boolean found = false;
          for (int i = 0; i < childSchema.size(); i++) {
            Column schemaCol = childSchema.get(i);
            if (matchesColumn(schemaCol, targetCol)) {
              projectionIndexes.add(i);
              if (sei.getAlias() != null) {
                // Create new column with alias
                schemaCol = new Column(schemaCol.getTable(), sei.getAlias().getName());
              }
              newSchema.add(schemaCol);
              found = true;
              break;
            }
          }
          if (!found) {
            throw new IllegalArgumentException(
                "Column " + targetCol.getFullyQualifiedName() + " not found in schema");
          }
        }
      }
    }
    this.outputSchema = newSchema;
  }

  private boolean matchesColumn(Column schemaCol, Column targetCol) {
    // Match by column name
    if (!schemaCol.getColumnName().equals(targetCol.getColumnName())) {
      return false;
    }

    // If target has a table reference, check it matches either table name or alias
    if (targetCol.getTable() != null) {
      String targetTable = targetCol.getTable().getName();
      String targetAlias =
          targetCol.getTable().getAlias() != null
              ? targetCol.getTable().getAlias().getName()
              : targetTable;

      String schemaTable = schemaCol.getTable().getName();
      String schemaAlias =
          schemaCol.getTable().getAlias() != null
              ? schemaCol.getTable().getAlias().getName()
              : schemaTable;

      return targetAlias.equals(schemaAlias) || targetAlias.equals(schemaTable);
    }

    return true;
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
      projectedValues.add(childTuple.getElementAtIndex(index));
    }
    return new Tuple(projectedValues);
  }
}
