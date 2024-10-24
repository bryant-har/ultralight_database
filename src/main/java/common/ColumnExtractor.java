package common;

import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;

/** Extracts columns from join expressions for sort-merge join. */
public class ColumnExtractor extends ExpressionVisitorAdapter {
  private final List<Column> leftColumns = new ArrayList<>();
  private final List<Column> rightColumns = new ArrayList<>();
  private final String leftTableName;
  private final String rightTableName;

  public ColumnExtractor(String leftTableName, String rightTableName) {
    this.leftTableName = leftTableName;
    this.rightTableName = rightTableName;
  }

  @Override
  public void visit(EqualsTo equalsTo) {
    if (equalsTo.getLeftExpression() instanceof Column
        && equalsTo.getRightExpression() instanceof Column) {

      Column leftCol = (Column) equalsTo.getLeftExpression();
      Column rightCol = (Column) equalsTo.getRightExpression();

      String leftTable = getTableName(leftCol);
      String rightTable = getTableName(rightCol);

      if (leftTable.equals(leftTableName) && rightTable.equals(rightTableName)) {
        leftColumns.add(leftCol);
        rightColumns.add(rightCol);
      } else if (leftTable.equals(rightTableName) && rightTable.equals(leftTableName)) {
        leftColumns.add(rightCol);
        rightColumns.add(leftCol);
      }
    }
  }

  @Override
  public void visit(AndExpression andExpression) {
    andExpression.getLeftExpression().accept(this);
    andExpression.getRightExpression().accept(this);
  }

  private String getTableName(Column col) {
    if (col.getTable().getAlias() != null) {
      return col.getTable().getAlias().getName();
    }
    return col.getTable().getName();
  }

  public List<Column> getLeftColumns() {
    return leftColumns;
  }

  public List<Column> getRightColumns() {
    return rightColumns;
  }
}
