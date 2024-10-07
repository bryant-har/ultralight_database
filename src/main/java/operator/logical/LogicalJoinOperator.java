package operator.logical;

import common.LogicalOperatorVisitor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class LogicalJoinOperator extends LogicalOperator {
  private LogicalOperator leftChild;
  private LogicalOperator rightChild;
  private Expression condition;

  public LogicalJoinOperator(
      LogicalOperator leftChild, LogicalOperator rightChild, Expression condition) {
    super(combineAndQualifySchemas(leftChild, rightChild));
    this.leftChild = leftChild;
    this.rightChild = rightChild;
    this.condition = condition;
  }

  private static List<Column> combineAndQualifySchemas(
      LogicalOperator left, LogicalOperator right) {
    List<Column> combinedSchema = new ArrayList<>();
    addQualifiedColumns(combinedSchema, left);
    addQualifiedColumns(combinedSchema, right);
    return combinedSchema;
  }

  private static void addQualifiedColumns(List<Column> schema, LogicalOperator operator) {
    for (Column col : operator.getSchema()) {
      Table table = col.getTable();
      if (table == null || table.getName() == null) {
        // If the column doesn't have a table, use the operator's name or alias
        table = new Table(operator.getClass().getSimpleName());
      }
      schema.add(new Column(table, col.getColumnName()));
    }
  }

  public Expression getCondition() {
    return condition;
  }

  @Override
  public void accept(LogicalOperatorVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public List<LogicalOperator> getChildren() {
    return Arrays.asList(leftChild, rightChild);
  }

  @Override
  public String toString() {
    return "LogicalJoinOperator: " + (condition != null ? condition.toString() : "Cross Product");
  }
}
