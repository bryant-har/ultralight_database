package operator.logical;

import common.LogicalOperatorVisitor;
import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class LogicalScanOperator extends LogicalOperator {
  private Table table;

  public LogicalScanOperator(Table table, List<Column> schema) {
    super(schema);
    this.table = table;
  }

  public Table getTable() {
    return table;
  }

  @Override
  public void accept(LogicalOperatorVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public List<LogicalOperator> getChildren() {
    return new ArrayList<>(); // Scan is a leaf node, so it has no children
  }

  @Override
  public String toString() {
    return "LogicalScanOperator: " + table.getName();
  }
}
