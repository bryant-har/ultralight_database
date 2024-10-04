package operator.logical;

import common.LogicalOperatorVisitor;
import java.util.Arrays;
import java.util.List;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectItem;

public class LogicalProjectOperator extends LogicalOperator {
  private LogicalOperator child;
  private List<SelectItem> selectItems;

  public LogicalProjectOperator(
      LogicalOperator child, List<SelectItem> selectItems, List<Column> schema) {
    super(schema);
    this.child = child;
    this.selectItems = selectItems;
  }

  public List<SelectItem> getSelectItems() {
    return selectItems;
  }

  @Override
  public void accept(LogicalOperatorVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public List<LogicalOperator> getChildren() {
    return Arrays.asList(child);
  }

  @Override
  public String toString() {
    return "LogicalProjectOperator: " + selectItems.toString();
  }
}
