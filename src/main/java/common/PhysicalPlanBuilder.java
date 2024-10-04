package common;

import java.util.ArrayList;
import java.util.Map;
import net.sf.jsqlparser.schema.Column;
import operator.logical.*;
import operator.physical.*;

public class PhysicalPlanBuilder implements LogicalOperatorVisitor {
  private Operator result;
  private Map<String, String> tableAliases;

  public PhysicalPlanBuilder(Map<String, String> tableAliases) {
    this.tableAliases = tableAliases;
  }

  public Operator getResult() {
    return result;
  }

  @Override
  public void visit(LogicalScanOperator op) {
    ArrayList<Column> schema = new ArrayList<>(op.getSchema());
    result = new ScanOperator(schema, op.getTable().getName());
  }

  @Override
  public void visit(LogicalSelectOperator op) {
    op.getChildren().get(0).accept(this);
    Operator child = result;
    result = new SelectOperator(child, op.getCondition(), tableAliases);
  }

  @Override
  public void visit(LogicalProjectOperator op) {
    op.getChildren().get(0).accept(this);
    Operator child = result;
    result = new ProjectOperator(child, op.getSelectItems());
  }

  @Override
  public void visit(LogicalJoinOperator op) {
    op.getChildren().get(0).accept(this);
    Operator leftChild = result;
    op.getChildren().get(1).accept(this);
    Operator rightChild = result;
    result = new JoinOperator(leftChild, rightChild, op.getCondition(), tableAliases);
  }

  @Override
  public void visit(LogicalSortOperator op) {
    op.getChildren().get(0).accept(this);
    Operator child = result;
    ArrayList<Column> schema = new ArrayList<>(op.getSchema());
    result = new SortOperator(schema, child, op.getOrderByElements());
  }

  @Override
  public void visit(LogicalDuplicateEliminationOperator op) {
    op.getChildren().get(0).accept(this);
    Operator child = result;
    ArrayList<Column> schema = new ArrayList<>(op.getSchema());
    result = new DuplicateElementEliminationOperator(schema, child);
  }
}
