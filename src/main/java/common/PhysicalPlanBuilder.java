package common;

import java.util.ArrayList;
import java.util.Map;
import net.sf.jsqlparser.schema.Column;
import operator.logical.*;
import operator.physical.*;

/**
 * The PhysicalPlanBuilder class is responsible for converting a logical query
 * plan
 * into a physical query plan. It implements the Visitor pattern to traverse the
 * logical operator tree and create corresponding physical operators.
 */
public class PhysicalPlanBuilder implements LogicalOperatorVisitor {
  /** The resulting physical operator after visiting a logical operator. */
  private Operator result;

  /** A map of table aliases to their actual table names. */
  private Map<String, String> tableAliases;

  /**
   * Constructs a new PhysicalPlanBuilder.
   *
   * @param tableAliases A map of table aliases to their actual table names.
   */
  public PhysicalPlanBuilder(Map<String, String> tableAliases) {
    this.tableAliases = tableAliases;
  }

  /**
   * Retrieves the result of the most recent visit operation.
   *
   * @return The physical operator created from the last visited logical operator.
   */
  public Operator getResult() {
    return result;
  }

  /**
   * Visits a LogicalScanOperator and creates a corresponding ScanOperator.
   *
   * @param op The LogicalScanOperator to visit.
   */
  @Override
  public void visit(LogicalScanOperator op) {
    ArrayList<Column> schema = new ArrayList<>(op.getSchema());
    result = new ScanOperator(schema, op.getTable().getName());
  }

  /**
   * Visits a LogicalSelectOperator and creates a corresponding SelectOperator.
   *
   * @param op The LogicalSelectOperator to visit.
   */
  @Override
  public void visit(LogicalSelectOperator op) {
    op.getChildren().get(0).accept(this);
    Operator child = result;
    result = new SelectOperator(child, op.getCondition(), tableAliases);
  }

  /**
   * Visits a LogicalProjectOperator and creates a corresponding ProjectOperator.
   *
   * @param op The LogicalProjectOperator to visit.
   */
  @Override
  public void visit(LogicalProjectOperator op) {
    op.getChildren().get(0).accept(this);
    Operator child = result;
    result = new ProjectOperator(child, op.getSelectItems());
  }

  /**
   * Visits a LogicalJoinOperator and creates a corresponding JoinOperator.
   *
   * @param op The LogicalJoinOperator to visit.
   */
  @Override
  public void visit(LogicalJoinOperator op) {
    op.getChildren().get(0).accept(this);
    Operator leftChild = result;
    op.getChildren().get(1).accept(this);
    Operator rightChild = result;
    result = new JoinOperator(leftChild, rightChild, op.getCondition(), tableAliases);
  }

  /**
   * Visits a LogicalSortOperator and creates a corresponding SortOperator.
   *
   * @param op The LogicalSortOperator to visit.
   */
  @Override
  public void visit(LogicalSortOperator op) {
    op.getChildren().get(0).accept(this);
    Operator child = result;
    ArrayList<Column> schema = new ArrayList<>(op.getSchema());
    result = new SortOperator(schema, child, op.getOrderByElements());
  }

  /**
   * Visits a LogicalDuplicateEliminationOperator and creates a corresponding
   * DuplicateElementEliminationOperator.
   *
   * @param op The LogicalDuplicateEliminationOperator to visit.
   */
  @Override
  public void visit(LogicalDuplicateEliminationOperator op) {
    op.getChildren().get(0).accept(this);
    Operator child = result;
    ArrayList<Column> schema = new ArrayList<>(op.getSchema());
    result = new DuplicateElementEliminationOperator(schema, child);
  }
}