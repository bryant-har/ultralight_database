package common;

import operator.logical.*;

/**
 * Visitor interface for logical operators in the query plan. This interface defines visit methods
 * for each type of logical operator.
 */
public interface LogicalOperatorVisitor {

  /**
   * Visit a LogicalScanOperator.
   *
   * @param scanOp The LogicalScanOperator to visit.
   */
  void visit(LogicalScanOperator scanOp);

  /**
   * Visit a LogicalSelectOperator.
   *
   * @param selectOp The LogicalSelectOperator to visit.
   */
  void visit(LogicalSelectOperator selectOp);

  /**
   * Visit a LogicalProjectOperator.
   *
   * @param projectOp The LogicalProjectOperator to visit.
   */
  void visit(LogicalProjectOperator projectOp);

  /**
   * Visit a LogicalJoinOperator.
   *
   * @param joinOp The LogicalJoinOperator to visit.
   */
  void visit(LogicalJoinOperator joinOp);

  /**
   * Visit a LogicalSortOperator.
   *
   * @param sortOp The LogicalSortOperator to visit.
   */
  void visit(LogicalSortOperator sortOp);

  /**
   * Visit a LogicalDuplicateEliminationOperator.
   *
   * @param distinctOp The LogicalDuplicateEliminationOperator to visit.
   */
  void visit(LogicalDuplicateEliminationOperator distinctOp);
}
