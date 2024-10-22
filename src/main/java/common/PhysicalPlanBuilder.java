package common;

import java.util.ArrayList;
import java.util.Map;

import join_algorithms.BNLJ;
import net.sf.jsqlparser.schema.Column;
import operator.logical.*;
import operator.physical.*;
import java.io.*;
import java.io.IOException;


/**
 * The PhysicalPlanBuilder class is responsible for converting a logical query plan into a physical
 * query plan. It implements the Visitor pattern to traverse the logical operator tree and create
 * corresponding physical operators.
 * 
 */

public class PhysicalPlanBuilder implements LogicalOperatorVisitor {
  /** The resulting physical operator after visiting a logical operator. */
  private Operator result;

  /** A map of table aliases to their actual table names. */
  private Map<String, String> tableAliases;
  private static final String INPUT_DIR = "src/test/resources/samples/input";
  private static final String CONFIG_FILE = INPUT_DIR + "/plan_builder_config.txt";

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

    int type_of_join = 0; 
    int number_of_pages_for_BNLJ = 1;

    try (BufferedReader reader = new BufferedReader(new FileReader(CONFIG_FILE))) {
      type_of_join = Integer.parseInt(reader.readLine().trim());
      number_of_pages_for_BNLJ = Integer.parseInt(reader.readLine().trim());

    } catch (IOException e) {
      e.printStackTrace();
    }
    // For P2, with BNLJ we switch this from the old JoinOperator TNLJ, TODO we need to choose when to use TNLJ vs BNJL

    if(type_of_join == 1){
      result = new BNLJ(leftChild, rightChild, op.getCondition(), tableAliases, number_of_pages_for_BNLJ);
    } else if (type_of_join == 0){
      result = new JoinOperator(leftChild, rightChild, op.getCondition(), tableAliases);
    }
    else if (type_of_join == 2){
      // insert SMJ here TODO 
    }
    else{
      throw (new UnsupportedOperationException("Join type not supported"));
    }
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
