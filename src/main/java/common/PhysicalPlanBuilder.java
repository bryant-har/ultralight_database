package common;

import java.io.*;
import java.util.ArrayList;
import java.util.Map;
import join_algorithms.BNLJ;
import join_algorithms.SMJ;
import net.sf.jsqlparser.schema.Column;
import operator.logical.*;
import operator.physical.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * PhysicalPlanBuilder is responsible for converting a logical query plan into a physical execution
 * plan. It implements the Visitor pattern to traverse the logical operator tree and create
 * corresponding physical operators based on configuration settings.
 *
 * <p>The configuration is read from a file that specifies: - Join method (TNLJ, BNLJ, or SMJ) and
 * buffer pages for BNLJ - Sort method (in-memory or external) and buffer pages for external sort
 *
 * <p>Configuration file format: Line 1: [join_method] [buffer_pages_if_bnlj] Line 2: [sort_method]
 * [buffer_pages_if_external]
 *
 * <p>Where: - join_method: 0=TNLJ, 1=BNLJ, 2=SMJ - sort_method: 0=in-memory, 1=external
 */
public class PhysicalPlanBuilder implements LogicalOperatorVisitor {
  private static final Logger logger = LogManager.getLogger();

  /** Join method constants */
  private static final int TNLJ = 0; // Tuple Nested Loop Join

  private static final int BNLJ = 1; // Block Nested Loop Join
  private static final int SMJ = 2; // Sort Merge Join

  /** Sort method constants */
  private static final int IN_MEMORY_SORT = 0; // In-memory sorting

  private static final int EXTERNAL_SORT = 1; // External merge sort

  /** The resulting physical operator after visiting a logical operator */
  private Operator result;

  /** Map of table aliases to their actual table names */
  private Map<String, String> tableAliases;

  /** Directory for temporary files used in external sort */
  private String tempDir;

  /** Configuration settings read from config file */
  private int joinMethod; // Which join algorithm to use

  private int joinBufferPages; // Number of buffer pages for BNLJ
  private int sortMethod; // Which sort algorithm to use
  private int sortBufferPages; // Number of buffer pages for external sort

  /**
   * Constructs a new PhysicalPlanBuilder.
   *
   * @param tableAliases Map of table aliases to actual table names
   * @param configFile Path to the configuration file
   * @param tempDir Directory for temporary files used in external operations
   */
  public PhysicalPlanBuilder(Map<String, String> tableAliases, String configFile, String tempDir) {
    this.tableAliases = tableAliases;
    this.tempDir = tempDir;
    readConfig(configFile);
  }

  /**
   * Reads and validates the configuration file.
   *
   * @param configFile Path to the configuration file
   * @throws RuntimeException if configuration file cannot be read or is invalid
   */
  private void readConfig(String configFile) {
    try (BufferedReader br = new BufferedReader(new FileReader(configFile))) {
      // Read join configuration
      String[] joinConfig = br.readLine().trim().split("\\s+");
      joinMethod = Integer.parseInt(joinConfig[0]);
      if (joinMethod == BNLJ) {
        if (joinConfig.length < 2) {
          throw new IllegalArgumentException("BNLJ requires buffer pages specification");
        }
        joinBufferPages = Integer.parseInt(joinConfig[1]);
      }

      // Read sort configuration
      String[] sortConfig = br.readLine().trim().split("\\s+");
      sortMethod = Integer.parseInt(sortConfig[0]);
      if (sortMethod == EXTERNAL_SORT) {
        if (sortConfig.length < 2) {
          throw new IllegalArgumentException("External sort requires buffer pages specification");
        }
        sortBufferPages = Integer.parseInt(sortConfig[1]);
        if (sortBufferPages < 3) {
          throw new IllegalArgumentException("External sort requires at least 3 buffer pages");
        }
      }
    } catch (IOException e) {
      logger.error("Error reading config file: " + e.getMessage());
      throw new RuntimeException("Failed to read configuration file", e);
    }
  }

  /**
   * Returns the physical operator created from the last visited logical operator.
   *
   * @return The resulting physical operator
   */
  public Operator getResult() {
    return result;
  }

  /**
   * Visits a LogicalScanOperator and creates a corresponding ScanOperator.
   *
   * @param op The LogicalScanOperator to visit
   */
  @Override
  public void visit(LogicalScanOperator op) {
    ArrayList<Column> schema = new ArrayList<>(op.getSchema());
    result = new ScanOperator(schema, op.getTable().getName());
  }

  /**
   * Visits a LogicalSelectOperator and creates a corresponding SelectOperator.
   *
   * @param op The LogicalSelectOperator to visit
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
   * @param op The LogicalProjectOperator to visit
   */
  @Override
  public void visit(LogicalProjectOperator op) {
    op.getChildren().get(0).accept(this);
    Operator child = result;
    result = new ProjectOperator(child, op.getSelectItems());
  }

  /**
   * Visits a LogicalJoinOperator and creates a corresponding physical join operator based on the
   * configuration (TNLJ, BNLJ, or SMJ).
   *
   * @param op The LogicalJoinOperator to visit
   */
  @Override
  public void visit(LogicalJoinOperator op) {
    op.getChildren().get(0).accept(this);
    Operator leftChild = result;
    op.getChildren().get(1).accept(this);
    Operator rightChild = result;

    int number_of_pages_for_BNLJ = 1;
    switch (joinMethod) {
      case TNLJ:
        result = new JoinOperator(leftChild, rightChild, op.getCondition(), tableAliases);
        break;
      case BNLJ:
        result =
            new BNLJ(
                leftChild, rightChild, op.getCondition(), tableAliases, number_of_pages_for_BNLJ);
        break;
      case SMJ:
        result = new SMJ(leftChild, rightChild, op.getCondition(), tableAliases);
        break;
      default:
        throw new IllegalStateException("Unknown join method: " + joinMethod);
    }
  }

  /**
   * Visits a LogicalSortOperator and creates a corresponding physical sort operator based on the
   * configuration (in-memory or external sort).
   *
   * @param op The LogicalSortOperator to visit
   */
  @Override
  public void visit(LogicalSortOperator op) {
    op.getChildren().get(0).accept(this);
    Operator child = result;
    ArrayList<Column> schema = new ArrayList<>(op.getSchema());

    switch (sortMethod) {
      case IN_MEMORY_SORT:
        result = new SortOperator(schema, child, op.getOrderByElements());
        break;
      case EXTERNAL_SORT:
        result = new ExternalSort(schema, child, op.getOrderByElements(), sortBufferPages, tempDir);
        break;
      default:
        throw new IllegalStateException("Unknown sort method: " + sortMethod);
    }
  }

  /**
   * Visits a LogicalDuplicateEliminationOperator and creates a corresponding
   * DuplicateElementEliminationOperator.
   *
   * @param op The LogicalDuplicateEliminationOperator to visit
   */
  @Override
  public void visit(LogicalDuplicateEliminationOperator op) {
    op.getChildren().get(0).accept(this);
    Operator child = result;
    ArrayList<Column> schema = new ArrayList<>(op.getSchema());
    result = new DuplicateElementEliminationOperator(schema, child);
  }
}
