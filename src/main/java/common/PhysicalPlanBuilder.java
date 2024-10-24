package common;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectItem;
import operator.logical.*;
import operator.physical.*;
import join_algorithms.BNLJ;
import join_algorithms.SMJ;

/**
 * The PhysicalPlanBuilder class transforms a logical query plan into a physical
 * query plan.
 * It reads configuration settings to determine which physical operators to use.
 */
public class PhysicalPlanBuilder implements LogicalOperatorVisitor {
  private Operator result;
  private final Map<String, String> tableAliases;
  private final int joinType; // 0: TNLJ, 1: BNLJ, 2: SMJ
  private final int joinBufferPages; // Buffer pages for BNLJ (if applicable)
  private final int sortType; // 0: in-memory, 1: external
  private final int sortBufferPages; // Buffer pages for external sort (if applicable)
  private final String tempDir; // Directory for external sort temp files

  /**
   * Constructs a PhysicalPlanBuilder by reading configuration from a file.
   */
  public PhysicalPlanBuilder(
      Map<String, String> tableAliases,
      String configPath,
      String tempDir) throws IOException {

    this.tableAliases = tableAliases;
    this.tempDir = tempDir;

    // Read config file
    try (BufferedReader reader = new BufferedReader(new FileReader(configPath))) {
      // Read join configuration
      String[] joinConfig = reader.readLine().trim().split("\\s+");
      this.joinType = Integer.parseInt(joinConfig[0]);
      this.joinBufferPages = (joinType == 1 && joinConfig.length > 1) ? Integer.parseInt(joinConfig[1]) : 0;

      // Read sort configuration
      String[] sortConfig = reader.readLine().trim().split("\\s+");
      this.sortType = Integer.parseInt(sortConfig[0]);
      this.sortBufferPages = (sortType == 1 && sortConfig.length > 1) ? Integer.parseInt(sortConfig[1]) : 0;

      // Validate configuration
      if (joinType < 0 || joinType > 2) {
        throw new IllegalArgumentException("Invalid join type: " + joinType);
      }
      if (sortType < 0 || sortType > 1) {
        throw new IllegalArgumentException("Invalid sort type: " + sortType);
      }
      if (joinType == 1 && joinBufferPages < 1) {
        throw new IllegalArgumentException("BNLJ requires buffer pages > 0");
      }
      if (sortType == 1 && sortBufferPages < 3) {
        throw new IllegalArgumentException("External sort requires at least 3 buffer pages");
      }
    }
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
    // Visit children first
    op.getChildren().get(0).accept(this);
    Operator leftChild = result;
    op.getChildren().get(1).accept(this);
    Operator rightChild = result;

    switch (joinType) {
      case 0: // TNLJ
        result = new JoinOperator(leftChild, rightChild, op.getCondition(), tableAliases);
        break;

      case 1: // BNLJ
        result = new BNLJ(leftChild, rightChild, op.getCondition(), tableAliases, joinBufferPages);
        break;

      case 2: // SMJ
        // Extract columns for sorting from the join condition
        List<Column> leftColumns = new ArrayList<>();
        List<Column> rightColumns = new ArrayList<>();
        extractJoinColumns(op.getCondition(), leftColumns, rightColumns, leftChild, rightChild);

        // Create sort orders
        List<OrderByElement> leftOrderBy = createOrderByElements(leftColumns);
        List<OrderByElement> rightOrderBy = createOrderByElements(rightColumns);

        // Add sort operators
        Operator sortedLeft = createSortOperator(leftChild, leftOrderBy);
        Operator sortedRight = createSortOperator(rightChild, rightOrderBy);

        // Create SMJ with sort columns
        result = new SMJ(sortedLeft, sortedRight, op.getCondition(),
            tableAliases, leftColumns, rightColumns);
        break;
    }
  }

  @Override
  public void visit(LogicalSortOperator op) {
    op.getChildren().get(0).accept(this);
    Operator child = result;
    result = createSortOperator(child, op.getOrderByElements());
  }

  @Override
  public void visit(LogicalDuplicateEliminationOperator op) {
    op.getChildren().get(0).accept(this);
    Operator child = result;

    // For DISTINCT, we need to sort first if not already sorted
    if (!(child instanceof SortOperator)) {
      List<OrderByElement> orderByElements = new ArrayList<>();
      for (Column col : child.getOutputSchema()) {
        OrderByElement orderBy = new OrderByElement();
        orderBy.setExpression(col);
        orderBy.setAsc(true);
        orderByElements.add(orderBy);
      }
      child = createSortOperator(child, orderByElements);
    }

    result = new DuplicateElementEliminationOperator(
        new ArrayList<>(child.getOutputSchema()),
        child);
  }

  private void extractJoinColumns(Expression joinCondition,
      List<Column> leftColumns,
      List<Column> rightColumns,
      Operator leftChild,
      Operator rightChild) {

    // Handle AND conditions
    if (joinCondition instanceof net.sf.jsqlparser.expression.operators.conditional.AndExpression) {
      net.sf.jsqlparser.expression.operators.conditional.AndExpression and = (net.sf.jsqlparser.expression.operators.conditional.AndExpression) joinCondition;
      extractJoinColumns(and.getLeftExpression(), leftColumns, rightColumns, leftChild, rightChild);
      extractJoinColumns(and.getRightExpression(), leftColumns, rightColumns, leftChild, rightChild);
      return;
    }

    // Handle equals conditions
    if (joinCondition instanceof net.sf.jsqlparser.expression.operators.relational.EqualsTo) {
      net.sf.jsqlparser.expression.operators.relational.EqualsTo equals = (net.sf.jsqlparser.expression.operators.relational.EqualsTo) joinCondition;

      if (equals.getLeftExpression() instanceof Column &&
          equals.getRightExpression() instanceof Column) {

        Column leftCol = (Column) equals.getLeftExpression();
        Column rightCol = (Column) equals.getRightExpression();

        // Get table names from schema to determine which columns belong to which
        // relation
        String leftChildTable = leftChild.getOutputSchema().get(0).getTable().getName();
        String leftColTable = leftCol.getTable().getName();
        String rightColTable = rightCol.getTable().getName();

        // First check if leftCol belongs to left relation
        if (belongsToSchema(leftCol, leftChild.getOutputSchema())) {
          if (belongsToSchema(rightCol, rightChild.getOutputSchema())) {
            leftColumns.add(leftCol);
            rightColumns.add(rightCol);
          }
        }
        // Check if rightCol belongs to left relation
        else if (belongsToSchema(rightCol, leftChild.getOutputSchema())) {
          if (belongsToSchema(leftCol, rightChild.getOutputSchema())) {
            leftColumns.add(rightCol);
            rightColumns.add(leftCol);
          }
        }
      }
    }
  }

  private boolean belongsToSchema(Column col, List<Column> schema) {
    String tableName = col.getTable().getName();
    String colName = col.getColumnName();

    for (Column schemaCol : schema) {
      if (schemaCol.getTable().getName().equals(tableName) &&
          schemaCol.getColumnName().equals(colName)) {
        return true;
      }
    }
    return false;
  }

  private Operator createSortOperator(Operator child, List<OrderByElement> orderByElements) {
    if (sortType == 0) {
      return new SortOperator(
          new ArrayList<>(child.getOutputSchema()),
          child,
          orderByElements);
    } else {
      return new ExternalSort(
          new ArrayList<>(child.getOutputSchema()),
          child,
          orderByElements,
          sortBufferPages,
          tempDir);
    }
  }

  private List<OrderByElement> createOrderByElements(List<Column> columns) {
    List<OrderByElement> orderByElements = new ArrayList<>();
    for (Column col : columns) {
      OrderByElement orderBy = new OrderByElement();
      orderBy.setExpression(col);
      orderBy.setAsc(true);
      orderByElements.add(orderBy);
    }
    return orderByElements;
  }
}