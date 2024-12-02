package common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import join_algorithms.BNLJ;
import join_algorithms.SMJ;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import operator.logical.LogicalDuplicateEliminationOperator;
import operator.logical.LogicalJoinOperator;
import operator.logical.LogicalProjectOperator;
import operator.logical.LogicalScanOperator;
import operator.logical.LogicalSelectOperator;
import operator.logical.LogicalSortOperator;
import operator.physical.DuplicateElementEliminationOperator;
import operator.physical.ExternalSort;
import operator.physical.IndexScanOperator;
import operator.physical.JoinOperator;
import operator.physical.Operator;
import operator.physical.ProjectOperator;
import operator.physical.ScanOperator;
import operator.physical.SelectOperator;
import operator.physical.SortOperator;

/**
 * The PhysicalPlanBuilder class transforms a logical query plan into a physical query plan. It
 * reads configuration settings to determine which physical operators to use.
 */
public class PhysicalPlanBuilder implements LogicalOperatorVisitor {
  private Operator result;
  private final Map<String, String> tableAliases;
  private final int joinType; // 0: TNLJ, 1: BNLJ, 2: SMJ
  private final int joinBufferPages; // Buffer pages for BNLJ (if applicable)
  private final int sortType; // 0: in-memory, 1: external
  private final int sortBufferPages; // Buffer pages for external sort (if applicable)
  private final String tempDir; // Directory for external sort temp files
  private DBCatalog dbCatalog;

  /** Constructs a PhysicalPlanBuilder by reading configuration from a file. */
  public PhysicalPlanBuilder(Map<String, String> tableAliases, String configPath, String tempDir)
      throws IOException {

    this.tableAliases = tableAliases;
    this.tempDir = tempDir;

    // Read config file
    try (BufferedReader reader = new BufferedReader(new FileReader(configPath))) {
      // Read join configuration
      String[] joinConfig = reader.readLine().trim().split("\\s+");
      this.joinType = Integer.parseInt(joinConfig[0]);
      this.joinBufferPages =
          (joinType == 1 && joinConfig.length > 1) ? Integer.parseInt(joinConfig[1]) : 0;

      // Read sort configuration
      String[] sortConfig = reader.readLine().trim().split("\\s+");
      this.sortType = Integer.parseInt(sortConfig[0]);
      this.sortBufferPages =
          (sortType == 1 && sortConfig.length > 1) ? Integer.parseInt(sortConfig[1]) : 0;

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

  /**
   * Constructs a new PhysicalPlanBuilder.
   *
   * @param tableAliases A map of table aliases to their actual table names.
   * @param tempDir Directory containing index files
   */
  public PhysicalPlanBuilder(Map<String, String> tableAliases, String tempDir) {
    this.joinType = 0;
    this.joinBufferPages = 1;
    this.sortType = 0;
    this.sortBufferPages = 1;
    this.tableAliases = tableAliases;
    this.dbCatalog = DBCatalog.getInstance();
    this.tempDir = tempDir;
  }

  /**
   * Gets the index of a column in a table's schema.
   *
   * @param tableName The name of the table
   * @param columnName The name of the column
   * @return The index of the column in the table's schema, or -1 if not found
   */
  private int getColumnIndex(String tableName, String columnName) {
    ArrayList<Column> columns = dbCatalog.getColumns(tableName);
    for (int i = 0; i < columns.size(); i++) {
      if (columns.get(i).getColumnName().equals(columnName)) {
        return i;
      }
    }
    return -1;
  }

  /** Checks if an index exists for the given table and column */
  private boolean hasIndex(String tableName, String columnName) {
    String indexPath = String.format("%s/%s.%s", tempDir, tableName, columnName);
    return new File(indexPath).exists();
  }

  /** Gets the actual table name from a possible alias */
  private String resolveTableName(String tableNameOrAlias) {
    return tableAliases.getOrDefault(tableNameOrAlias, tableNameOrAlias);
  }

  /** Retrieves the result of the most recent visit operation. */
  public Operator getResult() {
    return result;
  }

  /**
   * Visits a LogicalScanOperator and creates a corresponding physical operator. May create either a
   * regular ScanOperator or an IndexScanOperator depending on available indexes and query
   * conditions.
   */
  @Override
  public void visit(LogicalScanOperator op) {
    ArrayList<Column> schema = new ArrayList<>(op.getSchema());
    String tableName = op.getTable().getName();

    // For now, create a regular scan operator
    // Index selection will be handled in the SelectOperator visit
    result = new ScanOperator(schema, tableName);
  }

  /**
   * Visits a LogicalSelectOperator and creates a corresponding physical operator. May use an
   * IndexScanOperator if appropriate indexes exist.
   */
  @Override
  public void visit(LogicalSelectOperator op) {
    // First check if we can use an index scan
    if (op.getChildren().get(0) instanceof LogicalScanOperator) {
      LogicalScanOperator scanOp = (LogicalScanOperator) op.getChildren().get(0);
      String tableName = resolveTableName(scanOp.getTable().getName());

      // Analyze the selection condition for potential index usage
      SelectionAnalyzer analyzer = findBestIndex(tableName, op.getCondition());

      if (analyzer != null && analyzer.hasIndexConditions()) {
        // We found an index we can use
        String indexColumn = analyzer.getIndexedColumn();
        boolean isClustered = isIndexClustered(tableName, indexColumn);

        // Create an IndexScanOperator
        result =
            new IndexScanOperator(
                new ArrayList<>(scanOp.getSchema()),
                tableName,
                tempDir + "/" + tableName + "." + indexColumn,
                isClustered,
                analyzer.getLowKey(),
                analyzer.getHighKey());

        // If there are remaining conditions, add a SelectOperator on top
        List<Expression> remainingConditions = analyzer.getRemainingConditions();
        if (!remainingConditions.isEmpty()) {
          Expression remainingExpr = buildAndExpression(remainingConditions);
          result = new SelectOperator(result, remainingExpr, tableAliases);
        }
        return;
      }
    }

    // If we can't use an index, fall back to regular selection
    op.getChildren().get(0).accept(this);
    Operator child = result;
    result = new SelectOperator(child, op.getCondition(), tableAliases);
  }

  /** Analyzes selection conditions to find the best index to use */
  private SelectionAnalyzer findBestIndex(String tableName, Expression condition) {
    SelectionAnalyzer bestAnalyzer = null;
    int bestScore = -1;

    // Get all columns from the schema
    ArrayList<Column> columns = dbCatalog.getColumns(tableName);

    // Check each column that has an index
    for (Column column : columns) {
      String columnName = column.getColumnName();
      if (hasIndex(tableName, columnName)) {
        SelectionAnalyzer analyzer = new SelectionAnalyzer(tableName, columnName);
        condition.accept(analyzer);

        if (analyzer.hasIndexConditions()) {
          int score = scoreIndexUsage(analyzer, tableName, columnName);
          if (score > bestScore) {
            bestScore = score;
            bestAnalyzer = analyzer;
          }
        }
      }
    }

    return bestAnalyzer;
  }

  /** Scores how beneficial it would be to use a particular index */
  private int scoreIndexUsage(SelectionAnalyzer analyzer, String tableName, String columnName) {
    int score = 0;

    // Prefer equality conditions
    if (analyzer.getLowKey() != null && analyzer.getLowKey().equals(analyzer.getHighKey())) {
      score += 3;
    }

    // Prefer range conditions over unbounded scans
    if (analyzer.getLowKey() != null) score += 1;
    if (analyzer.getHighKey() != null) score += 1;

    // Prefer clustered indexes
    if (isIndexClustered(tableName, columnName)) {
      score += 2;
    }

    return score;
  }

  /** Checks if an index is clustered */
  private boolean isIndexClustered(String tableName, String columnName) {
    // This information should come from index_info.txt
    // For now, returning false as default
    return false;
  }

  /** Combines multiple conditions with AND */
  private Expression buildAndExpression(List<Expression> conditions) {
    if (conditions.isEmpty()) return null;
    if (conditions.size() == 1) return conditions.get(0);

    Expression result = conditions.get(0);
    for (int i = 1; i < conditions.size(); i++) {
      result = new AndExpression(result, conditions.get(i));
    }
    return result;
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
        result =
            new SMJ(
                sortedLeft,
                sortedRight,
                op.getCondition(),
                tableAliases,
                leftColumns,
                rightColumns);
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

    result =
        new DuplicateElementEliminationOperator(new ArrayList<>(child.getOutputSchema()), child);
  }

  private void extractJoinColumns(
      Expression joinCondition,
      List<Column> leftColumns,
      List<Column> rightColumns,
      Operator leftChild,
      Operator rightChild) {

    // Handle AND conditions
    if (joinCondition instanceof net.sf.jsqlparser.expression.operators.conditional.AndExpression) {
      net.sf.jsqlparser.expression.operators.conditional.AndExpression and =
          (net.sf.jsqlparser.expression.operators.conditional.AndExpression) joinCondition;
      extractJoinColumns(and.getLeftExpression(), leftColumns, rightColumns, leftChild, rightChild);
      extractJoinColumns(
          and.getRightExpression(), leftColumns, rightColumns, leftChild, rightChild);
      return;
    }

    // Handle equals conditions
    if (joinCondition instanceof net.sf.jsqlparser.expression.operators.relational.EqualsTo) {
      net.sf.jsqlparser.expression.operators.relational.EqualsTo equals =
          (net.sf.jsqlparser.expression.operators.relational.EqualsTo) joinCondition;

      if (equals.getLeftExpression() instanceof Column
          && equals.getRightExpression() instanceof Column) {

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
      if (schemaCol.getTable().getName().equals(tableName)
          && schemaCol.getColumnName().equals(colName)) {
        return true;
      }
    }
    return false;
  }

  private Operator createSortOperator(Operator child, List<OrderByElement> orderByElements) {
    if (sortType == 0) {
      return new SortOperator(new ArrayList<>(child.getOutputSchema()), child, orderByElements);
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
