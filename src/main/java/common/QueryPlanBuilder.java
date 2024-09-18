package common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import operator.*;

/**
 * The QueryPlanBuilder class is responsible for building the query execution plan based on the
 * parsed SQL statement. It converts an abstract SQL statement (represented by JSQLParser
 * structures) into a tree of relational operators that can be executed.
 */
public class QueryPlanBuilder {
  // A map to store table aliases (alias name -> table name)
  private Map<String, String> tableAliases;

  // Singleton instance of the database catalog, used to retrieve schema
  // information
  private DBCatalog dbCatalog;

  /**
   * Constructs a QueryPlanBuilder and initializes the table alias map and the catalog reference.
   */
  public QueryPlanBuilder() {
    tableAliases = new HashMap<>();
    dbCatalog = DBCatalog.getInstance(); // Get a singleton instance of the database catalog
  }

  /**
   * Builds a query execution plan based on the provided SQL statement. This method supports SELECT
   * statements with optional WHERE, JOIN, ORDER BY, and DISTINCT clauses.
   *
   * @param stmt The SQL statement to build the plan for.
   * @return The root operator of the constructed query execution plan.
   */
  public Operator buildPlan(Statement stmt) {
    // Ensure the statement is a SELECT statement
    if (!(stmt instanceof Select)) {
      throw new IllegalArgumentException("Only SELECT statements are supported.");
    }
    Select select = (Select) stmt;
    PlainSelect plainSelect = (PlainSelect) select.getSelectBody();

    // Extract the WHERE clause (if present)
    Expression whereExpression = plainSelect.getWhere();

    // Process the FROM clause and initialize the base operator
    FromItem fromItem = plainSelect.getFromItem();
    List<Join> joins = plainSelect.getJoins();

    // Create the base operator for the first table in the FROM clause
    Operator currentOperator = processFromItem(fromItem);

    // Process JOINs (if any), adding JoinOperators without specific join conditions
    if (joins != null) {
      for (Join join : joins) {
        // Process the table being joined
        Operator rightOperator = processFromItem(join.getRightItem());

        // Create a JoinOperator (currently without a condition)
        currentOperator = new JoinOperator(currentOperator, rightOperator, null, tableAliases);
      }
    }

    Operator root = currentOperator;

    // Apply the WHERE clause using a SelectOperator
    if (whereExpression != null) {
      root = new SelectOperator(root, whereExpression, tableAliases);
    }

    // Apply projection using a ProjectOperator
    List<SelectItem> selectItems = plainSelect.getSelectItems();
    root = new ProjectOperator(root, selectItems);

    // Handle the ORDER BY clause
    List<OrderByElement> orderByElements = plainSelect.getOrderByElements();
    boolean hasOrderBy = orderByElements != null && !orderByElements.isEmpty();

    // Check if the DISTINCT clause is present
    boolean isDistinct = plainSelect.getDistinct() != null;

    // If DISTINCT is present and no ORDER BY, add a SortOperator to remove
    // duplicates
    if (isDistinct && !hasOrderBy) {
      orderByElements = new ArrayList<>();
      for (Column col : root.getOutputSchema()) {
        OrderByElement orderByElement = new OrderByElement();
        orderByElement.setExpression(col);
        orderByElements.add(orderByElement);
      }
      root = new SortOperator(root.getOutputSchema(), root, orderByElements);
    } else if (hasOrderBy) {
      // If there's an ORDER BY clause, add the SortOperator
      root = new SortOperator(root.getOutputSchema(), root, orderByElements);
    }

    // If DISTINCT is present, apply the DuplicateElementEliminationOperator
    if (isDistinct) {
      root = new DuplicateElementEliminationOperator(root.getOutputSchema(), root);
    }

    return root; // Return the root operator of the query plan
  }

  /**
   * Processes a FROM item in the SQL query and creates the appropriate base operator. Only tables
   * are supported as FROM items.
   *
   * @param fromItem The FROM clause item to process.
   * @return The base operator (e.g., ScanOperator) for the table.
   */
  private Operator processFromItem(FromItem fromItem) {
    if (fromItem instanceof Table) {
      // Handle table FROM items
      Table table = (Table) fromItem;
      String tableName = table.getName();
      String tableAlias = table.getAlias() != null ? table.getAlias().getName() : tableName;

      // Store the alias mapping (alias -> table name)
      tableAliases.put(tableAlias, tableName);

      // Retrieve the columns for the table from the catalog
      ArrayList<Column> tableColumns = dbCatalog.getColumns(tableName);

      // Create aliased columns for the table schema
      ArrayList<Column> aliasedColumns = new ArrayList<>();
      for (Column col : tableColumns) {
        // Create a new Table object and set the alias
        Table colTable = new Table();
        colTable.setName(tableName);
        colTable.setAlias(new Alias(tableAlias)); // Assign alias to each column's table

        // Create a new Column object with the aliased Table
        Column aliasedColumn = new Column(colTable, col.getColumnName());
        aliasedColumns.add(aliasedColumn);
      }

      // Create and return a ScanOperator with the aliased schema
      return new ScanOperator(aliasedColumns, tableName);
    } else {
      throw new UnsupportedOperationException("Only table FROM items are supported.");
    }
  }
}
