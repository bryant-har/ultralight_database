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

public class QueryPlanBuilder {
  private Map<String, String> tableAliases;
  private DBCatalog dbCatalog;

  public QueryPlanBuilder() {
    tableAliases = new HashMap<>();
    dbCatalog = DBCatalog.getInstance();
  }

  public Operator buildPlan(Statement stmt) {
    if (!(stmt instanceof Select)) {
      throw new IllegalArgumentException("Only SELECT statements are supported.");
    }
    Select select = (Select) stmt;
    PlainSelect plainSelect = (PlainSelect) select.getSelectBody();

    // Extract the WHERE clause expression
    Expression whereExpression = plainSelect.getWhere();

    // Process FROM clause and build base operators
    FromItem fromItem = plainSelect.getFromItem();
    List<Join> joins = plainSelect.getJoins();

    Operator currentOperator = processFromItem(fromItem);

    // Build join tree without specific join conditions
    if (joins != null) {
      for (Join join : joins) {
        Operator rightOperator = processFromItem(join.getRightItem());

        // Create a JoinOperator without specific join conditions
        currentOperator = new JoinOperator(currentOperator, rightOperator, null, tableAliases);
      }
    }

    Operator root = currentOperator;

    // Apply the WHERE clause as a selection condition on top of the joins
    if (whereExpression != null) {
      root = new SelectOperator(root, whereExpression, tableAliases);
    }

    // Apply projection
    List<SelectItem> selectItems = plainSelect.getSelectItems();
    root = new ProjectOperator(root, selectItems);

    // Handle ORDER BY clause
    List<OrderByElement> orderByElements = plainSelect.getOrderByElements();
    boolean hasOrderBy = orderByElements != null && !orderByElements.isEmpty();

    // Check for DISTINCT clause
    boolean isDistinct = plainSelect.getDistinct() != null;

    // If DISTINCT is present and there's no ORDER BY, add a SortOperator
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

    // If DISTINCT is present, add the DuplicateElementEliminationOperator
    if (isDistinct) {
      root = new DuplicateElementEliminationOperator(root.getOutputSchema(), root);
    }

    return root;
  }

  private Operator processFromItem(FromItem fromItem) {
    if (fromItem instanceof Table) {
      Table table = (Table) fromItem;
      String tableName = table.getName();
      String tableAlias = table.getAlias() != null ? table.getAlias().getName() : tableName;

      // Store the alias mapping
      tableAliases.put(tableAlias, tableName);

      // Get columns from the catalog
      ArrayList<Column> tableColumns = dbCatalog.getColumns(tableName);

      // Assign aliases to columns
      for (Column col : tableColumns) {
        Table colTable = new Table();
        colTable.setName(tableName);
        colTable.setAlias(new Alias(tableAlias)); // Assign alias to each column's table
        col.setTable(colTable);
      }

      // Create the ScanOperator with the updated schema
      return new ScanOperator(tableColumns, tableName);
    } else {
      throw new UnsupportedOperationException("Only table FROM items are supported.");
    }
  }
}
