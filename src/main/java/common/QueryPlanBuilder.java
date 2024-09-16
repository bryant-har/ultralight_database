package common;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import jdk.jshell.spi.ExecutionControl.NotImplementedException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import operator.*;

public class QueryPlanBuilder {
  private Map<String, String> tableAliases;
  private Map<String, String> columnAliases;

  public QueryPlanBuilder() {
    tableAliases = new HashMap<>();
    columnAliases = new HashMap<>();
  }

  public Operator buildPlan(Statement stmt) throws NotImplementedException {
    if (!(stmt instanceof Select)) {
      throw new IllegalArgumentException("Only SELECT statements are supported.");
    }

    Select select = (Select) stmt;
    PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
    FromItem fromItem = plainSelect.getFromItem();
    Operator root;
    DBCatalog dbCatalog = DBCatalog.getInstance();

    if (fromItem instanceof Table) {
      Table table = (Table) fromItem;
      String tableName = table.getName();
      String tableAlias = table.getAlias() != null ? table.getAlias().getName() : tableName;
      tableAliases.put(tableAlias, tableName);

      ArrayList<Column> tableColumns = dbCatalog.getColumns(tableName);
      root = new ScanOperator(tableColumns, tableName);

      // Handle column aliases
      List<SelectItem> selectItems = plainSelect.getSelectItems();
      for (SelectItem item : selectItems) {
        if (item instanceof SelectExpressionItem) {
          SelectExpressionItem sei = (SelectExpressionItem) item;
          if (sei.getAlias() != null) {
            String columnName = sei.getExpression().toString();
            String columnAlias = sei.getAlias().getName();
            columnAliases.put(columnAlias, columnName);
          }
        }
      }

      // Apply projection
      root = new ProjectOperator(root, selectItems);

      // Check for ORDER BY clause
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
    } else {
      throw new NotImplementedException("Only single table queries are supported.");
    }
  }

  // Helper method to resolve column references with aliases
  public String resolveColumnReference(String columnRef) {
    if (columnAliases.containsKey(columnRef)) {
      return columnAliases.get(columnRef);
    }
    return columnRef;
  }

  // Helper method to resolve table references with aliases
  public String resolveTableReference(String tableRef) {
    if (tableAliases.containsKey(tableRef)) {
      return tableAliases.get(tableRef);
    }
    return tableRef;
  }
}