package common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import operator.logical.*;

public class LogicalPlanBuilder {
  private Map<String, String> tableAliases;

  public LogicalPlanBuilder() {
    this.tableAliases = new HashMap<>();
  }

  public LogicalOperator buildPlan(Select select) {
    if (!(select.getSelectBody() instanceof PlainSelect)) {
      throw new UnsupportedOperationException("Only PlainSelect is supported");
    }

    PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
    return buildPlanFromPlainSelect(plainSelect);
  }

  private LogicalOperator buildPlanFromPlainSelect(PlainSelect plainSelect) {
    // Start with the FROM clause
    LogicalOperator operator = buildFromItem(plainSelect.getFromItem());

    // Handle JOINs
    if (plainSelect.getJoins() != null) {
      for (Join join : plainSelect.getJoins()) {
        LogicalOperator rightOperator = buildFromItem(join.getRightItem());
        Expression onExpression = join.getOnExpression();
        operator =
            new LogicalJoinOperator(
                operator,
                rightOperator,
                onExpression,
                combineSchemas(operator.getSchema(), rightOperator.getSchema()));
      }
    }

    // Handle WHERE clause
    if (plainSelect.getWhere() != null) {
      operator = new LogicalSelectOperator(operator, plainSelect.getWhere());
    }

    // Handle SELECT clause (projection)
    operator =
        new LogicalProjectOperator(
            operator,
            plainSelect.getSelectItems(),
            projectSchema(operator.getSchema(), plainSelect.getSelectItems()));

    // Handle DISTINCT
    if (plainSelect.getDistinct() != null) {
      operator = new LogicalDuplicateEliminationOperator(operator);
    }

    // Handle ORDER BY
    if (plainSelect.getOrderByElements() != null) {
      operator = new LogicalSortOperator(operator, plainSelect.getOrderByElements());
    }

    return operator;
  }

  private LogicalOperator buildFromItem(FromItem fromItem) {
    if (fromItem instanceof Table) {
      Table table = (Table) fromItem;
      String tableName = table.getName();
      String tableAlias = table.getAlias() != null ? table.getAlias().getName() : tableName;
      tableAliases.put(tableAlias, tableName);
      List<Column> schema = getColumnsForTable(tableName);
      return new LogicalScanOperator(table, schema);
    } else if (fromItem instanceof SubSelect) {
      return buildPlan((Select) ((SubSelect) fromItem).getSelectBody());
    } else {
      throw new UnsupportedOperationException(
          "Unsupported FromItem: " + fromItem.getClass().getName());
    }
  }

  private List<Column> getColumnsForTable(String tableName) {
    // Use the correct method from DBCatalog to get columns for a table
    ArrayList<Column> columns = DBCatalog.getInstance().getColumns(tableName);
    return columns != null ? columns : new ArrayList<>();
  }

  private List<Column> combineSchemas(List<Column> schema1, List<Column> schema2) {
    List<Column> combinedSchema = new ArrayList<>(schema1);
    combinedSchema.addAll(schema2);
    return combinedSchema;
  }

  private List<Column> projectSchema(List<Column> inputSchema, List<SelectItem> selectItems) {
    List<Column> outputSchema = new ArrayList<>();

    for (SelectItem item : selectItems) {
      if (item instanceof AllColumns) {
        // SELECT * case: add all columns from input schema
        outputSchema.addAll(inputSchema);
      } else if (item instanceof SelectExpressionItem) {
        SelectExpressionItem sei = (SelectExpressionItem) item;
        if (sei.getExpression() instanceof Column) {
          // Simple column selection
          Column col = (Column) sei.getExpression();
          String columnName = col.getColumnName();

          // Find matching column in input schema
          Column matchingColumn =
              inputSchema.stream()
                  .filter(c -> c.getColumnName().equals(columnName))
                  .findFirst()
                  .orElse(null);

          if (matchingColumn != null) {
            // If there's an alias, create a new column with the alias
            if (sei.getAlias() != null) {
              outputSchema.add(new Column(matchingColumn.getTable(), sei.getAlias().getName()));
            } else {
              outputSchema.add(matchingColumn);
            }
          } else {
            throw new IllegalArgumentException("Column not found in input schema: " + columnName);
          }
        } else {
          // For expressions other than simple columns (e.g., functions, arithmetic)
          // We'll add a new column with the alias if provided, or a generated name
          String columnName =
              sei.getAlias() != null ? sei.getAlias().getName() : "expr_" + outputSchema.size();
          outputSchema.add(new Column(null, columnName));
        }
      }
    }

    return outputSchema;
  }

  public Map<String, String> getTableAliases() {
    return tableAliases;
  }
}
