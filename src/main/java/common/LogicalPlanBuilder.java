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

/**
 * The LogicalPlanBuilder class is responsible for constructing a logical query
 * plan
 * from a SQL select statement. It translates the SQL syntax into a tree of
 * logical
 * operators that represent the query's operations.
 */
public class LogicalPlanBuilder {
  /** Stores mappings of table aliases to their actual table names. */
  private Map<String, String> tableAliases;

  /**
   * Constructs a new LogicalPlanBuilder.
   */
  public LogicalPlanBuilder() {
    this.tableAliases = new HashMap<>();
  }

  /**
   * Builds a logical plan from a SQL Select statement.
   *
   * @param select The SQL Select statement to build the plan from.
   * @return The root LogicalOperator of the constructed logical plan.
   * @throws UnsupportedOperationException if the select body is not a
   *                                       PlainSelect.
   */
  public LogicalOperator buildPlan(Select select) {
    if (!(select.getSelectBody() instanceof PlainSelect)) {
      throw new UnsupportedOperationException("Only PlainSelect is supported");
    }

    PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
    return buildPlanFromPlainSelect(plainSelect);
  }

  /**
   * Builds a logical plan from a PlainSelect object.
   *
   * @param plainSelect The PlainSelect object to build the plan from.
   * @return The root LogicalOperator of the constructed logical plan.
   */
  private LogicalOperator buildPlanFromPlainSelect(PlainSelect plainSelect) {
    LogicalOperator operator = buildFromItem(plainSelect.getFromItem());

    // Handle JOINs
    if (plainSelect.getJoins() != null) {
      for (Join join : plainSelect.getJoins()) {
        LogicalOperator rightOperator = buildFromItem(join.getRightItem());
        Expression onExpression = join.getOnExpression();
        operator = new LogicalJoinOperator(
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
    operator = new LogicalProjectOperator(
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

  /**
   * Builds a logical operator from a FromItem (which can be a table or a
   * subquery).
   *
   * @param fromItem The FromItem to build the operator from.
   * @return A LogicalOperator representing the FromItem.
   * @throws UnsupportedOperationException if the FromItem is not a Table or
   *                                       SubSelect.
   */
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

  /**
   * Retrieves the columns for a given table from the DBCatalog.
   *
   * @param tableName The name of the table.
   * @return A list of Columns for the specified table.
   */
  private List<Column> getColumnsForTable(String tableName) {
    ArrayList<Column> columns = DBCatalog.getInstance().getColumns(tableName);
    return columns != null ? columns : new ArrayList<>();
  }

  /**
   * Combines two schemas (lists of columns) into a single schema.
   *
   * @param schema1 The first schema.
   * @param schema2 The second schema.
   * @return A combined list of columns from both schemas.
   */
  private List<Column> combineSchemas(List<Column> schema1, List<Column> schema2) {
    List<Column> combinedSchema = new ArrayList<>(schema1);
    combinedSchema.addAll(schema2);
    return combinedSchema;
  }

  /**
   * Projects the input schema based on the select items.
   *
   * @param inputSchema The input schema to project from.
   * @param selectItems The list of select items specifying the projection.
   * @return A new schema after applying the projection.
   * @throws IllegalArgumentException if a specified column is not found in the
   *                                  input schema.
   */
  private List<Column> projectSchema(List<Column> inputSchema, List<SelectItem> selectItems) {
    List<Column> outputSchema = new ArrayList<>();

    for (SelectItem item : selectItems) {
      if (item instanceof AllColumns) {
        outputSchema.addAll(inputSchema);
      } else if (item instanceof SelectExpressionItem) {
        SelectExpressionItem sei = (SelectExpressionItem) item;
        if (sei.getExpression() instanceof Column) {
          Column col = (Column) sei.getExpression();
          String columnName = col.getColumnName();

          Column matchingColumn = inputSchema.stream()
              .filter(c -> c.getColumnName().equals(columnName))
              .findFirst()
              .orElse(null);

          if (matchingColumn != null) {
            if (sei.getAlias() != null) {
              outputSchema.add(new Column(matchingColumn.getTable(), sei.getAlias().getName()));
            } else {
              outputSchema.add(matchingColumn);
            }
          } else {
            throw new IllegalArgumentException("Column not found in input schema: " + columnName);
          }
        } else {
          String columnName = sei.getAlias() != null ? sei.getAlias().getName() : "expr_" + outputSchema.size();
          outputSchema.add(new Column(null, columnName));
        }
      }
    }

    return outputSchema;
  }

  /**
   * Retrieves the map of table aliases to their actual table names.
   *
   * @return A map of table aliases to table names.
   */
  public Map<String, String> getTableAliases() {
    return tableAliases;
  }
}