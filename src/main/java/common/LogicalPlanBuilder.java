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
 * plan from a SQL
 * select statement. It translates the SQL syntax into a tree of logical
 * operators that represent
 * the query's operations.
 */
public class LogicalPlanBuilder {
  /** Stores mappings of table aliases to their actual table names. */
  private Map<String, String> tableAliases;

  /** Constructs a new LogicalPlanBuilder. */
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
    // Start with the FROM clause
    LogicalOperator operator = buildFromItem(plainSelect.getFromItem());

    // Handle JOINs
    List<Join> joins = plainSelect.getJoins();
    if (joins != null) {
      for (Join join : joins) {
        LogicalOperator rightOperator = buildFromItem(join.getRightItem());

        // For implicit joins (comma-separated in FROM clause),
        // create a cross product initially
        operator = new LogicalJoinOperator(
            operator, rightOperator, null // No condition for implicit join at this stage
        );
      }
    }

    // Handle WHERE clause
    Expression whereExpression = plainSelect.getWhere();
    if (whereExpression != null) {
      // If we have a cross product from implicit join,
      // the WHERE condition becomes the join condition
      if (operator instanceof LogicalJoinOperator
          && ((LogicalJoinOperator) operator).getCondition() == null) {
        List<LogicalOperator> children = ((LogicalJoinOperator) operator).getChildren();
        operator = new LogicalJoinOperator(children.get(0), children.get(1), whereExpression);
      } else {
        operator = new LogicalSelectOperator(operator, whereExpression);
      }
    }

    // Handle SELECT clause (projection)
    operator = new LogicalProjectOperator(
        operator,
        plainSelect.getSelectItems(),
        projectSchema(operator.getSchema(), plainSelect.getSelectItems()));

    // Handle ORDER BY
    if (plainSelect.getOrderByElements() != null) {
      operator = new LogicalSortOperator(operator, plainSelect.getOrderByElements());
    }

    // Handle DISTINCT
    if (plainSelect.getDistinct() != null) {
      // If there's no explicit ORDER BY, add a sort operator before DISTINCT
      if (plainSelect.getOrderByElements() == null) {
        // Create a list of OrderByElements based on all columns in the current schema
        List<OrderByElement> orderByElements = new ArrayList<>();
        for (Column column : operator.getSchema()) {
          OrderByElement orderByElement = new OrderByElement();
          orderByElement.setExpression(column);
          orderByElements.add(orderByElement);
        }
        operator = new LogicalSortOperator(operator, orderByElements);
      }
      operator = new LogicalDuplicateEliminationOperator(operator);
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
      List<Column> schema = getColumnsForTable(tableName, tableAlias);
      return new LogicalScanOperator(table, schema);
    } else if (fromItem instanceof SubSelect) {
      return buildPlan((Select) ((SubSelect) fromItem).getSelectBody());
    } else {
      throw new UnsupportedOperationException(
          "Unsupported FromItem: " + fromItem.getClass().getName());
    }
  }

  /**
   * Retrieves the columns for a given table from the DBCatalog and applies the
   * table alias.
   *
   * @param tableName  The name of the table.
   * @param tableAlias The alias of the table.
   * @return A list of Columns for the specified table with the alias applied.
   */
  private List<Column> getColumnsForTable(String tableName, String tableAlias) {
    ArrayList<Column> columns = DBCatalog.getInstance().getColumns(tableName);
    ArrayList<Column> aliasedColumns = new ArrayList<>();
    for (Column col : columns) {
      Table aliasedTable = new Table(tableAlias);
      aliasedColumns.add(new Column(aliasedTable, col.getColumnName()));
    }
    return aliasedColumns;
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