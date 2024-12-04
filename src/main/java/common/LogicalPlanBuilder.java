package common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import operator.logical.*;

/**
 * The LogicalPlanBuilder class is responsible for constructing a logical query plan from a SQL
 * select statement. It handles selection pushing using UnionFind and constructs multi-way joins.
 */
public class LogicalPlanBuilder {
  private Map<String, String> tableAliases; // Map of table aliases to actual table names
  private DBCatalog dbCatalog; // Database catalog for metadata access
  private WhereClauseVisitor currentWhereVisitor; // Visitor to process WHERE clause

  public LogicalPlanBuilder() {
    this.tableAliases = new HashMap<>();
    this.dbCatalog = DBCatalog.getInstance();
  }

  /**
   * Builds a logical query plan from a SQL SELECT statement.
   *
   * @param select The SQL SELECT statement.
   * @return The root of the logical query plan.
   */
  public LogicalOperator buildPlan(Select select) {
    if (!(select.getSelectBody() instanceof PlainSelect)) {
      throw new UnsupportedOperationException("Only PlainSelect is supported");
    }

    PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
    return buildPlanFromPlainSelect(plainSelect);
  }

  /**
   * Builds a logical plan from a PlainSelect statement.
   *
   * @param plainSelect The PlainSelect statement.
   * @return The root of the logical query plan.
   */
  private LogicalOperator buildPlanFromPlainSelect(PlainSelect plainSelect) {
    // Build table alias map for WHERE clause processing
    Map<String, Table> tableAliasMap = new HashMap<>();
    addTableAlias(plainSelect.getFromItem(), tableAliasMap);
    List<Join> joins = plainSelect.getJoins();
    if (joins != null) {
      for (Join join : joins) {
        addTableAlias(join.getRightItem(), tableAliasMap);
      }
    }

    // Process WHERE clause using UnionFind
    Expression whereExpression = plainSelect.getWhere();
    this.currentWhereVisitor = new WhereClauseVisitor(whereExpression, tableAliasMap);
    List<Expression> residualConditions = currentWhereVisitor.getJoinExpressions();
    UnionFind unionFind = currentWhereVisitor.getUnionFind();

    // Process the FROM clause and any JOINs
    List<LogicalOperator> scanOperators = new ArrayList<>();

    // Add the main FROM item
    FromItem fromItem = plainSelect.getFromItem();
    scanOperators.add(buildFromItem(fromItem));

    // Process JOIN items if any
    if (joins != null) {
      for (Join join : joins) {
        scanOperators.add(buildFromItem(join.getRightItem()));
      }
    }

    // Push down selections to each base table using UnionFind constraints
    List<LogicalOperator> operatorsWithSelections = new ArrayList<>();
    for (LogicalOperator scanOp : scanOperators) {
      Expression localCondition = buildLocalCondition(scanOp.getSchema(), unionFind);
      if (localCondition != null) {
        operatorsWithSelections.add(new LogicalSelectOperator(scanOp, localCondition));
      } else {
        operatorsWithSelections.add(scanOp);
      }
    }

    // Create multi-way join with remaining conditions
    LogicalOperator operator =
        new LogicalJoinOperator(operatorsWithSelections, residualConditions, unionFind);

    // Handle projection
    operator =
        new LogicalProjectOperator(
            operator,
            plainSelect.getSelectItems(),
            projectSchema(operator.getSchema(), plainSelect.getSelectItems()));

    // Add ORDER BY if present
    if (plainSelect.getOrderByElements() != null) {
      operator = new LogicalSortOperator(operator, plainSelect.getOrderByElements());
    }

    // Handle DISTINCT
    if (plainSelect.getDistinct() != null) {
      // If there's no explicit ORDER BY, add sort operator before DISTINCT
      if (plainSelect.getOrderByElements() == null) {
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
   * Retrieves the current WhereClauseVisitor.
   *
   * @return The WhereClauseVisitor.
   */
  private WhereClauseVisitor getCurrentWhereVisitor() {
    return currentWhereVisitor;
  }

  /**
   * Combines multiple conditions using AND.
   *
   * @param existing The existing condition.
   * @param newConditions List of new conditions.
   * @return The combined condition.
   */
  private Expression combineConditions(Expression existing, List<Expression> newConditions) {
    if (newConditions == null || newConditions.isEmpty()) {
      return existing;
    }

    Expression combinedCondition = null;
    for (Expression condition : newConditions) {
      if (combinedCondition == null) {
        combinedCondition = condition;
      } else {
        combinedCondition = new AndExpression(combinedCondition, condition);
      }
    }

    if (existing != null) {
      combinedCondition = new AndExpression(existing, combinedCondition);
    }

    return combinedCondition;
  }

  /**
   * Builds a local condition for a table's schema using UnionFind.
   *
   * @param schema The table's schema.
   * @param unionFind The UnionFind structure.
   * @return The local condition as an Expression.
   */
  private Expression buildLocalCondition(List<Column> schema, UnionFind unionFind) {
    List<Expression> localConditions = new ArrayList<>();

    // Get table name from first column in schema
    String tableName = null;
    if (!schema.isEmpty()) {
      tableName = schema.get(0).getTable().getName();
    }

    // Add selection expressions specific to this table
    if (tableName != null && getCurrentWhereVisitor() != null) {
      Expression tableSelections = getCurrentWhereVisitor().getSelectExpressions().get(tableName);
      if (tableSelections != null) {
        localConditions.add(tableSelections);
      }
    }

    // Add conditions from UnionFind
    for (Column col : schema) {
      String fullyQualifiedName = col.getFullyQualifiedName();
      UnionFind.UnionElement element = unionFind.find(fullyQualifiedName);

      if (element == null) continue;

      // Equality constraint
      Double equalityConstraint = element.getEqualityConstraint();
      if (equalityConstraint != null) {
        localConditions.add(
            new EqualsTo()
                .withLeftExpression(col)
                .withRightExpression(new DoubleValue(equalityConstraint.toString())));
      }

      // Lower bound constraint
      Double lowerBound = element.getLowerBound();
      if (lowerBound != null) {
        localConditions.add(
            new GreaterThanEquals()
                .withLeftExpression(col)
                .withRightExpression(new DoubleValue(lowerBound.toString())));
      }

      // Upper bound constraint
      Double upperBound = element.getUpperBound();
      if (upperBound != null) {
        localConditions.add(
            new MinorThanEquals()
                .withLeftExpression(col)
                .withRightExpression(new DoubleValue(upperBound.toString())));
      }
    }

    return combineConditions(null, localConditions);
  }

  /**
   * Adds a table alias to the alias map for FROM items.
   *
   * @param fromItem The FROM item.
   * @param aliasMap The alias map.
   */
  private void addTableAlias(FromItem fromItem, Map<String, Table> aliasMap) {
    if (fromItem instanceof Table) {
      Table table = (Table) fromItem;
      String alias = table.getAlias() != null ? table.getAlias().getName() : table.getName();
      aliasMap.put(alias, table);
    }
  }

  /**
   * Builds a logical operator for a FROM item.
   *
   * @param fromItem The FROM item.
   * @return The logical operator.
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
   * Retrieves the schema for a table with an alias.
   *
   * @param tableName The table name.
   * @param tableAlias The table alias.
   * @return The schema as a list of columns.
   */
  private List<Column> getColumnsForTable(String tableName, String tableAlias) {
    ArrayList<Column> columns = dbCatalog.getColumns(tableName);
    ArrayList<Column> aliasedColumns = new ArrayList<>();
    for (Column col : columns) {
      Table aliasedTable = new Table(tableAlias);
      aliasedColumns.add(new Column(aliasedTable, col.getColumnName()));
    }
    return aliasedColumns;
  }

  /**
   * Projects the schema for the given select items.
   *
   * @param inputSchema The input schema.
   * @param selectItems The select items.
   * @return The projected schema.
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

          Column matchingColumn =
              inputSchema.stream()
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
          String columnName =
              sei.getAlias() != null ? sei.getAlias().getName() : "expr_" + outputSchema.size();
          outputSchema.add(new Column(null, columnName));
        }
      }
    }

    return outputSchema;
  }

  /**
   * Retrieves the table aliases.
   *
   * @return The map of table aliases.
   */
  public Map<String, String> getTableAliases() {
    return tableAliases;
  }
}
