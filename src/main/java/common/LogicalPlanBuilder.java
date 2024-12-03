package common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import operator.logical.*;
import net.sf.jsqlparser.expression.DoubleValue;

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
   * Builds a logical plan from a PlainSelect object with Union-Find integration
   */
  private LogicalOperator buildPlanFromPlainSelect(PlainSelect plainSelect) {
    // Process WHERE clause first
    Expression whereExpression = plainSelect.getWhere();
    WhereClauseVisitor whereVisitor = new WhereClauseVisitor();
    List<Expression> residualConditions = whereExpression != null
        ? whereVisitor.process(whereExpression)
        : new ArrayList<>();

    LogicalOperator operator = buildFromItem(plainSelect.getFromItem());

    // Handle JOINs with Union-Find pushed selections donw
    List<Join> joins = plainSelect.getJoins();
    List<Expression> globalResidualJoinConditions = new ArrayList<>();

    if (joins != null) {
      for (Join join : joins) {
        LogicalOperator rightOperator = buildFromItem(join.getRightItem());
        Table rightTable = (Table) join.getRightItem();
        Table leftTable = operator.getSchema().get(0).getTable();

        List<String> usedTableNames = new ArrayList<>();
        usedTableNames.add(
            (rightTable.getAlias() != null)
                ? rightTable.getAlias().getName()
                : rightTable.getName());
        usedTableNames.add(
            (leftTable.getAlias() != null)
                ? leftTable.getAlias().getName()
                : leftTable.getName());

        List<Expression> leftTableConditions = new ArrayList<>();
        List<Expression> rightTableConditions = new ArrayList<>();
        List<Expression> joinConditions = new ArrayList<>();

        for (Expression residual : residualConditions) {
          // what is going on here?
          List<String> conditionTables = ExpressionEvaluator.getTablesInExpression(residual);
          if (conditionTables.size() == 1) {
            if (usedTableNames.contains(conditionTables.get(0))) {
              if (conditionTables.get(0).equals(usedTableNames.get(0))) {
                rightTableConditions.add(residual);
              } else {
                leftTableConditions.add(residual);
              }
            }
          } else if (conditionTables.size() > 1) {
            joinConditions.add(residual);
          }
        }

        // Add local conditions based on Union-Find
        UnionFind unionFind = whereVisitor.getUnionFind();
        Expression leftLocalCondition = buildLocalCondition(operator.getSchema(), unionFind);
        Expression rightLocalCondition = buildLocalCondition(rightOperator.getSchema(), unionFind);

        if (leftLocalCondition != null || !leftTableConditions.isEmpty()) {
          Expression combinedLeftCondition = combineConditions(leftLocalCondition, leftTableConditions);
          operator = new LogicalSelectOperator(operator, combinedLeftCondition);
        }

        if (rightLocalCondition != null || !rightTableConditions.isEmpty()) {
          Expression combinedRightCondition = combineConditions(rightLocalCondition, rightTableConditions);
          rightOperator = new LogicalSelectOperator(rightOperator, combinedRightCondition);
        }

        // Collect global residual join conditions
        globalResidualJoinConditions.addAll(joinConditions);

        // Combine with cross product or final join condition
        Expression joinExpression = combineConditions(null, joinConditions);
        operator = new LogicalJoinOperator(
            operator,
            rightOperator,
            joinExpression);
      }
    }

    // Add global residual join conditions to the top-level join operator if needed
    if (!globalResidualJoinConditions.isEmpty()) {
      ((LogicalJoinOperator) operator).addAdditionalJoinConditions(globalResidualJoinConditions);
    }

    // selects
    operator = new LogicalProjectOperator(
        operator,
        plainSelect.getSelectItems(),
        projectSchema(operator.getSchema(), plainSelect.getSelectItems()));

    // ORDER BY
    if (plainSelect.getOrderByElements() != null) {
      operator = new LogicalSortOperator(operator, plainSelect.getOrderByElements());
    }

    // DISTINCT
    if (plainSelect.getDistinct() != null) {
      // If there's no explicit ORDER BY, add a sort operator before DISTINCT
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
   * Builds a condition for a schema based on Union-Find constraints
   */
  private Expression buildLocalCondition(List<Column> schema, UnionFind unionFind) {
    List<Expression> localConditions = new ArrayList<>();

    for (Column col : schema) {
      String fullyQualifiedName = col.getFullyQualifiedName();
      UnionFind.UnionElement element = unionFind.find(fullyQualifiedName);

      if (element == null)
        continue;

      // Equality constraint
      Double equalityConstraint = element.getEqualityConstraint();
      if (equalityConstraint != null) {
        localConditions.add(
            new EqualsTo().withLeftExpression(col).withRightExpression(new DoubleValue(equalityConstraint.toString())));
      }

      // Lower bound constraint
      Double lowerBound = element.getLowerBound();
      if (lowerBound != null) {
        localConditions.add(
            new GreaterThanEquals().withLeftExpression(col)
                .withRightExpression(new DoubleValue(lowerBound.toString())));
      }

      // Upper bound constraint
      Double upperBound = element.getUpperBound();
      if (upperBound != null) {
        localConditions.add(
            new MinorThanEquals().withLeftExpression(col).withRightExpression(new DoubleValue(upperBound.toString())));
      }
    }

    return combineConditions(null, localConditions);
  }

  // create and expression

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
