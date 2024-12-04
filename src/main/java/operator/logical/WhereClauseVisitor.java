package operator.logical;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

/**
 * The {@code WhereClauseVisitor} class processes the WHERE clause of a SQL query. It separates join
 * conditions from selection conditions and identifies whether a condition involves columns from
 * different tables (join) or from a single table (selection). This class extends {@link
 * ExpressionVisitorAdapter} from the JSqlParser library.
 *
 * <p>Additionally, it utilizes a {@link UnionFind} data structure to keep track of equality and
 * inequality constraints on columns, which can be used for query optimization.
 *
 * <h2>Key Features</h2>
 *
 * <ul>
 *   <li>Distinguishes between join conditions (columns from different tables) and selection
 *       conditions (within a single table).
 *   <li>Supports various comparison operators, including equality and inequalities.
 *   <li>Integrates with a Union-Find data structure for constraint propagation.
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * Expression whereCondition = ...; // Obtain the WHERE condition from a parsed SQL query
 * Map<String, Table> aliasMap = ...; // Map of table aliases to tables
 * WhereClauseVisitor visitor = new WhereClauseVisitor(whereCondition, aliasMap);
 * List<Expression> joinExpressions = visitor.getJoinExpressions();
 * Map<String, Expression> selectExpressions = visitor.getSelectExpressions();
 * UnionFind unionFind = visitor.getUnionFind();
 * }</pre>
 *
 * @see net.sf.jsqlparser.expression.ExpressionVisitorAdapter
 * @see net.sf.jsqlparser.expression.Expression
 * @see operator.logical.UnionFind
 */
public class WhereClauseVisitor extends ExpressionVisitorAdapter {
  /** List of join expressions extracted from the WHERE clause. */
  private List<Expression> joinExpressions = new ArrayList<>();

  /** Map of selection expressions for each table, keyed by table name. */
  private Map<String, Expression> selectExpressions = new HashMap<>();

  /** Map of table aliases to actual tables. */
  private Map<String, Table> aliasMap;

  /** Union-Find data structure to manage equality and inequality constraints among columns. */
  private UnionFind unionFind;

  /**
   * Default no-argument constructor for testing purposes. Initializes the visitor with no WHERE
   * condition and an empty alias map.
   */
  public WhereClauseVisitor() {
    this(null, new HashMap<>());
  }

  /**
   * Constructs a {@code WhereClauseVisitor} with the given WHERE condition and alias map. The
   * visitor immediately processes the WHERE condition upon construction.
   *
   * @param whereCondition the WHERE condition of the SQL query
   * @param aliasMap a map of table aliases to actual tables
   */
  public WhereClauseVisitor(Expression whereCondition, Map<String, Table> aliasMap) {
    this.aliasMap = aliasMap;
    this.unionFind = new UnionFind();
    if (whereCondition != null) {
      whereCondition.accept(this);
    }
  }

  /**
   * Visits an {@link AndExpression}, recursively visiting its left and right expressions.
   *
   * @param andExpr the {@code AndExpression} to visit
   */
  @Override
  public void visit(AndExpression andExpr) {
    andExpr.getLeftExpression().accept(this);
    andExpr.getRightExpression().accept(this);
  }

  /**
   * Visits an {@link EqualsTo} expression, handling equality conditions between columns and/or
   * values.
   *
   * @param expr the {@code EqualsTo} expression to visit
   */
  @Override
  public void visit(EqualsTo expr) {
    if (expr.getLeftExpression() instanceof Column && expr.getRightExpression() instanceof Column) {
      // Column = Column case
      handleColumnEqualsColumn(expr);
    } else if (expr.getLeftExpression() instanceof Column) {
      // Column = Value case
      handleColumnEqualsValue(expr, (Column) expr.getLeftExpression(), expr.getRightExpression());
    } else if (expr.getRightExpression() instanceof Column) {
      // Value = Column case
      handleColumnEqualsValue(expr, (Column) expr.getRightExpression(), expr.getLeftExpression());
    }
  }

  /**
   * Handles equality expressions where both sides are columns.
   *
   * @param expr the equality expression to handle
   */
  private void handleColumnEqualsColumn(EqualsTo expr) {
    Column leftCol = (Column) expr.getLeftExpression();
    Column rightCol = (Column) expr.getRightExpression();

    Table leftTable = getTableFromAlias(leftCol.getTable().getName());
    Table rightTable = getTableFromAlias(rightCol.getTable().getName());

    if (!leftTable.equals(rightTable)) {
      // Different tables - this is a join condition
      joinExpressions.add(expr);

      // Add to UnionFind for constraint propagation
      unionFind.union(
          unionFind.find(leftCol.getFullyQualifiedName()),
          unionFind.find(rightCol.getFullyQualifiedName()));
    } else {
      // Same table - this is a selection condition
      selectExpressions.put(
          leftTable.getName(), mergeExpressions(selectExpressions.get(leftTable.getName()), expr));

      // Add to UnionFind for potential constraint propagation
      unionFind.union(
          unionFind.find(leftCol.getFullyQualifiedName()),
          unionFind.find(rightCol.getFullyQualifiedName()));
    }
  }

  /**
   * Handles equality expressions where one side is a column and the other is a value.
   *
   * @param expr the equality expression to handle
   * @param column the column involved in the equality
   * @param valueExpr the value expression (should be a {@link DoubleValue})
   */
  private void handleColumnEqualsValue(EqualsTo expr, Column column, Expression valueExpr) {
    if (valueExpr instanceof DoubleValue) {
      Table table = getTableFromAlias(column.getTable().getName());
      selectExpressions.put(
          table.getName(), mergeExpressions(selectExpressions.get(table.getName()), expr));

      // Add constraint to UnionFind
      UnionFind.UnionElement element = unionFind.find(column.getFullyQualifiedName());
      unionFind.setEqualityConstraint(element, ((DoubleValue) valueExpr).getValue());
    }
  }

  /**
   * Visits a {@link GreaterThan} expression.
   *
   * @param expr the {@code GreaterThan} expression to visit
   */
  @Override
  public void visit(GreaterThan expr) {
    handleComparison(expr, true, false);
  }

  /**
   * Visits a {@link GreaterThanEquals} expression.
   *
   * @param expr the {@code GreaterThanEquals} expression to visit
   */
  @Override
  public void visit(GreaterThanEquals expr) {
    handleComparison(expr, true, true);
  }

  /**
   * Visits a {@link MinorThan} expression.
   *
   * @param expr the {@code MinorThan} expression to visit
   */
  @Override
  public void visit(MinorThan expr) {
    handleComparison(expr, false, false);
  }

  /**
   * Visits a {@link MinorThanEquals} expression.
   *
   * @param expr the {@code MinorThanEquals} expression to visit
   */
  @Override
  public void visit(MinorThanEquals expr) {
    handleComparison(expr, false, true);
  }

  /**
   * Handles inequality comparison expressions.
   *
   * @param op the comparison operator
   * @param isLower whether the operator represents a lower bound (e.g., &gt;, &gt;=)
   * @param inclusive whether the operator is inclusive (e.g., &gt;=, &lt;=)
   */
  private void handleComparison(ComparisonOperator op, boolean isLower, boolean inclusive) {
    Expression left = op.getLeftExpression();
    Expression right = op.getRightExpression();
    if (right instanceof LongValue r) {
      right = new DoubleValue(r.getStringValue());
    }
    if (left instanceof LongValue l ) {
      left = new DoubleValue(l.getStringValue());
    }


    if (left instanceof Column && right instanceof DoubleValue) {
      // Column OP Value case
      handleColumnComparison(op, (Column) left, (DoubleValue) right, isLower, inclusive);
    } else if (right instanceof Column && left instanceof DoubleValue) {
      // Value OP Column case
      handleColumnComparison(op, (Column) right, (DoubleValue) left, !isLower, inclusive);
    } else if (left instanceof Column && right instanceof Column) {
      // Column OP Column case
      handleColumnComparison(op, (Column) left, (Column) right);
    }
  }

  /**
   * Handles inequality comparisons between a column and a value.
   *
   * @param op the comparison operator
   * @param column the column involved in the comparison
   * @param value the value involved in the comparison
   * @param isLower whether the operator represents a lower bound
   * @param inclusive whether the operator is inclusive
   */
  private void handleColumnComparison(
      ComparisonOperator op, Column column, DoubleValue value, boolean isLower, boolean inclusive) {
    Table table = getTableFromAlias(column.getTable().getName());
    selectExpressions.put(
        table.getName(), mergeExpressions(selectExpressions.get(table.getName()), op));

    // Add bound to UnionFind
    UnionFind.UnionElement element = unionFind.find(column.getFullyQualifiedName());
    double val = value.getValue();
    if (isLower) {
      unionFind.setLowerBound(element, inclusive ? val : val + 1);
    } else {
      unionFind.setUpperBound(element, inclusive ? val : val - 1);
    }
  }

  /**
   * Handles inequality comparisons between two columns.
   *
   * @param op the comparison operator
   * @param leftCol the left column in the comparison
   * @param rightCol the right column in the comparison
   */
  private void handleColumnComparison(ComparisonOperator op, Column leftCol, Column rightCol) {
    Table leftTable = getTableFromAlias(leftCol.getTable().getName());
    Table rightTable = getTableFromAlias(rightCol.getTable().getName());

    if (!leftTable.equals(rightTable)) {
      // Different tables - this is a join condition
      joinExpressions.add(op);
    } else {
      // Same table - this is a selection condition
      selectExpressions.put(
          leftTable.getName(), mergeExpressions(selectExpressions.get(leftTable.getName()), op));
    }
  }

  /**
   * Retrieves the actual table from an alias.
   *
   * @param alias the table alias
   * @return the actual {@link Table} object
   */
  private Table getTableFromAlias(String alias) {
    return aliasMap.get(alias);
  }

  /**
   * Merges two expressions using an {@link AndExpression}.
   *
   * @param existingExpr the existing expression
   * @param newExpr the new expression to merge
   * @return the merged expression
   */
  private Expression mergeExpressions(Expression existingExpr, Expression newExpr) {
    if (existingExpr == null) {
      return newExpr;
    } else {
      return new AndExpression(existingExpr, newExpr);
    }
  }

  /**
   * Returns the list of join expressions extracted from the WHERE clause.
   *
   * @return a list of join expressions
   */
  public List<Expression> getJoinExpressions() {
    return joinExpressions;
  }

  /**
   * Returns the map of selection expressions for each table.
   *
   * @return a map of table names to selection expressions
   */
  public Map<String, Expression> getSelectExpressions() {
    return selectExpressions;
  }

  /**
   * Returns the {@link UnionFind} structure containing constraints on columns.
   *
   * @return the UnionFind structure
   */
  public UnionFind getUnionFind() {
    return unionFind;
  }

  /**
   * Processes an equality condition between two attributes for testing purposes.
   *
   * @param attr1 the first attribute (fully qualified column name)
   * @param attr2 the second attribute (fully qualified column name)
   */
  public void visitEquality(String attr1, String attr2) {
    UnionFind.UnionElement elt1 = unionFind.find(attr1);
    UnionFind.UnionElement elt2 = unionFind.find(attr2);
    unionFind.union(elt1, elt2);
  }

  /**
   * Processes a bound condition on an attribute for testing purposes.
   *
   * @param attr the attribute (fully qualified column name)
   * @param operator the operator (e.g., "=", "&lt;", "&lt;=", "&gt;", "&gt;=")
   * @param value the value for the bound
   */
  public void visitBound(String attr, String operator, double value) {
    UnionFind.UnionElement element = unionFind.find(attr);
    switch (operator) {
      case "=":
        unionFind.setEqualityConstraint(element, value);
        break;
      case "<":
        unionFind.setUpperBound(element, value - 1);
        break;
      case "<=":
        unionFind.setUpperBound(element, value);
        break;
      case ">":
        unionFind.setLowerBound(element, value + 1);
        break;
      case ">=":
        unionFind.setLowerBound(element, value);
        break;
      default:
        // Handle unsupported operators if necessary
        break;
    }
  }

  /**
   * Retrieves the lower bound of an attribute from the UnionFind structure.
   *
   * @param attr the attribute (fully qualified column name)
   * @return the lower bound, or {@code null} if not set
   */
  public Double getLowerBound(String attr) {
    UnionFind.UnionElement element = unionFind.find(attr);
    return element != null ? element.getLowerBound() : null;
  }

  /**
   * Retrieves the upper bound of an attribute from the UnionFind structure.
   *
   * @param attr the attribute (fully qualified column name)
   * @return the upper bound, or {@code null} if not set
   */
  public Double getUpperBound(String attr) {
    UnionFind.UnionElement element = unionFind.find(attr);
    return element != null ? element.getUpperBound() : null;
  }

  /**
   * Retrieves the equality constraint of an attribute from the UnionFind structure.
   *
   * @param attr the attribute (fully qualified column name)
   * @return the equality constraint value, or {@code null} if not set
   */
  public Double getEqualityConstraint(String attr) {
    UnionFind.UnionElement element = unionFind.find(attr);
    return element != null ? element.getEqualityConstraint() : null;
  }
}
