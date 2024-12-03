package operator.logical;

import java.util.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import operator.logical.UnionFind;
import net.sf.jsqlparser.expression.DoubleValue;

/**
 * The WhereVisitor class processes the WHERE clause of a SQL query, separating
 * join conditions from
 * selection conditions. It identifies whether a condition involves columns from
 * different tables
 * (join) or from a single table (selection).
 */
public class WhereClauseVisitor extends ExpressionVisitorAdapter {
  private List<Expression> joinExpressions = new ArrayList<>();
  private Map<String, Expression> selectExpressions = new HashMap<>();
  private Map<String, Table> aliasMap;
  private UnionFind unionFind;

  // Add no-arg constructor for testing
  public WhereClauseVisitor() {
    this(null, new HashMap<>());
  }

  public WhereClauseVisitor(Expression whereCondition, Map<String, Table> aliasMap) {
    this.aliasMap = aliasMap;
    this.unionFind = new UnionFind();
    if (whereCondition != null) {
      whereCondition.accept(this);
    }
  }

  @Override
  public void visit(AndExpression andExpr) {
    andExpr.getLeftExpression().accept(this);
    andExpr.getRightExpression().accept(this);
  }

  @Override
  public void visit(EqualsTo expr) {
    if (expr.getLeftExpression() instanceof Column && expr.getRightExpression() instanceof Column) {
      Column leftCol = (Column) expr.getLeftExpression();
      Column rightCol = (Column) expr.getRightExpression();

      Table leftTable = getTableFromAlias(leftCol.getTable().getName());
      Table rightTable = getTableFromAlias(rightCol.getTable().getName());

      if (!leftTable.equals(rightTable)) {
        // Different tables - this is a join condition
        joinExpressions.add(expr);

        // Also add to UnionFind for constraint propagation
        UnionFind.UnionElement leftElt = unionFind.find(leftCol.getFullyQualifiedName());
        UnionFind.UnionElement rightElt = unionFind.find(rightCol.getFullyQualifiedName());
        unionFind.union(leftElt, rightElt);
      } else {
        // Same table - this is a selection condition
        selectExpressions.put(
            leftTable.getName(),
            mergeExpressions(selectExpressions.get(leftTable.getName()), expr));

        // Still add to UnionFind for potential constraint propagation
        UnionFind.UnionElement leftElt = unionFind.find(leftCol.getFullyQualifiedName());
        UnionFind.UnionElement rightElt = unionFind.find(rightCol.getFullyQualifiedName());
        unionFind.union(leftElt, rightElt);
      }
    } else if (expr.getLeftExpression() instanceof Column) {
      // Column = Value case
      Column col = (Column) expr.getLeftExpression();
      if (expr.getRightExpression() instanceof DoubleValue) {
        Table table = getTableFromAlias(col.getTable().getName());
        selectExpressions.put(
            table.getName(),
            mergeExpressions(selectExpressions.get(table.getName()), expr));

        // Add constraint to UnionFind
        UnionFind.UnionElement element = unionFind.find(col.getFullyQualifiedName());
        unionFind.setEqualityConstraint(element, ((DoubleValue) expr.getRightExpression()).getValue());
      }
    } else if (expr.getRightExpression() instanceof Column) {
      // Value = Column case
      Column col = (Column) expr.getRightExpression();
      if (expr.getLeftExpression() instanceof DoubleValue) {
        Table table = getTableFromAlias(col.getTable().getName());
        selectExpressions.put(
            table.getName(),
            mergeExpressions(selectExpressions.get(table.getName()), expr));

        // Add constraint to UnionFind
        UnionFind.UnionElement element = unionFind.find(col.getFullyQualifiedName());
        unionFind.setEqualityConstraint(element, ((DoubleValue) expr.getLeftExpression()).getValue());
      }
    }
  }

  // Add handling for inequality comparisons
  @Override
  public void visit(GreaterThan expr) {
    handleComparison(expr, true, false);
  }

  @Override
  public void visit(GreaterThanEquals expr) {
    handleComparison(expr, true, true);
  }

  @Override
  public void visit(MinorThan expr) {
    handleComparison(expr, false, false);
  }

  @Override
  public void visit(MinorThanEquals expr) {
    handleComparison(expr, false, true);
  }

  private void handleComparison(ComparisonOperator op, boolean isLower, boolean inclusive) {
    Expression left = op.getLeftExpression();
    Expression right = op.getRightExpression();

    // Handle Column OP Value case
    if (left instanceof Column && right instanceof DoubleValue) {
      Column col = (Column) left;
      Table table = getTableFromAlias(col.getTable().getName());
      selectExpressions.put(
          table.getName(),
          mergeExpressions(selectExpressions.get(table.getName()), op));

      // Add bound to UnionFind
      UnionFind.UnionElement element = unionFind.find(col.getFullyQualifiedName());
      double value = ((DoubleValue) right).getValue();
      if (isLower) {
        unionFind.setLowerBound(element, inclusive ? value : value + 1);
      } else {
        unionFind.setUpperBound(element, inclusive ? value : value - 1);
      }
    }
    // Handle Value OP Column case
    else if (right instanceof Column && left instanceof DoubleValue) {
      Column col = (Column) right;
      Table table = getTableFromAlias(col.getTable().getName());
      selectExpressions.put(
          table.getName(),
          mergeExpressions(selectExpressions.get(table.getName()), op));

      // Add bound to UnionFind
      UnionFind.UnionElement element = unionFind.find(col.getFullyQualifiedName());
      double value = ((DoubleValue) left).getValue();
      if (isLower) {
        unionFind.setUpperBound(element, inclusive ? value : value - 1);
      } else {
        unionFind.setLowerBound(element, inclusive ? value : value + 1);
      }
    }
    // Handle Column OP Column case
    else if (left instanceof Column && right instanceof Column) {
      Column leftCol = (Column) left;
      Column rightCol = (Column) right;
      Table leftTable = getTableFromAlias(leftCol.getTable().getName());
      Table rightTable = getTableFromAlias(rightCol.getTable().getName());

      if (!leftTable.equals(rightTable)) {
        // Different tables - this is a join condition
        joinExpressions.add(op);
      } else {
        // Same table - this is a selection condition
        selectExpressions.put(
            leftTable.getName(),
            mergeExpressions(selectExpressions.get(leftTable.getName()), op));
      }
    }
  }

  // Keep existing helper methods
  private Table getTableFromAlias(String alias) {
    return aliasMap.get(alias);
  }

  private Expression mergeExpressions(Expression existingExpr, Expression newExpr) {
    if (existingExpr == null) {
      return newExpr;
    } else {
      return new AndExpression(existingExpr, newExpr);
    }
  }

  public List<Expression> getJoinExpressions() {
    return joinExpressions;
  }

  public Map<String, Expression> getSelectExpressions() {
    return selectExpressions;
  }

  public UnionFind getUnionFind() {
    return unionFind;
  }

  public void visitEquality(String attr1, String attr2) {
    UnionFind.UnionElement elt1 = unionFind.find(attr1);
    UnionFind.UnionElement elt2 = unionFind.find(attr2);
    unionFind.union(elt1, elt2);
  }

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
    }
  }

  // Add these accessor methods for testing
  public Double getLowerBound(String attr) {
    UnionFind.UnionElement element = unionFind.find(attr);
    return element != null ? element.getLowerBound() : null;
  }

  public Double getUpperBound(String attr) {
    UnionFind.UnionElement element = unionFind.find(attr);
    return element != null ? element.getUpperBound() : null;
  }

  public Double getEqualityConstraint(String attr) {
    UnionFind.UnionElement element = unionFind.find(attr);
    return element != null ? element.getEqualityConstraint() : null;
  }
}