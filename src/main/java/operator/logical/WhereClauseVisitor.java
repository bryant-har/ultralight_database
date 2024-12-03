package operator.logical;

import java.util.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;

public class WhereClauseVisitor {
  private final UnionFind unionFind;
  private final List<Expression> residualConditions;

  public WhereClauseVisitor() {
    this.unionFind = new UnionFind();
    this.residualConditions = new ArrayList<>();
  }

  /** Processes the WHERE clause and builds union find. */
  public List<Expression> process(Expression whereClause) {
    if (whereClause == null) return residualConditions;

    List<Expression> conditions = extractConditions(whereClause);
    for (Expression condition : conditions) {
      if (condition instanceof ComparisonOperator) {
        processComparison((ComparisonOperator) condition);
      } else {
        residualConditions.add(condition);
      }
    }

    return residualConditions;
  }

  /** Processes a comparison operato handling both equality and bounds. */
  private void processComparison(ComparisonOperator comparison) {
    Expression left = comparison.getLeftExpression();
    Expression right = comparison.getRightExpression();

    if (comparison instanceof EqualsTo) {
      processEquality((EqualsTo) comparison);
    } else if (left instanceof Column && isNumericValue(right)) {
      // Handle attribute OP value (e.g., R.A > 5)
      String attr = ((Column) left).getFullyQualifiedName();
      double value = extractNumericValue(right);
      visitBound(attr, comparison.getStringExpression(), value);
    } else {
      // Add to residual conditions if not in the expected format
      residualConditions.add(comparison);
    }
  }

  /** Processes equality conditions */
  private void processEquality(EqualsTo equality) {
    Expression left = equality.getLeftExpression();
    Expression right = equality.getRightExpression();

    if (left instanceof Column && right instanceof Column) {
      visitEquality(
          ((Column) left).getFullyQualifiedName(), ((Column) right).getFullyQualifiedName());
    } else if (left instanceof Column && isNumericValue(right)) {
      String attr = ((Column) left).getFullyQualifiedName();
      double value = extractNumericValue(right);
      visitBound(attr, "=", value);
    } else {
      // not supported, it is set aside
      residualConditions.add(equality);
    }
  }

  /** Extracts individual conditions from an AND expression. */
  private List<Expression> extractConditions(Expression expr) {
    List<Expression> conditions = new ArrayList<>();
    if (expr instanceof AndExpression) {
      conditions.addAll(extractConditions(((AndExpression) expr).getLeftExpression()));
      conditions.addAll(extractConditions(((AndExpression) expr).getRightExpression()));
    } else {
      conditions.add(expr);
    }
    return conditions;
  }

  private void visitEquality(String attr1, String attr2) {
    unionFind.union(unionFind.find(attr1), unionFind.find(attr2));
  }

  private void visitBound(String attr, String operator, double value) {
    UnionFind.UnionElement elt = unionFind.find(attr);
    switch (operator) {
      case "=" -> unionFind.setEqualityConstraint(elt, value);
      case "<" -> unionFind.setUpperBound(elt, value - 1);
      case "<=" -> unionFind.setUpperBound(elt, value);
      case ">" -> unionFind.setLowerBound(elt, value + 1);
      case ">=" -> unionFind.setLowerBound(elt, value);
    }
  }

  private boolean isNumericValue(Expression expr) {
    return expr instanceof LongValue || expr instanceof DoubleValue;
  }

  private double extractNumericValue(Expression expr) {
    if (expr instanceof LongValue) {
      return ((LongValue) expr).getValue();
    } else if (expr instanceof DoubleValue) {
      return ((DoubleValue) expr).getValue();
    }
    throw new IllegalArgumentException("Expression is not a numeric value");
  }

  public UnionFind getUnionFind() {
    return this.unionFind;
  }
}
