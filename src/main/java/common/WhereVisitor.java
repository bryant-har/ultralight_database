package common;

import java.util.*;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

/**
 * The WhereVisitor class processes the WHERE clause of a SQL query, separating join conditions from
 * selection conditions. It identifies whether a condition involves columns from different tables
 * (join) or from a single table (selection).
 */
public class WhereVisitor extends ExpressionVisitorAdapter {

  private List<Expression> joinExpressions = new ArrayList<>();
  private Map<String, Expression> selectExpressions = new HashMap<>();
  private Map<String, Table> aliasMap = new HashMap<>();

  /**
   * Constructor that processes the provided WHERE condition.
   *
   * @param whereCondition The WHERE clause expression to process.
   */
  public WhereVisitor(Expression whereCondition, Map<String, Table> aliasMap) {
    this.aliasMap = aliasMap;
    if (whereCondition != null) {
      whereCondition.accept(this);
    }
  }

  /**
   * Visits AND expressions and processes both left and right sides.
   *
   * @param andExpr The AND expression to process.
   */
  @Override
  public void visit(AndExpression andExpr) {
    andExpr.getLeftExpression().accept(this);
    andExpr.getRightExpression().accept(this);
  }

  /**
   * Visits EQUALS expressions and processes them as binary expressions.
   *
   * @param expr The EqualsTo expression to process.
   */
  @Override
  public void visit(EqualsTo expr) {
    handleBinaryExpression(expr);
  }

  /**
   * Visits GREATER THAN expressions and processes them as binary expressions.
   *
   * @param expr The GreaterThan expression to process.
   */
  @Override
  public void visit(GreaterThan expr) {
    handleBinaryExpression(expr);
  }

  /**
   * Visits GREATER THAN OR EQUALS expressions and processes them as binary expressions.
   *
   * @param expr The GreaterThanEquals expression to process.
   */
  @Override
  public void visit(GreaterThanEquals expr) {
    handleBinaryExpression(expr);
  }

  /**
   * Visits LESS THAN expressions and processes them as binary expressions.
   *
   * @param expr The MinorThan expression to process.
   */
  @Override
  public void visit(MinorThan expr) {
    handleBinaryExpression(expr);
  }

  /**
   * Visits LESS THAN OR EQUALS expressions and processes them as binary expressions.
   *
   * @param expr The MinorThanEquals expression to process.
   */
  @Override
  public void visit(MinorThanEquals expr) {
    handleBinaryExpression(expr);
  }

  /**
   * Handles binary expressions by determining whether they are join or selection conditions.
   *
   * @param expr The binary expression to process.
   */
  /// Helper method to handle binary expressions
  private void handleBinaryExpression(Expression expr) {
    if (expr instanceof BinaryExpression) {
      BinaryExpression binaryExpr = (BinaryExpression) expr;

      if (isJoinCondition(binaryExpr)) {
        joinExpressions.add(binaryExpr);

        // Expression flippedExpr = flipTables(binaryExpr);
        // joinExpressions.add(flippedExpr);

      } else {
        // This is a selection condition
        Column column = (Column) binaryExpr.getLeftExpression(); // Safely cast as BinaryExpression
        Table table = getTableFromAlias(column.getTable().getName());
        selectExpressions.put(
            table.getName(), mergeExpressions(selectExpressions.get(table.getName()), binaryExpr));
        
      }
    }
  }

  // private Expression flipTables(BinaryExpression expr) {
  //   Expression left = expr.getLeftExpression();
  //   Expression right = expr.getRightExpression();
  //   // String operator - expr.getStringOpeartor
  //   expr.setLeftExpression(right);
  //   expr.setRightExpression(left);
  //   return expr;
  // }

  /**
   * Checks if the binary expression represents a join condition (involves columns from different
   * tables).
   *
   * @param expr The binary expression to check.
   * @return True if the expression is a join condition, false otherwise.
   */
  // Check if the expression is a join condition (both sides are columns from different tables)
  private boolean isJoinCondition(BinaryExpression expr) {
    if (expr.getLeftExpression() instanceof Column && expr.getRightExpression() instanceof Column) {
      Column leftColumn = (Column) expr.getLeftExpression();
      Column rightColumn = (Column) expr.getRightExpression();

      Table leftTable = getTableFromAlias(leftColumn.getTable().getName());
      Table rightTable = getTableFromAlias(rightColumn.getTable().getName());

      boolean checkJoin = !leftTable.equals(rightTable);

      System.out.println("checkJoin: " + checkJoin);
      return checkJoin;
    }
    return false;
  }

  /**
   * Merges multiple expressions for the same table using AND logic.
   *
   * @param existingExpr The existing expression for the table.
   * @param newExpr The new expression to add.
   * @return The merged expression combining both.
   */
  // Merges multiple expressions for the same table
  private Expression mergeExpressions(Expression existingExpr, Expression newExpr) {
    if (existingExpr == null) {
      return newExpr; // No existing expression, just use the new one
    } else {
      return new AndExpression(existingExpr, newExpr); // Combine with AND
    }
  }

  // TODO potentially other visit operators/ methods...

  /**
   * Returns the list of join conditions extracted from the WHERE clause.
   *
   * @return A list of join expressions.
   */
  public List<Expression> getJoinExpressions() {
    return joinExpressions;
  }

  /**
   * Returns the map of selection conditions for each table.
   *
   * @return A map of selection expressions with the table name as the key.
   */
  public Map<String, Expression> getSelectExpressions() {
    return selectExpressions;
  }

  /**
   * Helper method to get the base table name from an alias or table name.
   *
   * @param aliasOrTableName The alias or table name.
   * @return The base table name.
   */
  private Table getTableFromAlias(String alias) {
    // System.out.println("w: " + aliasMap);
    return aliasMap.get(alias);
  }
}
