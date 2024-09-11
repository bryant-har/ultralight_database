package common;

import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;

/** Class to represent expression visitors. */
public class ExprVisitor extends ExpressionVisitorAdapter {

  private boolean resultBool = true;
  private long resultLong = 0;
  private ExpressionContext context;
  private Tuple values;

  public ExprVisitor(Tuple toCheck, ExpressionContext context) {
    this.context = context;
    values = toCheck;
  }

  @Override
  public void visit(LongValue value) {
    resultLong = value.getValue();
    resultBool = resultLong != 0;
  }

  @Override
  public void visit(EqualsTo expr) {
    expr.getLeftExpression().accept(this);
    long lval = this.resultLong;
    expr.getRightExpression().accept(this);
    long rval = this.resultLong;
    resultBool = lval == rval;
  }

  public boolean getResult() {
    return resultBool;
  }
}
