package operator;

import common.ExprVisitor;
import common.ExpressionContext;
import common.Tuple;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

/** Class to represent scan operators. e.g. select * from table */
public class SelectOperator extends Operator {
  private final ScanOperator scanner;
  private final ExpressionContext exprCtx;
  private Expression expr;

  public SelectOperator(ArrayList<Column> outputSchema, String tableName, Expression expr) {
    super(outputSchema);
    scanner = new ScanOperator(outputSchema, tableName);
    this.exprCtx = new ExpressionContext(outputSchema);
    this.expr = expr;
  }

  /** Resets cursor on the operator to the beginning */
  public void reset() {
    scanner.reset();
  }
  ;

  /**
   * Get next tuple from operator
   *
   * @return next Tuple, or null if we are at the end
   */
  public Tuple getNextTuple() {
    Tuple curr = scanner.getNextTuple();
    while (curr != null) {
      ExprVisitor exprVisitor = new ExprVisitor(curr, exprCtx);
      if (expr != null) {      
        expr.accept(exprVisitor);
      }
      if (exprVisitor.getResult()) {
        return curr;
      }
      curr = scanner.getNextTuple();
    }
    return null;
  }

  /**
   * Collects all tuples of this operator.
   *
   * @return A list of Tuples.
   */
  public List<Tuple> getAllTuples() {
    Tuple t;
    List<Tuple> tuples = new ArrayList<>();
    while ((t = getNextTuple()) != null) {
      tuples.add(t);
    }

    return tuples;
  }

  /**
   * Iterate through output of operator and send it all to the specified printStream)
   *
   * @param printStream stream to receive output, one tuple per line.
   */
  public void dump(PrintStream printStream) {
    Tuple t;
    while ((t = this.getNextTuple()) != null) {
      printStream.println(t);
    }
  }
}
