package join_algorithms;

import common.ExpressionEvaluator;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import operator.physical.Operator;
import java.util.Map;
import java.util.List;
import common.Tuple;
import java.util.ArrayList;

public class BNLJ extends Operator {

  private Operator leftChild;
  private Operator rightChild;
  private Expression joinCondition;

  private Map<String, String> tableAliases;

  private ExpressionEvaluator expressionEvaluator;

  private int numberPages;
  private final int TUPLESONPAGE = 1024;

  private List<Tuple> block = new ArrayList<>();
  private boolean isBlockEmpty = true;

  private int outerPointer;
  private int innerPointer;

  private Tuple leftTuple;
  private Tuple rightTuple;

  public BNLJ(Operator leftChild, Operator rightChild, Expression joinCondition, Map<String, String> tableAliases,
      int numberPages) {
    super(combineSchemas(leftChild.getOutputSchema(), rightChild.getOutputSchema()));
    this.leftChild = leftChild;
    this.rightChild = rightChild;
    this.joinCondition = joinCondition;
    this.tableAliases = tableAliases;
    this.expressionEvaluator = new ExpressionEvaluator(tableAliases);
    this.numberPages = numberPages;
    this.outerPointer = 0;
    this.innerPointer = 0;
    getBlock();
  }

  public List<Tuple> getBlock() {
    for (int i = 0; i < numberPages * TUPLESONPAGE; i++) {
      Tuple tuple = leftChild.getNextTuple();
      if (tuple == null) {
        break;
      }
      block.add(tuple);
    }
    return block;
  }

  @Override
  public Tuple getNextTuple() {

    if (isBlockEmpty) {
      block = getBlock();
      outerPointer = 0;
      innerPointer = 0;
      if (block.isEmpty()) {
        return null;
      }
    }

    leftTuple = block.get(outerPointer);
    rightTuple = rightChild.getNextTuple();
    while (leftTuple != null && rightTuple != null) {
      Tuple joinedTuple = joinTuples(leftTuple, rightTuple);
      innerPointer++;
      outerPointer++;
      if (joinCondition == null || evaluateJoinCondition(joinedTuple)) {
        if (outerPointer >= numberPages * TUPLESONPAGE) {
          isBlockEmpty = true;
        }
        return joinedTuple;
      }

    }

    return null;

  }

  @Override
  public void reset() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'reset'");
  }

  private static ArrayList<Column> combineSchemas(
      ArrayList<Column> leftSchema, ArrayList<Column> rightSchema) {
    ArrayList<Column> combinedSchema = new ArrayList<>();

    // Process columns from the left schema and add them to the combined schema
    for (Column col : leftSchema) {
      Table table = new Table();
      table.setName(col.getTable().getName());

      // Retain table alias if present, or use the table name as the alias if not
      if (col.getTable().getAlias() != null) {
        table.setAlias(new Alias(col.getTable().getAlias().getName()));
      } else {
        table.setAlias(new Alias(col.getTable().getName()));
      }

      // Create a new column for the combined schema
      Column newCol = new Column(table, col.getColumnName());
      combinedSchema.add(newCol);
    }

    // Process columns from the right schema and add them to the combined schema
    for (Column col : rightSchema) {
      Table table = new Table();
      table.setName(col.getTable().getName());

      // Retain table alias if present, or use the table name as the alias if not
      if (col.getTable().getAlias() != null) {
        table.setAlias(new Alias(col.getTable().getAlias().getName()));
      } else {
        table.setAlias(new Alias(col.getTable().getName()));
      }

      // Create a new column for the combined schema
      Column newCol = new Column(table, col.getColumnName());
      combinedSchema.add(newCol);
    }

    return combinedSchema;
  }

  private Tuple joinTuples(Tuple left, Tuple right) {
    // Create a new list by concatenating the elements of both tuples
    ArrayList<Integer> combinedData = new ArrayList<>(left.getAllElements());
    combinedData.addAll(right.getAllElements());
    return new Tuple(combinedData); // Return the joined tuple
  }

  private boolean evaluateJoinCondition(Tuple tuple) {
    return expressionEvaluator.evaluate(joinCondition, tuple, this.outputSchema);
  }

}
