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

  private List<Tuple> block; 
  private boolean isBlockEmpty;

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
    this.block = new ArrayList<>(); 
    getBlock();
    this.isBlockEmpty = false;

  }

  public List<Tuple> getBlock() {
    block.clear();
    
    for (int i = 0; i < numberPages * TUPLESONPAGE; i++) {
      Tuple tuple = leftChild.getNextTuple();
      if (tuple == null) {
        break;
      }
      block.add(tuple);
    }
    return block;
  }

  /**
 * Retrieves the next joined tuple from the BNLJ
 * 
 * This method gets blocks of tuples from the left child/aoutter nd 
 * tuples from the right to produce joined tuples, then checks join cond..
 * 
 * @return the next joined tuple, or null if no more tuples exist.
 */
  @Override
  public Tuple getNextTuple() {

    while (true) {

      if (isBlockEmpty) {
        block = getBlock();

        if (block.isEmpty()) {
          return null;
        }
        isBlockEmpty = false;
        outerPointer = 0;
        rightChild.reset();
      }

     
      if (outerPointer >= block.size()) {
        // processed all tuples in the current block
        isBlockEmpty = true;
        continue; // on next iter. of while true, will get new block
      }

      leftTuple = block.get(outerPointer);
      rightTuple = rightChild.getNextTuple();

      if (rightTuple == null) {
        // reset right/inner and move to next tupe in the block
        outerPointer++;
        rightChild.reset();
        innerPointer = 0;
        continue;

      }

      Tuple joinedTuple = joinTuples(leftTuple, rightTuple);
      innerPointer++;

      if (joinCondition == null || evaluateJoinCondition(joinedTuple)) {
        // Use this as a debug statemnt 
        // System.out.println("returning out joinedTuple " + joinedTuple + " innerPointer: " + innerPointer
        //     + " outerPointer: " + outerPointer);
        return joinedTuple;

      }
    }

  }

  @Override
  public void reset() {
    leftChild.reset();
    rightChild.reset();
    block.clear();
    isBlockEmpty = true;
    outerPointer = 0;
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
