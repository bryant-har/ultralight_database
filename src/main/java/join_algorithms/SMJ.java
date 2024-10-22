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
import java.util.Collections;

public class SMJ extends Operator {

  private Operator leftChild;
  private Operator rightChild;
  private Expression joinCondition;

  private Map<String, String> tableAliases;

  private ExpressionEvaluator expressionEvaluator;

  private List<Tuple> leftBlock;
  private List<Tuple> rightBlock;
  private int leftPointer;
  private int rightPointer;

  private Tuple leftTuple;
  private Tuple rightTuple;

  public SMJ(Operator leftChild, Operator rightChild, Expression joinCondition, Map<String, String> tableAliases) {
    super(combineSchemas(leftChild.getOutputSchema(), rightChild.getOutputSchema()));
    this.leftChild = leftChild; // TODO: This should be a Sort Operator
    this.rightChild = rightChild; // TODO: This should be a Sort Operator
    this.joinCondition = joinCondition;
    this.tableAliases = tableAliases;
    this.expressionEvaluator = new ExpressionEvaluator(tableAliases);
    this.leftBlock = new ArrayList<>();
    this.rightBlock = new ArrayList<>();
    getSortedBlocks();
  }

  private void getSortedBlocks() {
    Tuple tuple;
    while ((tuple = leftChild.getNextTuple()) != null) {
      leftBlock.add(tuple);
    }
    while ((tuple = rightChild.getNextTuple()) != null) {
      rightBlock.add(tuple);
    }

    // TODO: In theory this should still work as equal tuples will be local to each other,
    // But really we should convert left and right child to new Sort Operators
    Collections.sort(leftBlock);
    Collections.sort(rightBlock);
  }

  @Override
  public Tuple getNextTuple() {
    while (leftPointer < leftBlock.size() && rightPointer < rightBlock.size()) {
      leftTuple = leftBlock.get(leftPointer);
      rightTuple = rightBlock.get(rightPointer);

      int comparison = leftTuple.compareTo(rightTuple);
      if (comparison == 0) {
        Tuple joinedTuple = joinTuples(leftTuple, rightTuple);
        rightPointer++;
        if (joinCondition == null || evaluateJoinCondition(joinedTuple)) {
          return joinedTuple;
        }
      } else if (comparison < 0) {
        leftPointer++;
      } else {
        rightPointer++;
      }
    }
    return null;
  }

  @Override
  public void reset() {
    leftChild.reset();
    rightChild.reset();
    leftBlock.clear();
    rightBlock.clear();
    leftPointer = 0;
    rightPointer = 0;
    getSortedBlocks();
  }

  private static ArrayList<Column> combineSchemas(
      ArrayList<Column> leftSchema, ArrayList<Column> rightSchema) {
    ArrayList<Column> combinedSchema = new ArrayList<>();
    for (Column col : leftSchema) {
      Table table = new Table();
      table.setName(col.getTable().getName());
      if (col.getTable().getAlias() != null) {
        table.setAlias(new Alias(col.getTable().getAlias().getName()));
      } else {
        table.setAlias(new Alias(col.getTable().getName()));
      }
      Column newCol = new Column(table, col.getColumnName());
      combinedSchema.add(newCol);
    }
    for (Column col : rightSchema) {
      Table table = new Table();
      table.setName(col.getTable().getName());
      if (col.getTable().getAlias() != null) {
        table.setAlias(new Alias(col.getTable().getAlias().getName()));
      } else {
        table.setAlias(new Alias(col.getTable().getName()));
      }
      Column newCol = new Column(table, col.getColumnName());
      combinedSchema.add(newCol);
    }
    return combinedSchema;
  }

  private Tuple joinTuples(Tuple left, Tuple right) {
    ArrayList<Integer> combinedData = new ArrayList<>(left.getAllElements());
    combinedData.addAll(right.getAllElements());
    return new Tuple(combinedData);
  }

  private boolean evaluateJoinCondition(Tuple tuple) {
    return expressionEvaluator.evaluate(joinCondition, tuple, this.outputSchema);
  }

}
