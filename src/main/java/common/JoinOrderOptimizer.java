package common;

import java.util.*;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import operator.logical.UnionFind;
import operator.physical.Operator;
import operator.physical.ScanOperator;

/**
 * The JoinOrderOptimizer class is responsible for finding the optimal join order for a query using
 * a cost-based dynamic programming approach. It takes into account the sizes of relations,
 * selectivity of join conditions, and intermediate results.
 */
public class JoinOrderOptimizer {
  private final List<Operator> baseOperators; // List of base relation operators
  private final List<Expression> joinConditions; // List of join conditions
  private final UnionFind unionFind; // UnionFind structure for equality constraints
  private final DBCatalog dbCatalog; // Database catalog for metadata

  // Dynamic programming tables
  private Map<BitSet, Integer> dpCost; // Stores the minimum cost for subsets
  private Map<BitSet, List<Integer>> dpOrder; // Stores the optimal order for subsets
  private Map<BitSet, Integer> dpSize; // Stores the estimated size for subsets
  private Map<BitSet, Map<String, Integer>> dpVValues; // Stores V-values (distinct values)
  private Map<BitSet, List<Expression>> dpConditions; // Stores join conditions for subsets

  /**
   * Constructs the JoinOrderOptimizer with the given inputs.
   *
   * @param baseOperators List of base relation operators.
   * @param joinConditions List of join conditions.
   * @param unionFind UnionFind structure for equality constraints.
   * @param dbCatalog Database catalog for metadata.
   */
  public JoinOrderOptimizer(
      List<Operator> baseOperators,
      List<Expression> joinConditions,
      UnionFind unionFind,
      DBCatalog dbCatalog) {
    this.baseOperators = baseOperators;
    this.joinConditions = joinConditions;
    this.unionFind = unionFind;
    this.dbCatalog = dbCatalog;

    // Initialize dynamic programming tables
    this.dpCost = new HashMap<>();
    this.dpOrder = new HashMap<>();
    this.dpSize = new HashMap<>();
    this.dpVValues = new HashMap<>();
    this.dpConditions = new HashMap<>();

    computeOptimalJoinOrder(); // Compute the optimal join order
  }

  /** Computes the optimal join order using a dynamic programming approach. */
  private void computeOptimalJoinOrder() {
    int n = baseOperators.size();

    // Initialize base cases (single relations)
    for (int i = 0; i < n; i++) {
      BitSet set = new BitSet(n);
      set.set(i);

      // Get base relation stats
      Operator op = baseOperators.get(i);
      String tableName = getTableName(op);

      // Cost of single relation is 0
      dpCost.put(set, 0);

      // Order is just the single relation
      List<Integer> order = new ArrayList<>();
      order.add(i);
      dpOrder.put(set, order);

      // Size is the relation size after any selections
      dpSize.put(set, estimateBaseSize(op));

      // V-values are from the catalog, adjusted for selections
      dpVValues.put(set, computeBaseVValues(op));

      // No join conditions for single relation
      dpConditions.put(set, new ArrayList<>());
    }

    // Consider increasingly larger subsets
    for (int size = 2; size <= n; size++) {
      for (BitSet set : generateSubsets(n, size)) {
        int bestCost = Integer.MAX_VALUE;
        List<Integer> bestOrder = null;
        Map<String, Integer> bestVValues = null;
        List<Expression> bestConditions = null;
        int bestSize = 0;

        // Try each possible way to split this subset
        for (BitSet leftSet : generateAllSplits(set)) {
          BitSet rightSet = (BitSet) set.clone();
          rightSet.andNot(leftSet);

          // Get stats for both sides
          int leftSize = dpSize.get(leftSet);
          int rightSize = dpSize.get(rightSet);
          Map<String, Integer> leftVValues = dpVValues.get(leftSet);
          Map<String, Integer> rightVValues = dpVValues.get(rightSet);

          // Find applicable join conditions
          List<Expression> applicableConditions = findApplicableConditions(leftSet, rightSet);

          // Compute join size and cost
          int joinSize =
              estimateJoinSize(
                  leftSize, rightSize, leftVValues, rightVValues, applicableConditions);

          int totalCost =
              dpCost.get(leftSet)
                  + dpCost.get(rightSet)
                  + (size == n ? 0 : joinSize); // Avoid adding join size for the final join

          // Update the best plan if this is better
          if (totalCost < bestCost) {
            bestCost = totalCost;

            // Combine orders
            bestOrder = new ArrayList<>(dpOrder.get(leftSet));
            bestOrder.addAll(dpOrder.get(rightSet));

            // Combine V-values and conditions
            bestVValues = combineVValues(leftVValues, rightVValues, applicableConditions, joinSize);

            bestConditions = new ArrayList<>(dpConditions.get(leftSet));
            bestConditions.addAll(dpConditions.get(rightSet));
            bestConditions.addAll(applicableConditions);

            bestSize = joinSize;
          }
        }

        // Save the best plan for this subset
        dpCost.put(set, bestCost);
        dpOrder.put(set, bestOrder);
        dpSize.put(set, bestSize);
        dpVValues.put(set, bestVValues);
        dpConditions.put(set, bestConditions);
      }
    }
  }

  /** Generates all subsets of a given size from a set of n elements. */
  private Set<BitSet> generateSubsets(int n, int size) {
    Set<BitSet> subsets = new HashSet<>();
    BitSet set = new BitSet(n);
    generateSubsetsHelper(0, size, n, set, subsets);
    return subsets;
  }

  /** Helper method to generate subsets recursively. */
  private void generateSubsetsHelper(
      int start, int remainingSize, int n, BitSet current, Set<BitSet> subsets) {
    if (remainingSize == 0) {
      subsets.add((BitSet) current.clone());
      return;
    }

    for (int i = start; i < n; i++) {
      current.set(i);
      generateSubsetsHelper(i + 1, remainingSize - 1, n, current, subsets);
      current.clear(i);
    }
  }

  /** Generates all possible splits of a subset into two disjoint subsets. */
  private Set<BitSet> generateAllSplits(BitSet set) {
    Set<BitSet> splits = new HashSet<>();
    BitSet split = new BitSet(set.length());
    generateSplitsHelper(0, set, split, splits);
    return splits;
  }

  /** Helper method to generate splits recursively. */
  private void generateSplitsHelper(
      int position, BitSet original, BitSet current, Set<BitSet> splits) {
    if (position >= original.length()) {
      if (!current.isEmpty() && current.cardinality() < original.cardinality()) {
        splits.add((BitSet) current.clone());
      }
      return;
    }

    if (!original.get(position)) {
      generateSplitsHelper(position + 1, original, current, splits);
      return;
    }

    // Don't include this bit
    generateSplitsHelper(position + 1, original, current, splits);

    // Include this bit
    current.set(position);
    generateSplitsHelper(position + 1, original, current, splits);
    current.clear(position);
  }

  /** Finds applicable join conditions for two disjoint subsets. */
  private List<Expression> findApplicableConditions(BitSet leftSet, BitSet rightSet) {
    List<Expression> applicable = new ArrayList<>();
    for (Expression condition : joinConditions) {
      if (isConditionApplicable(condition, leftSet, rightSet)) {
        applicable.add(condition);
      }
    }
    return applicable;
  }

  /** Checks if a condition is applicable to two disjoint subsets. */
  private boolean isConditionApplicable(Expression condition, BitSet leftSet, BitSet rightSet) {
    Set<String> conditionTables = getTablesInCondition(condition);

    boolean usesLeft = false;
    boolean usesRight = false;
    boolean usesOther = false;

    for (String table : conditionTables) {
      int index = getTableIndex(table);
      if (leftSet.get(index)) usesLeft = true;
      else if (rightSet.get(index)) usesRight = true;
      else usesOther = true;
    }

    return usesLeft && usesRight && !usesOther;
  }

  /** Estimates the size of the result of a join. */
  private int estimateJoinSize(
      int leftSize,
      int rightSize,
      Map<String, Integer> leftVValues,
      Map<String, Integer> rightVValues,
      List<Expression> conditions) {
    double joinSize = leftSize * rightSize;

    for (Expression condition : conditions) {
      if (isEquiJoinCondition(condition)) {
        Column leftCol = getLeftColumn(condition);
        Column rightCol = getRightColumn(condition);

        String leftKey = leftCol.getFullyQualifiedName();
        String rightKey = rightCol.getFullyQualifiedName();

        Integer leftV = leftVValues.getOrDefault(leftKey, 100);
        Integer rightV = rightVValues.getOrDefault(rightKey, 100);

        joinSize /= Math.max(leftV, rightV);
      }
    }

    return Math.max(1, (int) joinSize);
  }

  /** Computes the V-values for a base operator. */
  private Map<String, Integer> computeBaseVValues(Operator op) {
    Map<String, Integer> vValues = new HashMap<>();
    String tableName = getTableName(op);

    for (Column col : op.getOutputSchema()) {
      String columnName = col.getColumnName();
      DBCatalog.ColumnStats stats = dbCatalog.getColumnStats(tableName, columnName);

      if (stats != null) {
        int distinctValues = Math.abs(stats.maxValue - stats.minValue) + 1;
        vValues.put(col.getFullyQualifiedName(), distinctValues);
        vValues.put(columnName, distinctValues);
      } else {
        vValues.put(col.getFullyQualifiedName(), 100);
        vValues.put(columnName, 100);
      }
    }

    return vValues;
  }

  /** Combines the V-values of two subsets after a join. */
  private Map<String, Integer> combineVValues(
      Map<String, Integer> leftVValues,
      Map<String, Integer> rightVValues,
      List<Expression> conditions,
      int joinSize) {
    Map<String, Integer> combined = new HashMap<>();

    combined.putAll(leftVValues);
    combined.putAll(rightVValues);

    for (Expression condition : conditions) {
      if (isEquiJoinCondition(condition)) {
        Column leftCol = getLeftColumn(condition);
        Column rightCol = getRightColumn(condition);
        String leftKey = leftCol.getFullyQualifiedName();
        String rightKey = rightCol.getFullyQualifiedName();

        int leftV = leftVValues.getOrDefault(leftKey, 100);
        int rightV = rightVValues.getOrDefault(rightKey, 100);

        int minV = Math.min(leftV, rightV);
        minV = Math.min(minV, joinSize);

        combined.put(leftKey, minV);
        combined.put(rightKey, minV);
      }
    }

    for (Map.Entry<String, Integer> entry : combined.entrySet()) {
      entry.setValue(Math.min(entry.getValue(), joinSize));
    }

    return combined;
  }

  /** Estimates the size of a base operator. */
  private int estimateBaseSize(Operator op) {
    if (op instanceof ScanOperator) {
      String tableName = getTableName(op);
      return dbCatalog.getTableTupleCount(tableName);
    }
    return 1000;
  }

  /** Gets the table name associated with an operator. */
  private String getTableName(Operator op) {
    return op.getOutputSchema().get(0).getTable().getName();
  }

  /** Gets the index of a table in the list of base operators. */
  private int getTableIndex(String tableName) {
    for (int i = 0; i < baseOperators.size(); i++) {
      if (getTableName(baseOperators.get(i)).equals(tableName)) {
        return i;
      }
    }
    return -1;
  }

  /** Gets the set of table names involved in a join condition. */
  private Set<String> getTablesInCondition(Expression condition) {
    Set<String> tables = new HashSet<>();
    if (condition instanceof EqualsTo) {
      EqualsTo equals = (EqualsTo) condition;

      if (equals.getLeftExpression() instanceof Column) {
        Column col = (Column) equals.getLeftExpression();
        tables.add(col.getTable().getName());
      }

      if (equals.getRightExpression() instanceof Column) {
        Column col = (Column) equals.getRightExpression();
        tables.add(col.getTable().getName());
      }
    } else if (condition instanceof ComparisonOperator) {
      ComparisonOperator comp = (ComparisonOperator) condition;

      if (comp.getLeftExpression() instanceof Column) {
        Column col = (Column) comp.getLeftExpression();
        tables.add(col.getTable().getName());
      }

      if (comp.getRightExpression() instanceof Column) {
        Column col = (Column) comp.getRightExpression();
        tables.add(col.getTable().getName());
      }
    }
    return tables;
  }

  /** Checks if a condition is an equality join condition. */
  private boolean isEquiJoinCondition(Expression condition) {
    if (!(condition instanceof EqualsTo)) {
      return false;
    }

    EqualsTo equals = (EqualsTo) condition;
    return equals.getLeftExpression() instanceof Column
        && equals.getRightExpression() instanceof Column;
  }

  /** Gets the left column from a binary condition. */
  private Column getLeftColumn(Expression condition) {
    if (condition instanceof BinaryExpression) {
      Expression left = ((BinaryExpression) condition).getLeftExpression();
      if (left instanceof Column) {
        return (Column) left;
      }
    }
    throw new IllegalArgumentException("Not a valid join condition");
  }

  /** Gets the right column from a binary condition. */
  private Column getRightColumn(Expression condition) {
    if (condition instanceof BinaryExpression) {
      Expression right = ((BinaryExpression) condition).getRightExpression();
      if (right instanceof Column) {
        return (Column) right;
      }
    }
    throw new IllegalArgumentException("Not a valid join condition");
  }

  /** Gets the optimal join order as a list of indices. */
  public List<Integer> getOptimalOrder() {
    BitSet allTables = new BitSet(baseOperators.size());
    allTables.set(0, baseOperators.size());
    return dpOrder.get(allTables);
  }

  /** Gets the join conditions for the optimal join order. */
  public List<Expression> getJoinConditions() {
    BitSet allTables = new BitSet(baseOperators.size());
    allTables.set(0, baseOperators.size());
    return dpConditions.get(allTables);
  }
}
