README

# Top Level Class
The top-level class of the code is Compiler.java.

# Selection Pushing
The selection pushing logic is implemented in the LogicalPlanBuilder class. The buildLocalCondition method is responsible for analyzing the UnionFind and generating the selection conditions that can be pushed down to each base table. This is done by iterating through the columns in the schema and extracting the equality constraints, lower bounds, and upper bounds from the UnionFind elements. The generated selection conditions are then combined and added to the LogicalSelectOperator for each base table.

# Choice of Implementation for Logical Selection Operator
The LogicalSelectOperator is used to represent a selection operation in the logical plan. It takes the child operator and the selection condition as inputs. The LogicalPlanBuilder class is responsible for creating this operator.

# Choice of Join Order
The join order optimization is implemented in the JoinOrderOptimizer class. This class uses a dynamic programming approach to compute the optimal join order. It considers all possible ways to split the input set of relations and computes the cost of each plan. The plan with the lowest cost is chosen as the optimal join order.
The JoinOrderOptimizer class uses several helper methods to generate subsets, find applicable join conditions, estimate join sizes, and combine V-values. The computed optimal join order and join conditions are then used to create the LogicalJoinOperator in the LogicalPlanBuilder class.
Choice of Implementation for Join Operator
The physical JoinOperator is used to represent a join operation. It takes the left and right child operators, the join condition, and the table alias map as inputs. The PhysicalPlanBuilder class is responsible for creating this operator based on the logical plan.
The JoinOperator class implements the actual join logic, iterating through the tuples from the left and right child operators and evaluating the join condition to produce the final joined tuples.