#TOP LEVEL CLASS
Compiler.Java

#JOIN CONDITION LOGIC
The entire WHERE clause is stored in the whereExpression, but the code does not differentiate between join and filter conditions. The SelectOperator applies the entire WHERE clause (both filter and join conditions) after the joins have already been created without conditions, which results in filtering the tuples after performing a Cartesian product of the joined tables.