package operator.logical;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.express

    rt net.sf.jsqlparser.expressi
         net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
impor

    
         net.sf.jsqlparser.schema.Column;
        
        
impor

    ic class WhereClauseVisitor {
        ivate final UnionFind unionFind;
        ivate final List<Ex
            
                 WhereClauseVisitor() {
                is.uni
            this.resid
                
                
            
                cesses the WHERE clause and builds union
                
            
                 List<Expression> process(Expression
                 (wher
            
                st<Expression> conditions = extractCondition
                r (Exp
              if (condition instanceof ComparisonOperator) {
     

                residualConditions.ad
              }
     

     
        return residualConditions;
        


    /* *
     * Processes a comparison operato handling
        /
    p

        Expression left = comparisn.getLeftExpression();
        Expression right = comparison.getRightExpressi
        
     

 
          String attr = ((Column) left).getFullyQualifiedName();
            double value = extractNumericValue(right);
            visitBound(attr, comparison.getStringExpression(), value);
        } else {
            // Add to residual conditions if not in the expected format
            residualConditions.add(comparison);
        }
    }

    /**
     * Processes equality conditions
     */
    private void processEquality(EqualsTo equality) {
        Expression left = equality.getLeftExpression();
        Expression right = equality.getRightExpression();

        if (left instanceof Column && right instanceof Column) {
            visitEquality(
                ((Column) left).getFullyQualifiedName(),
                ((Column) right).getFullyQualifiedName()
            );
        } else if (left instanceof Column && isNumericValue(right)) {
            String attr = ((Column) left).getFullyQualifiedName();
            double value = extractNumericValue(right);
            visitBound(attr, "=", value);
        } else {
            // not supported, it is set aside 
            residualConditions.add(equality);
        }
    }
   
    /**
     * Extracts individual conditions from an AND expression.
     */
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
