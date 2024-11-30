package operator.logical;

public class WhereClauseVisitor {
    private UnionFind unionFind;
  
    public WhereClauseVisitor() {
      this.unionFind = new UnionFind();
    }
  
    public void visitEquality(String attr1, String attr2) {
      UnionFind.UnionElement elt1 = unionFind.find(attr1);
      UnionFind.UnionElement elt2 = unionFind.find(attr2);
      unionFind.union(elt1, elt2);
    }
  
    public void visitBound(String attr, String operator, double value) {
      UnionFind.UnionElement elt = unionFind.find(attr);
      switch (operator) {
        case "<":
          unionFind.setUpperBound(elt, value - 1);
          break;
        case "<=":
          unionFind.setUpperBound(elt, value);
          break;
        case ">":
          unionFind.setLowerBound(elt, value + 1);
          break;
        case ">=":
          unionFind.setLowerBound(elt, value);
          break;
        case "=":
          unionFind.setEqualityConstraint(elt, value);
          break;
      }
    }
  
    public UnionFind getUnionFind() {
      return this.unionFind;
    }
  }
  