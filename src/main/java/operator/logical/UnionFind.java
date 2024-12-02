package operator.logical;

import java.util.Map;
import java.util.HashMap;

public class UnionFind {

  private Map<String, UnionElement> elements;

  public UnionFind() {
    elements = new HashMap<>();
  }

  // emplopys path compression --
  // https://www.geeksforgeeks.org/union-by-rank-and-path-compression-in-union-find-algorithm/
  public UnionElement findSet(UnionElement element) {
    if (element.parent != element) {
      element.parent = findSet(element.parent);
    }
    return element.parent;
  }

  // for a given attribute -- find or create an element
  public UnionElement find(String attribute) {
    if (!elements.containsKey(attribute)) {
      elements.put(attribute, new UnionElement(attribute));
    }
    return findSet(elements.get(attribute));
  }

  // given two elements -- union them
  public void union(UnionElement elt1, UnionElement elt2) {
    UnionElement root1 = findSet(elt1);
    UnionElement root2 = findSet(elt2);

    // potentially have to account for merging constraints
    mergeConstraints(root1, root2);

    if (root1 == root2) {
      return;
    }

    if (root1.rank < root2.rank) {
      root1.parent = root2;
    } else if (root1.rank > root2.rank) {
      root2.parent = root1;
    } else {
      root2.parent = root1;
      root1.rank++;
    }
  }

  private void mergeConstraints(UnionElement root1, UnionElement root2) {
    // equality constraints
    if (root1.equalityConstraint != null && root2.equalityConstraint != null) {
      if (!root1.equalityConstraint.equals(root2.equalityConstraint)) {
        throw new IllegalStateException("Conflicting equality constraints");
      }
    } else if (root1.equalityConstraint != null) {
      setEqualityConstraint(root2, root1.equalityConstraint);
    } else if (root2.equalityConstraint != null) {
      setEqualityConstraint(root1, root2.equalityConstraint);
    }

    // merge bounds
    Double newLower = null;
    if (root1.lowerBound != null && root2.lowerBound != null) {
      newLower = Math.max(root1.lowerBound, root2.lowerBound);
    } else {
      newLower = root1.lowerBound != null ? root1.lowerBound : root2.lowerBound;
    }

    Double newUpper = null;
    if (root1.upperBound != null && root2.upperBound != null) {
      newUpper = Math.min(root1.upperBound, root2.upperBound);
    } else {
      newUpper = root1.upperBound != null ? root1.upperBound : root2.upperBound;
    }

    // vaalidate
    if (newLower != null && newUpper != null && newLower > newUpper) {
      throw new IllegalStateException("Inconsistent bounds after union");
    }

    root1.lowerBound = newLower;
    root1.upperBound = newUpper;
    root2.lowerBound = newLower;
    root2.upperBound = newUpper;
  }

  // given a union-find element, set its lower bound, upper bound, or
  // equality constraint to a particular value.

  public void setLowerBound(UnionElement element, double value) {
    UnionElement root = findSet(element);
    if (root.equalityConstraint != null && value > root.equalityConstraint) {
      throw new IllegalStateException("Lower bound conflicts with equality constraint");
    }
    if (root.upperBound != null && value > root.upperBound) {
      throw new IllegalStateException("Lower bound > upper bound");
    }
    root.lowerBound = value;
  }

  public void setUpperBound(UnionElement element, double value) {
    UnionElement root = findSet(element);
    if (root.equalityConstraint != null && value < root.equalityConstraint) {
      throw new IllegalStateException("Upper bound conflicts with equality constraint");
    }
    if (root.lowerBound != null && value < root.lowerBound) {
      throw new IllegalStateException("Upper bound < lower bound");
    }
    root.upperBound = value;
  }

  public void setEqualityConstraint(UnionElement element, double value) {
    UnionElement root = findSet(element);
    if (root.lowerBound != null && value < root.lowerBound) {
      throw new IllegalStateException("Equality constraint conflicts with lower bound");
    }
    if (root.upperBound != null && value > root.upperBound) {
      throw new IllegalStateException("Equality constraint conflicts with upper bound");
    }
    root.equalityConstraint = value;
    root.lowerBound = value;
    root.upperBound = value;
  }

  // class that keeps track of each element -- set of attributes and 3 constants
  public class UnionElement {
    private UnionElement parent;
    private int rank;
    private Double lowerBound;
    private Double upperBound;
    private Double equalityConstraint;
    private String attribute;

    public UnionElement(String attribute) {
      this.parent = this;
      this.rank = 0;
      this.attribute = attribute;
      this.lowerBound = null;
      this.upperBound = null;
      this.equalityConstraint = null;
    }

  
    public Double getLowerBound() {
      return lowerBound;
    }
    public Double getUpperBound() {
      return upperBound;
    }
    public Double getEqualityConstraint() {
      return equalityConstraint;
    }
  }
  public static void main(String[] args) {
    UnionFind uf = new UnionFind();
  
    UnionFind.UnionElement elt1 = uf.find("A");
    UnionFind.UnionElement elt2 = uf.find("B");
    UnionFind.UnionElement elt3 = uf.find("C");
  
    uf.union(elt1, elt2);
    assert uf.find("A") == uf.find("B");
  
    uf.setLowerBound(elt1, 10);
    uf.setUpperBound(elt2, 20);
    assert elt1.lowerBound == 10 && elt1.upperBound == 20;
  
    try {
      uf.setEqualityConstraint(elt3, 15);
      uf.setLowerBound(elt3, 20); // Should throw an exception
    } catch (IllegalStateException e) {
      System.out.println("Caught expected conflict: " + e.getMessage());
    }
  }
 
  
}
