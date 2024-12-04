package operator.logical;

import java.util.HashMap;
import java.util.Map;

/**
 * The {@code UnionFind} class implements a union-find (disjoint-set) data structure for managing
 * attributes and their associated constraints. This structure is used for equality propagation and
 * constraint validation in query optimization scenarios.
 *
 * <p>Each attribute is represented as a {@link UnionElement}, which can store bounds (lower and
 * upper) or an equality constraint. The union-find mechanism enables efficient merging of
 * constraints and validation.
 *
 * <h2>Key Features</h2>
 *
 * <ul>
 *   <li>Union by rank and path compression for efficient operations.
 *   <li>Management of lower bounds, upper bounds, and equality constraints.
 *   <li>Validation of merged constraints during union operations.
 * </ul>
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * UnionFind uf = new UnionFind();
 *
 * // Find or create elements for attributes
 * UnionFind.UnionElement elt1 = uf.find("A");
 * UnionFind.UnionElement elt2 = uf.find("B");
 *
 * // Union two elements
 * uf.union(elt1, elt2);
 *
 * // Set bounds or equality constraints
 * uf.setLowerBound(elt1, 10);
 * uf.setUpperBound(elt2, 20);
 * uf.setEqualityConstraint(elt1, 15);
 * }</pre>
 *
 * @see UnionElement
 */
public class UnionFind {

  private Map<String, UnionElement> elements; // Map of attribute names to their UnionElements

  /** Constructs an empty {@code UnionFind} instance. */
  public UnionFind() {
    elements = new HashMap<>();
  }

  /**
   * Finds the representative element of the set containing the given {@code UnionElement}, with
   * path compression.
   *
   * @param element The {@code UnionElement} to find.
   * @return The representative of the set.
   */
  public UnionElement findSet(UnionElement element) {
    if (element.parent != element) {
      element.parent = findSet(element.parent);
    }
    return element.parent;
  }

  /**
   * Finds or creates a {@code UnionElement} for the specified attribute.
   *
   * @param attribute The attribute name.
   * @return The corresponding {@code UnionElement}.
   */
  public UnionElement find(String attribute) {
    if (!elements.containsKey(attribute)) {
      elements.put(attribute, new UnionElement(attribute));
    }
    return findSet(elements.get(attribute));
  }

  /**
   * Unions the sets containing two {@code UnionElement}s, merging their constraints.
   *
   * @param elt1 The first {@code UnionElement}.
   * @param elt2 The second {@code UnionElement}.
   */
  public void union(UnionElement elt1, UnionElement elt2) {
    UnionElement root1 = findSet(elt1);
    UnionElement root2 = findSet(elt2);

    mergeConstraints(root1, root2);

    if (root1 == root2) {
      return;
    }

    if (root1.rank < root2.rank) {
      root1.parent = root2;
      root2.rank++;
    } else {
      root2.parent = root1;
      root1.rank++;
    }
  }

  /**
   * Merges constraints between two root {@code UnionElement}s during a union operation.
   *
   * @param root1 The first root {@code UnionElement}.
   * @param root2 The second root {@code UnionElement}.
   * @throws IllegalStateException If constraints are inconsistent.
   */
  private void mergeConstraints(UnionElement root1, UnionElement root2) {
    // Merge equality constraints
    if (root1.equalityConstraint != null && root2.equalityConstraint != null) {
      if (!root1.equalityConstraint.equals(root2.equalityConstraint)) {
        throw new IllegalStateException("Conflicting equality constraints");
      }
    } else if (root1.equalityConstraint != null) {
      setEqualityConstraint(root2, root1.equalityConstraint);
    } else if (root2.equalityConstraint != null) {
      setEqualityConstraint(root1, root2.equalityConstraint);
    }

    // Merge bounds
    Double newLower = null;
    Double newUpper = null;
    if (root1.lowerBound != null && root2.lowerBound != null) {
      newLower = Math.max(root1.lowerBound, root2.lowerBound);
    } else {
      newLower = root1.lowerBound != null ? root1.lowerBound : root2.lowerBound;
    }

    if (root1.upperBound != null && root2.upperBound != null) {
      newUpper = Math.min(root1.upperBound, root2.upperBound);
    } else {
      newUpper = root1.upperBound != null ? root1.upperBound : root2.upperBound;
    }

    if (newLower != null && newUpper != null && newLower > newUpper) {
      throw new IllegalStateException("Inconsistent bounds after union");
    }

    root1.lowerBound = newLower;
    root1.upperBound = newUpper;
    root2.lowerBound = newLower;
    root2.upperBound = newUpper;
  }

  /**
   * Sets the lower bound for the given {@code UnionElement}.
   *
   * @param element The {@code UnionElement}.
   * @param value The lower bound value.
   * @throws IllegalStateException If the new bound conflicts with existing constraints.
   */
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

  /**
   * Sets the upper bound for the given {@code UnionElement}.
   *
   * @param element The {@code UnionElement}.
   * @param value The upper bound value.
   * @throws IllegalStateException If the new bound conflicts with existing constraints.
   */
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

  /**
   * Sets an equality constraint for the given {@code UnionElement}.
   *
   * @param element The {@code UnionElement}.
   * @param value The equality constraint value.
   * @throws IllegalStateException If the new constraint conflicts with existing bounds.
   */
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

  /**
   * The {@code UnionElement} class represents an element in the union-find structure. Each element
   * stores its parent, rank, bounds, equality constraint, and associated attribute.
   */
  public class UnionElement {
    private UnionElement parent; // Parent element for union-find
    private int rank; // Rank for union by rank
    private Double lowerBound; // Lower bound constraint
    private Double upperBound; // Upper bound constraint
    private Double equalityConstraint; // Equality constraint
    private String attribute; // Attribute name

    /**
     * Constructs a new {@code UnionElement} for the given attribute.
     *
     * @param attribute The attribute name.
     */
    public UnionElement(String attribute) {
      this.parent = this;
      this.rank = 0;
      this.attribute = attribute;
      this.lowerBound = null;
      this.upperBound = null;
      this.equalityConstraint = null;
    }

    /**
     * Returns the lower bound of the element.
     *
     * @return The lower bound, or {@code null} if not set.
     */
    public Double getLowerBound() {
      return lowerBound;
    }

    /**
     * Returns the upper bound of the element.
     *
     * @return The upper bound, or {@code null} if not set.
     */
    public Double getUpperBound() {
      return upperBound;
    }

    /**
     * Returns the equality constraint of the element.
     *
     * @return The equality constraint, or {@code null} if not set.
     */
    public Double getEqualityConstraint() {
      return equalityConstraint;
    }
  }

  /**
   * Main method for basic testing of the {@code UnionFind} class.
   *
   * @param args Command-line arguments (not used).
   */
  public static void main(String[] args) {
    UnionFind uf = new UnionFind();

    UnionFind.UnionElement elt1 = uf.find("A");
    UnionFind.UnionElement elt2 = uf.find("B");
    UnionFind.UnionElement elt3 = uf.find("C");

    uf.union(elt1, elt2);
    assert uf.find("A") == uf.find("B");

    uf.setLowerBound(elt1, 10);
    uf.setUpperBound(elt2, 20);
    assert elt1.getLowerBound() == 10 && elt1.getUpperBound() == 20;

    try {
      uf.setEqualityConstraint(elt3, 15);
      uf.setLowerBound(elt3, 20); // Should throw an exception
    } catch (IllegalStateException e) {
      System.out.println("Caught expected conflict: " + e.getMessage());
    }
  }
}
