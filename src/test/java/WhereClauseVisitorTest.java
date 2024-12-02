
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import operator.logical.WhereClauseVisitor;
import operator.logical.UnionFind;

class WhereClauseVisitorTest {

    private WhereClauseVisitor visitor;

    @BeforeEach
    void setUp() {
        visitor = new WhereClauseVisitor();
    }

    @Test
    void testVisitEquality() {
        // Test union of two attributes
        visitor.visitEquality("attr1", "attr2");
        UnionFind.UnionElement elt1 = visitor.getUnionFind().find("attr1");
        UnionFind.UnionElement elt2 = visitor.getUnionFind().find("attr2");
        assertSame(elt1, elt2, "attr1 and attr2 should be in the same union.");
    }

    @Test
    void testVisitBoundUpperBound() {
        // Test upper bound constraint
        visitor.visitBound("attr1", "<", 10.0);
        UnionFind.UnionElement elt = visitor.getUnionFind().find("attr1");
        assertEquals(9.0, elt.getUpperBound(), "Upper bound for attr1 should be set to 9.0.");
    }

    @Test
    void testVisitBoundLowerBound() {
        // Test lower bound constraint
        visitor.visitBound("attr1", ">", 5.0);
        UnionFind.UnionElement elt = visitor.getUnionFind().find("attr1");
        assertEquals(6.0, elt.getLowerBound(), "Lower bound for attr1 should be set to 6.0.");
    }

    @Test
    void testVisitBoundEquality() {
        // Test equality constraint
        visitor.visitBound("attr1", "=", 15.0);
        UnionFind.UnionElement elt = visitor.getUnionFind().find("attr1");
        assertEquals(15.0, elt.getEqualityConstraint(), "Equality constraint for attr1 should be set to 15.0.");
    }

    @Test
    void testVisitBoundLessThanOrEqual() {
        // Test "<=" operator
        visitor.visitBound("attr1", "<=", 20.0);
        UnionFind.UnionElement elt = visitor.getUnionFind().find("attr1");
        assertEquals(20.0, elt.getUpperBound(), "Upper bound for attr1 should be set to 20.0.");
    }

    @Test
    void testVisitBoundGreaterThanOrEqual() {
        // Test ">=" operator
        visitor.visitBound("attr1", ">=", 10.0);
        UnionFind.UnionElement elt = visitor.getUnionFind().find("attr1");
        assertEquals(10.0, elt.getLowerBound(), "Lower bound for attr1 should be set to 10.0.");
    }
}
