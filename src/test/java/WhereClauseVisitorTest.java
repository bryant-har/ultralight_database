
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
        visitor.visitEquality("attr1", "attr2");
        UnionFind.UnionElement elt1 = visitor.getUnionFind().find("attr1");
        UnionFind.UnionElement elt2 = visitor.getUnionFind().find("attr2");
        assertSame(elt1, elt2, "attr1 and attr2 should be in the same union.");
    }

    @Test
    void testVisitBoundUpperBound() {
        visitor.visitBound("attr1", "<", 10.0);
        UnionFind.UnionElement elt = visitor.getUnionFind().find("attr1");
        assertEquals(9.0, elt.getUpperBound(), "Upper bound for attr1 should be set to 9.0.");
    }

    @Test
    void testVisitBoundLowerBound() {
        visitor.visitBound("attr1", ">", 5.0);
        UnionFind.UnionElement elt = visitor.getUnionFind().find("attr1");
        assertEquals(6.0, elt.getLowerBound(), "Lower bound for attr1 should be set to 6.0.");
    }

    @Test
    void testVisitBoundEquality() {
        visitor.visitBound("attr1", "=", 15.0);
        UnionFind.UnionElement elt = visitor.getUnionFind().find("attr1");
        assertEquals(15.0, elt.getEqualityConstraint(), "Equality constraint for attr1 should be set to 15.0.");
    }

    @Test
    void testVisitBoundLessThanOrEqual() {
        visitor.visitBound("attr1", "<=", 20.0);
        UnionFind.UnionElement elt = visitor.getUnionFind().find("attr1");
        assertEquals(20.0, elt.getUpperBound(), "Upper bound for attr1 should be set to 20.0.");
    }

    @Test
    void testVisitBoundGreaterThanOrEqual() {
        visitor.visitBound("attr1", ">=", 10.0);
        UnionFind.UnionElement elt = visitor.getUnionFind().find("attr1");
        assertEquals(10.0, elt.getLowerBound(), "Lower bound for attr1 should be set to 10.0.");
    }

    @Test
    void testComplexConstraints() {
        //  multiple constraints
        visitor.visitEquality("attr1", "attr2");
        visitor.visitBound("attr1", ">", 5.0);
        visitor.visitBound("attr2", "<", 10.0);
        visitor.visitBound("attr3", "=", 7.0);

        UnionFind.UnionElement elt1 = visitor.getUnionFind().find("attr1");
        UnionFind.UnionElement elt2 = visitor.getUnionFind().find("attr2");
        assertSame(elt1, elt2, "attr1 and attr2 should be in the same union.");

        //  bounds for attr1 (while implicity has attr2 because of the union)
        assertEquals(6.0, visitor.getLowerBound("attr1"), "Lower bound for attr1 should be 6.0.");
        assertEquals(9.0, visitor.getUpperBound("attr1"), "Upper bound for attr1 should be 9.0.");

        // Verify bounds for attr2 (should match attr1 since they are unioned)
        assertEquals(6.0, visitor.getLowerBound("attr2"), "Lower bound for attr2 should be 6.0.");
        assertEquals(9.0, visitor.getUpperBound("attr2"), "Upper bound for attr2 should be 9.0.");

        assertEquals(7.0, visitor.getEqualityConstraint("attr3"), "Equality constraint for attr3 should be 7.0.");

    }

    
}
