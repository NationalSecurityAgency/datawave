package datawave.test.framework.generators.query.term;

import static datawave.test.framework.util.MetadataColumn.E;
import static datawave.test.framework.util.MetadataColumn.I;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.List;

import org.junit.jupiter.api.Test;

class IsNotNullTermTest extends AbstractQueryTermTest {

    @Test
    void testQueryAndPlan() {
        List<Evaluation> evaluations = apply(new IsNotNullTerm(), field(List.of("a"), List.of(1, 2), I, E));

        assertEquals(1, evaluations.size());
        assertEquals("filter:isNotNull(FIELD)", evaluations.get(0).getQuery());
        assertEquals("!(FIELD == null)", evaluations.get(0).getPlan());
    }

    @Test
    void testAppliedOncePerFieldRegardlessOfValueCount() {
        List<Evaluation> evaluations = apply(new IsNotNullTerm(), field(List.of("a", "b", "c", "d"), List.of(1, 2, 3, 4), I, E));

        assertEquals(1, evaluations.size(), "four values still produce a single evaluation");
    }

    @Test
    void testIdsAreTheFieldsFullPresenceSet() {
        List<Evaluation> evaluations = apply(new IsNotNullTerm(), field(List.of("a", "b"), List.of(2, 4, 6), I, E));

        assertEquals(List.of(2, 4, 6), evaluations.get(0).getIds());
    }

    @Test
    void testIsNotNegated() {
        assertFalse(new IsNotNullTerm().isNegated());
    }
}
