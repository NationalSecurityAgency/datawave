package datawave.test.framework.generators.query.term;

import static datawave.test.framework.util.MetadataColumn.E;
import static datawave.test.framework.util.MetadataColumn.I;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

class NeTermTest extends AbstractQueryTermTest {

    @Test
    void testQueryAndPlanAreWrappedInANegation() {
        List<Evaluation> evaluations = apply(new NeTerm(), field(List.of("Alpha"), List.of(1, 2), I, E));

        assertEquals(1, evaluations.size());
        assertEquals("!(FIELD == 'Alpha')", evaluations.get(0).getQuery());
        assertEquals("!(FIELD == 'alpha')", evaluations.get(0).getPlan());
    }

    @Test
    void testIsNegated() {
        assertTrue(new NeTerm().isNegated());
    }

    @Test
    void testIdsAreTheMatchingEventsNotTheComplement() {
        List<Evaluation> evaluations = apply(new NeTerm(), field(List.of("a", "b"), List.of(1, 2, 3, 4), I, E));

        assertEquals(2, evaluations.size());
        assertEquals("!(FIELD == 'a')", evaluations.get(0).getQuery());
        assertEquals(List.of(1, 3), evaluations.get(0).getIds(), "the ids match the value, leaving the complement to the combining factory");
        assertEquals(List.of(2, 4), evaluations.get(1).getIds());
    }
}
