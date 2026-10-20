package datawave.test.framework.generators.query.term;

import static datawave.test.framework.util.MetadataColumn.E;
import static datawave.test.framework.util.MetadataColumn.I;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

class IsNullTermTest extends AbstractQueryTermTest {

    @Test
    void testQueryAndPlan() {
        List<Evaluation> evaluations = apply(new IsNullTerm(), field(List.of("a"), List.of(1, 2), I, E));

        assertEquals(1, evaluations.size());
        assertEquals("filter:isNull(FIELD)", evaluations.get(0).getQuery());
        assertEquals("FIELD == null", evaluations.get(0).getPlan());
    }

    @Test
    void testAppliedOncePerFieldRegardlessOfValueCount() {
        List<Evaluation> evaluations = apply(new IsNullTerm(), field(List.of("a", "b", "c"), List.of(1, 2, 3), I, E));

        assertEquals(1, evaluations.size());
    }

    /**
     * The term stores present ids; combining logic subtracts them.
     */
    @Test
    void testIdsAreThePresenceSetAndTheTermIsNegated() {
        List<Evaluation> evaluations = apply(new IsNullTerm(), field(List.of("a"), List.of(2, 4), I, E));

        assertEquals(List.of(2, 4), evaluations.get(0).getIds());
        assertTrue(new IsNullTerm().isNegated(), "the ids are the presence set, so the term must be negated for a factory to subtract them");
    }

    @Test
    void testTracksTheSameIdsAsIsNotNull() {
        List<Evaluation> isNull = apply(new IsNullTerm(), field(List.of("a"), List.of(1, 3, 5), I, E));
        List<Evaluation> isNotNull = apply(new IsNotNullTerm(), field(List.of("a"), List.of(1, 3, 5), I, E));

        assertEquals(isNotNull.get(0).getIds(), isNull.get(0).getIds());
    }
}
