package datawave.test.framework.generators.query.term;

import static datawave.test.framework.util.MetadataColumn.E;
import static datawave.test.framework.util.MetadataColumn.I;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.Test;

import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NumberType;
import datawave.data.type.Type;
import datawave.test.framework.FieldMetadata;

class EqTermTest extends AbstractQueryTermTest {

    @Test
    void testQueryAndPlanForASingleValue() {
        List<Evaluation> evaluations = apply(new EqTerm(), field(List.of("a"), List.of(1, 2), I, E));

        assertEquals(1, evaluations.size());
        assertEquals("FIELD == 'a'", evaluations.get(0).getQuery());
        assertEquals("FIELD == 'a'", evaluations.get(0).getPlan());
        assertEquals(List.of(1, 2), evaluations.get(0).getIds());
    }

    /**
     * Query text keeps the original value; plan text uses the normalized value.
     */
    @Test
    void testPlanCarriesTheNormalizedValue() {
        List<Evaluation> evaluations = apply(new EqTerm(), field(List.of("Alpha"), List.of(1), I, E));

        assertEquals("FIELD == 'Alpha'", evaluations.get(0).getQuery());
        assertEquals("FIELD == 'alpha'", evaluations.get(0).getPlan());
    }

    @Test
    void testAppliedOncePerValueWithThatValuesEvents() {
        List<Evaluation> evaluations = apply(new EqTerm(), field(List.of("a", "b"), List.of(1, 2, 3, 4), I, E));

        assertEquals(2, evaluations.size());
        assertEquals("FIELD == 'a'", evaluations.get(0).getQuery());
        assertEquals(List.of(1, 3), evaluations.get(0).getIds());
        assertEquals("FIELD == 'b'", evaluations.get(1).getQuery());
        assertEquals(List.of(2, 4), evaluations.get(1).getIds());
    }

    @Test
    void testValueWithNoEventsYieldsAnEmptyIdSet() {
        List<Evaluation> evaluations = apply(new EqTerm(), field(List.of("a", "b"), List.of(1), I, E));

        assertEquals(2, evaluations.size());
        assertEquals(List.of(1), evaluations.get(0).getIds());
        assertEquals(List.of(), evaluations.get(1).getIds());
    }

    @Test
    void testMultipleNormalizersAreRejected() {
        List<Type<?>> two = List.of(new LcNoDiacriticsType(), new NumberType());
        FieldMetadata field = field(List.of("1"), List.of(1), two, I, E);

        assertThrows(RuntimeException.class, () -> apply(new EqTerm(), field));
    }

    @Test
    void testIsNotNegated() {
        assertFalse(new EqTerm().isNegated());
    }

    @Test
    void testNegationCanBeOverridden() {
        EqTerm term = new EqTerm();
        term.setIsNegated(true);
        assertEquals(true, term.isNegated());
    }
}
