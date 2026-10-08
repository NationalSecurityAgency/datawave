package datawave.test.framework.generators.query.term;

import static datawave.test.framework.util.MetadataColumn.E;
import static datawave.test.framework.util.MetadataColumn.I;
import static datawave.test.framework.util.MetadataColumn.TF;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.Test;

import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NumberType;
import datawave.data.type.Type;
import datawave.test.framework.FieldMetadata;

class PhraseTermTest extends AbstractQueryTermTest {

    @Test
    void testQueryAndPlanForATwoWordPhrase() {
        List<Evaluation> evaluations = apply(new PhraseTerm(), field(List.of("Alpha Beta"), List.of(1, 2), I, TF));

        assertEquals(1, evaluations.size());
        assertEquals("content:phrase(FIELD, termOffsetMap, 'Alpha', 'Beta')", evaluations.get(0).getQuery());
        assertEquals("content:phrase(FIELD, termOffsetMap, 'alpha', 'beta') && FIELD == 'alpha' && FIELD == 'beta'", evaluations.get(0).getPlan());
        assertEquals(List.of(1, 2), evaluations.get(0).getIds());
    }

    @Test
    void testSelectsAPrefixOfThePhrase() {
        List<Evaluation> evaluations = apply(new PhraseTerm(3), field(List.of("alpha beta gamma delta"), List.of(1), I, TF));

        assertEquals("content:phrase(FIELD, termOffsetMap, 'alpha', 'beta', 'gamma')", evaluations.get(0).getQuery());
    }

    @Test
    void testWordCountBelowTwoIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new PhraseTerm(1));
        assertThrows(IllegalArgumentException.class, () -> new PhraseTerm(0));
    }

    @Test
    void testPhraseShorterThanTheWordCountIsRejected() {
        FieldMetadata field = field(List.of("alpha"), List.of(1), I, TF);
        assertThrows(IllegalStateException.class, () -> apply(new PhraseTerm(), field));
    }

    @Test
    void testMultipleNormalizersAreRejected() {
        List<Type<?>> two = List.of(new LcNoDiacriticsType(), new NumberType());
        FieldMetadata field = field(List.of("alpha beta"), List.of(1), two, I, TF);

        assertThrows(IllegalStateException.class, () -> apply(new PhraseTerm(), field));
    }

    @Test
    void testAppliedOncePerValue() {
        List<Evaluation> evaluations = apply(new PhraseTerm(), field(List.of("alpha beta", "gamma delta"), List.of(1, 2), I, TF));

        assertEquals(2, evaluations.size());
        assertEquals("content:phrase(FIELD, termOffsetMap, 'alpha', 'beta')", evaluations.get(0).getQuery());
        assertEquals(List.of(1), evaluations.get(0).getIds());
        assertEquals("content:phrase(FIELD, termOffsetMap, 'gamma', 'delta')", evaluations.get(1).getQuery());
        assertEquals(List.of(2), evaluations.get(1).getIds());
    }

    @Test
    void testIsNotNegated() {
        assertFalse(new PhraseTerm().isNegated());
    }

    @Test
    void testEventOnlyContentFieldStillBuildsATerm() {
        List<Evaluation> evaluations = apply(new PhraseTerm(), field(List.of("alpha beta"), List.of(1), E, TF));

        assertEquals("content:phrase(FIELD, termOffsetMap, 'alpha', 'beta')", evaluations.get(0).getQuery());
    }
}
