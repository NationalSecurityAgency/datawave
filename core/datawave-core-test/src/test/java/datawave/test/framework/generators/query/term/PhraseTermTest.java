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
import datawave.data.type.NoOpType;
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
    void testMatchesNormalizedPhrasesWithinOtherValues() {
        FieldMetadata field = field(List.of("alpha beta", "Alpha Béta gamma", "zero alpha beta end", "alpha x beta", "beta alpha", "ALPHA BETA"),
                        List.of(1, 2, 3, 4, 5, 6), I, TF);
        List<Evaluation> evaluations = apply(new PhraseTerm(), field);

        assertEquals(List.of(1, 2, 3, 6), evaluations.get(0).getIds());
        assertEquals(List.of(1, 2, 3, 6), evaluations.get(1).getIds());
        assertEquals(List.of(1, 2, 3, 6), evaluations.get(5).getIds());
    }

    @Test
    void testValueWithoutEventsCanGenerateAMatchingPhrase() {
        FieldMetadata field = field(List.of("alpha beta gamma", "alpha beta"), List.of(9), I, TF);
        List<Evaluation> evaluations = apply(new PhraseTerm(), field);

        assertEquals(List.of(9), evaluations.get(1).getIds());
    }

    @Test
    void testRepeatedWordsRequireAdjacentPositionsAndDoNotDuplicateIds() {
        FieldMetadata field = field(List.of("go go", "go go go", "go stop go", "stop go go go"), List.of(1, 2, 3, 4, 5), I, TF);
        List<Evaluation> evaluations = apply(new PhraseTerm(), field);

        assertEquals(List.of(1, 2, 4, 5), evaluations.get(0).getIds());
        assertEquals(List.of(1, 2, 4, 5), evaluations.get(1).getIds());
    }

    @Test
    void testMatchingUsesTheConfiguredWordCount() {
        FieldMetadata field = field(List.of("alpha beta gamma delta", "alpha beta other gamma", "zero alpha beta gamma", "alpha beta gamma"),
                        List.of(1, 2, 3, 4), I, TF);
        List<Evaluation> evaluations = apply(new PhraseTerm(3), field);

        assertEquals(List.of(1, 3, 4), evaluations.get(0).getIds());
        assertEquals(List.of(1, 3, 4), evaluations.get(3).getIds());
    }

    @Test
    void testMatchingUsesTheConfiguredNormalizer() {
        FieldMetadata field = field(List.of("Alpha Beta", "alpha beta", "zero Alpha Beta"), List.of(1, 2, 3), List.of(new NoOpType()), I, TF);
        List<Evaluation> evaluations = apply(new PhraseTerm(), field);

        assertEquals(List.of(1, 3), evaluations.get(0).getIds());
        assertEquals(List.of(2), evaluations.get(1).getIds());
    }

    @Test
    void testNonContentFieldsAreRejected() {
        FieldMetadata indexed = field(List.of("alpha beta"), List.of(1), I);
        FieldMetadata eventOnly = field(List.of("alpha beta"), List.of(1), E);
        FieldMetadata indexedEvent = field(List.of("alpha beta"), List.of(1), I, E);

        assertThrows(IllegalStateException.class, () -> apply(new PhraseTerm(), indexed));
        assertThrows(IllegalStateException.class, () -> apply(new PhraseTerm(), eventOnly));
        assertThrows(IllegalStateException.class, () -> apply(new PhraseTerm(), indexedEvent));
    }

    @Test
    void testReusingTermReflectsChangedValuesAndEventIds() {
        PhraseTerm term = new PhraseTerm();
        FieldMetadata field = field(List.of("alpha beta", "alpha beta gamma"), List.of(1, 2, 3), I, TF);
        List<Integer> originalIds = apply(term, field).get(0).getIds();
        assertEquals(List.of(1, 2, 3), originalIds);

        field.setValues(List.of("alpha beta", "gamma delta"));
        assertEquals(List.of(1, 3), apply(term, field).get(0).getIds());

        field.setEventIds(List.of(7, 8));
        assertEquals(List.of(7), apply(term, field).get(0).getIds());
        assertEquals(List.of(1, 2, 3), originalIds);
        assertThrows(UnsupportedOperationException.class, () -> originalIds.add(99));
    }

    @Test
    void testReusingTermReflectsChangedNormalizerAndField() {
        PhraseTerm term = new PhraseTerm();
        FieldMetadata field = field(List.of("alpha beta", "ALPHA BETA"), List.of(1, 2), I, TF);
        assertEquals(List.of(1, 2), apply(term, field).get(0).getIds());

        field.setNormalizers(List.of(new NoOpType()));
        assertEquals(List.of(1), apply(term, field).get(0).getIds());

        FieldMetadata other = field(List.of("alpha beta", "alpha beta gamma"), List.of(11, 12), E, TF);
        assertEquals(List.of(11, 12), apply(term, other).get(0).getIds());
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
