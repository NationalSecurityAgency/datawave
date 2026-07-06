package datawave.test.framework.generators.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import datawave.data.type.LcNoDiacriticsType;
import datawave.test.framework.FieldMetadata;
import datawave.test.framework.generators.query.term.EqTerm;
import datawave.test.framework.generators.query.term.IsNotNullTerm;
import datawave.test.framework.generators.query.term.IsNullTerm;
import datawave.test.framework.generators.query.term.NeTerm;
import datawave.test.framework.util.MetadataColumn;

class IntersectionFactoryTest extends BaseQueryMetadataFactoryTest {

    private IntersectionFactory factory;

    @Override
    IntersectionFactory getQueryMetadataFactory() {
        return factory;
    }

    /**
     * Parity with the previous {@code IndexedTermAndIsNotNull} behavior: an indexed EQ term intersected with an event-only isNotNull term, pulling from two
     * independent field lists.
     */
    @Test
    @Override
    void testIdealConditions() {
        FieldMetadata indexed = createIndexed();
        indexed.setEventIds(List.of(1, 2));
        indexed.setValues(List.of("a"));
        indexed.setNormalizers(List.of(new LcNoDiacriticsType()));

        FieldMetadata eventOnly = createEventOnly();
        eventOnly.setEventIds(List.of(1, 2));
        eventOnly.setValues(List.of("b"));
        eventOnly.setNormalizers(List.of(new LcNoDiacriticsType()));

        factory = new IntersectionFactory(new EqTerm(), new IsNotNullTerm());
        factory.setLeftMetadata(List.of(indexed));
        factory.setRightMetadata(List.of(eventOnly));

        List<QueryMetadata> expected = new ArrayList<>();
        expected.add(QueryMetadata.of("INDEXED == 'a' && filter:isNotNull(EVENT_ONLY)", "INDEXED == 'a' && !(EVENT_ONLY == null)", List.of(1, 2)));

        executeTest(expected);
    }

    /**
     * When both sides share the same field list, self-pairing and symmetric duplicates must be skipped.
     */
    @Test
    void testSelfComposedSkipsDiagonal() {
        FieldMetadata first = createIndexed();
        first.setEventIds(List.of(1, 2));
        first.setValues(List.of("a"));
        first.setNormalizers(List.of(new LcNoDiacriticsType()));

        FieldMetadata second = new FieldMetadata("INDEXED_2");
        second.setMetadataColumns(List.of(MetadataColumn.I, MetadataColumn.E));
        second.setEventIds(List.of(1, 2));
        second.setValues(List.of("z"));
        second.setNormalizers(List.of(new LcNoDiacriticsType()));

        List<FieldMetadata> shared = List.of(first, second);

        factory = new IntersectionFactory(new EqTerm(), new EqTerm());
        factory.setLeftMetadata(shared);
        factory.setRightMetadata(shared);

        List<QueryMetadata> results = factory.getQueries();
        assertEquals(1, results.size());
        assertEquals("INDEXED == 'a' && INDEXED_2 == 'z'", results.get(0).getQuery());
    }

    /**
     * A single negated term should have its ids complemented against the field's full event set before intersecting.
     */
    @Test
    void testSingleNegationComplementsIds() {
        // a single value so the left (EQ) side only produces one combination
        FieldMetadata left = createIndexed();
        left.setEventIds(List.of(1, 2, 3));
        left.setValues(List.of("a"));
        left.setNormalizers(List.of(new LcNoDiacriticsType()));

        // two values so the right (NE) side's complement is not trivially empty
        FieldMetadata right = new FieldMetadata("FIELD_B");
        right.setMetadataColumns(List.of(MetadataColumn.I, MetadataColumn.E));
        right.setEventIds(List.of(1, 2, 3, 4));
        right.setValues(List.of("x", "y"));
        right.setNormalizers(List.of(new LcNoDiacriticsType()));

        factory = new IntersectionFactory(new EqTerm(), new NeTerm());
        factory.setLeftMetadata(List.of(left));
        factory.setRightMetadata(List.of(right));

        List<QueryMetadata> results = factory.getQueries();
        assertEquals(2, results.size());

        QueryMetadata neY = results.stream().filter(m -> m.getQuery().equals("INDEXED == 'a' && !(FIELD_B == 'y')")).findFirst().orElseThrow();
        // FIELD_B == 'y' matches ids [2, 4]; the complement against FIELD_B's full event set [1,2,3,4] is [1, 3],
        // intersected with the left side's ids [1, 2, 3] leaves [1, 3]
        assertEquals(List.of(1, 3), sorted(neY.getIds()));
    }

    /**
     * FIELD_A present && isNull(FIELD_B) should match events present in FIELD_A but absent from FIELD_B: fieldAIds.removeAll(fieldBIds)
     */
    @Test
    void testIsNullSubtractsFieldPresence() {
        FieldMetadata fieldA = createIndexed();
        fieldA.setEventIds(List.of(1, 2, 3, 4));
        fieldA.setValues(List.of("a"));
        fieldA.setNormalizers(List.of(new LcNoDiacriticsType()));

        FieldMetadata fieldB = createEventOnly();
        fieldB.setEventIds(List.of(2, 4));
        fieldB.setValues(List.of("b"));
        fieldB.setNormalizers(List.of(new LcNoDiacriticsType()));

        factory = new IntersectionFactory(new EqTerm(), new IsNullTerm());
        factory.setLeftMetadata(List.of(fieldA));
        factory.setRightMetadata(List.of(fieldB));

        List<QueryMetadata> results = factory.getQueries();
        assertEquals(1, results.size());
        assertEquals("INDEXED == 'a' && filter:isNull(EVENT_ONLY)", results.get(0).getQuery());
        assertEquals("INDEXED == 'a' && EVENT_ONLY == null", results.get(0).getPlan());
        assertEquals(List.of(1, 3), sorted(results.get(0).getIds()));
    }

    @Test
    void testTwoNegatedTermsRejected() {
        factory = new IntersectionFactory(new NeTerm(), new NeTerm());
        factory.setLeftMetadata(List.of(createIndexed()));
        factory.setRightMetadata(List.of(createEventOnly()));
        assertThrows(IllegalStateException.class, () -> factory.getQueries());
    }

    private List<Integer> sorted(List<Integer> ids) {
        List<Integer> copy = new ArrayList<>(ids);
        copy.sort(Integer::compareTo);
        return copy;
    }
}
