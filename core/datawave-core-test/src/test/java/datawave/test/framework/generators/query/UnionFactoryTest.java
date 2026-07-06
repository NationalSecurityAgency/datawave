package datawave.test.framework.generators.query;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import datawave.data.type.LcNoDiacriticsType;
import datawave.test.framework.FieldMetadata;
import datawave.test.framework.generators.query.term.EqTerm;
import datawave.test.framework.generators.query.term.NeTerm;
import datawave.test.framework.util.MetadataColumn;

class UnionFactoryTest extends BaseQueryMetadataFactoryTest {

    private UnionFactory factory;

    @Override
    UnionFactory getQueryMetadataFactory() {
        return factory;
    }

    /**
     * Parity with the previous {@code UnionIndexedTerms} behavior: two distinct indexed fields sharing the same field list must be paired exactly once,
     * skipping self-pairing and symmetric duplicates.
     */
    @Test
    @Override
    void testIdealConditions() {
        FieldMetadata first = createIndexed();
        first.setEventIds(List.of(1, 2));
        first.setValues(List.of("a"));
        first.setNormalizers(List.of(new LcNoDiacriticsType()));

        FieldMetadata second = new FieldMetadata("INDEXED_2");
        second.setMetadataColumns(List.of(MetadataColumn.I, MetadataColumn.E));
        second.setEventIds(List.of(2, 3));
        second.setValues(List.of("z"));
        second.setNormalizers(List.of(new LcNoDiacriticsType()));

        List<FieldMetadata> shared = List.of(first, second);

        factory = new UnionFactory(new EqTerm(), new EqTerm());
        factory.setLeftMetadata(shared);
        factory.setRightMetadata(shared);

        List<QueryMetadata> expected = new ArrayList<>();
        expected.add(QueryMetadata.of("INDEXED == 'a' || INDEXED_2 == 'z'", "INDEXED == 'a' || INDEXED_2 == 'z'", List.of(1, 2, 3)));

        executeTest(expected);
    }

    @Test
    void testNegatedTermRejected() {
        factory = new UnionFactory(new EqTerm(), new NeTerm());
        factory.setLeftMetadata(List.of(createIndexed()));
        factory.setRightMetadata(List.of(createEventOnly()));
        assertThrows(IllegalStateException.class, () -> factory.getQueries());
    }
}
