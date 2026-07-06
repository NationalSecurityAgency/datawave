package datawave.test.framework.generators.query;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import datawave.data.type.LcNoDiacriticsType;
import datawave.test.framework.FieldMetadata;
import datawave.test.framework.generators.query.term.EqTerm;

class SingleTermFactoryTest extends BaseQueryMetadataFactoryTest {

    @Override
    protected QueryMetadataFactory getQueryMetadataFactory() {
        SingleTermFactory metadataFactory = new SingleTermFactory(new EqTerm());
        metadataFactory.setFieldMetadata(fields);
        return metadataFactory;
    }

    @Test
    @Override
    void testIdealConditions() {
        List<Integer> eventIds = List.of(1, 2, 3);
        FieldMetadata field = createIndexed();
        field.setValues(List.of("1", "2", "3"));
        field.setEventIds(eventIds);
        field.setNormalizers(List.of(new LcNoDiacriticsType()));
        fields.add(field);

        List<QueryMetadata> expected = new ArrayList<>();
        expected.add(QueryMetadata.of("INDEXED == '1'", "INDEXED == '1'", eventIds));
        expected.add(QueryMetadata.of("INDEXED == '2'", "INDEXED == '2'", eventIds));
        expected.add(QueryMetadata.of("INDEXED == '3'", "INDEXED == '3'", eventIds));
        executeTest(expected);
    }
}
