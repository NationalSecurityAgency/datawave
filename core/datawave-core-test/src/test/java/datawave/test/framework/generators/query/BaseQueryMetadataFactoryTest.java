package datawave.test.framework.generators.query;

import static datawave.test.framework.util.MetadataColumn.E;
import static datawave.test.framework.util.MetadataColumn.I;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;

import datawave.test.framework.FieldMetadata;

/**
 * Base class for {@link QueryMetadataFactory} tests.
 * <p>
 * Given a list of input {@link FieldMetadata} entries, determine the output {@link QueryMetadata}
 */
public abstract class BaseQueryMetadataFactoryTest {

    protected List<FieldMetadata> fields = new ArrayList<>();

    @BeforeEach
    void beforeEach() {
        fields.clear();
    }

    /**
     * Each test should provide its own instance of {@link QueryMetadataFactory}
     *
     * @return the query metadata factory
     */
    abstract QueryMetadataFactory getQueryMetadataFactory();

    /**
     * Utility method that creates {@link FieldMetadata} for an indexed field
     *
     * @return indexed {@link FieldMetadata}
     */
    protected FieldMetadata createIndexed() {
        FieldMetadata metadata = new FieldMetadata("INDEXED");
        metadata.setMetadataColumns(List.of(I, E));
        return metadata;
    }

    /**
     * Utility method that creates {@link FieldMetadata} for an event-only field
     *
     * @return event-only {@link FieldMetadata}
     */
    protected FieldMetadata createEventOnly() {
        FieldMetadata metadata = new FieldMetadata("EVENT_ONLY");
        metadata.setMetadataColumns(List.of(E));
        return metadata;
    }

    abstract void testIdealConditions();

    /**
     * Executes the test given the configured {@link FieldMetadata} and the provided {@link QueryMetadata} expectations
     *
     * @param expected
     *            the expected {@link QueryMetadata}
     */
    protected void executeTest(List<QueryMetadata> expected) {
        QueryMetadataFactory factory = getQueryMetadataFactory();

        List<QueryMetadata> results = factory.getQueries();
        assertEquals(expected, results);
    }

}
