package datawave.query.transformer;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.core.query.result.event.DefaultResponseObjectFactory;
import datawave.marking.MarkingFunctions;
import datawave.marking.MarkingFunctionsFactory;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.attributes.Document;
import datawave.query.attributes.Numeric;
import datawave.query.function.serializer.KryoDocumentSerializer;
import datawave.table.constants.TableName;
import datawave.webservice.query.result.event.DefaultEvent;
import datawave.webservice.query.result.event.DefaultField;
import datawave.webservice.query.result.event.ResponseObjectFactory;

public class DocumentTransformerTest {

    // variables used to build the transformer
    private final Query query = new QueryImpl();
    private final MarkingFunctions<?> markingFunctions = MarkingFunctionsFactory.createMarkingFunctions();
    private final ResponseObjectFactory responseObjectFactory = new DefaultResponseObjectFactory();
    private final KryoDocumentSerializer serializer = new KryoDocumentSerializer();

    // used to create the document, attributes
    private final Key documentKey = new Key("row", "datatype\0uid");
    private Document document;
    private DocumentTransformer transformer;

    // used to transform the document attributes
    private final Map<String,List<String>> primaryToSecondaryFieldMap = new HashMap<>();

    @BeforeEach
    public void beforeEach() {
        query.setQueryAuthorizations("A,B,C");
        document = new Document();
        // load the document with a default value
        givenDocumentFieldValue("field1", "5");

        String tableName = TableName.SHARD;
        transformer = new DocumentTransformer(tableName, query, markingFunctions, responseObjectFactory, true);
    }

    @AfterEach
    public void afterEach() {
        primaryToSecondaryFieldMap.clear();
        document = null;
    }

    private void givenDocumentFieldValue(String field, String value) {
        assertNotNull(document);
        document.put(field, new Numeric(value, documentKey, true));
    }

    private void givenFieldMapping(String field, List<String> mappings) {
        primaryToSecondaryFieldMap.put(field, mappings);
    }

    private Map.Entry<Key,Value> createEntry() {
        Map.Entry<Key,Document> entry = new SimpleEntry<>(documentKey, document);
        return serializer.apply(entry);
    }

    @Test
    public void testNoPrimaryToSecondaryMap() {
        DefaultEvent event = (DefaultEvent) transformer.transform(createEntry());

        List<DefaultField> fields = event.getFields();
        assertFieldValue(fields, "field1", "5");
    }

    @Test
    public void testPrimaryToSecondaryMapWithEmptySecondaryMap() {
        givenFieldMapping("field2", Collections.emptyList());
        transformer.setPrimaryToSecondaryFieldMap(primaryToSecondaryFieldMap);

        DefaultEvent event = (DefaultEvent) transformer.transform(createEntry());

        List<DefaultField> fields = event.getFields();
        assertFieldValue(fields, "field1", "5");
    }

    @Test
    public void testPrimaryToSecondaryFieldMapDoesNotMatch() {
        givenFieldMapping("field2", List.of("field3"));
        transformer.setPrimaryToSecondaryFieldMap(primaryToSecondaryFieldMap);

        DefaultEvent event = (DefaultEvent) transformer.transform(createEntry());

        List<DefaultField> fields = event.getFields();
        assertFieldValue(fields, "field1", "5");
    }

    @Test
    public void testPrimaryToSecondaryFieldMapMatches() {
        givenFieldMapping("field2", List.of("field1"));
        transformer.setPrimaryToSecondaryFieldMap(primaryToSecondaryFieldMap);

        DefaultEvent event = (DefaultEvent) transformer.transform(createEntry());

        List<DefaultField> fields = event.getFields();
        assertFieldValue(fields, "field2", "5");
        assertFieldValue(fields, "field1", "5");
    }

    @Test
    public void testPrimaryToSecondaryFieldMapMatchesOrderSet() {
        givenDocumentFieldValue("field3", "6");
        givenFieldMapping("field2", List.of("field3", "field1"));

        transformer.setPrimaryToSecondaryFieldMap(primaryToSecondaryFieldMap);

        DefaultEvent event = (DefaultEvent) transformer.transform(createEntry());

        List<DefaultField> fields = event.getFields();
        assertFieldValue(fields, "field2", "6");
        assertFieldValue(fields, "field3", "6");
        assertFieldValue(fields, "field1", "5");
    }

    private void assertFieldValue(List<DefaultField> fields, String expectedField, String expectedValue) {

        boolean success = false;
        for (DefaultField field : fields) {
            if (expectedField.equals(field.getName()) && expectedValue.equals(field.getValueString())) {
                success = true;
                break;
            }
        }
        assertTrue(success);
    }
}
