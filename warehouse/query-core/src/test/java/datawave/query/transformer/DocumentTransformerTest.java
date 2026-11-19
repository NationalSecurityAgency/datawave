package datawave.query.transformer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

import datawave.core.query.exception.EmptyObjectException;
import datawave.core.query.result.event.DefaultResponseObjectFactory;
import datawave.marking.MarkingFunctions;
import datawave.marking.MarkingFunctionsFactory;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.attributes.Document;
import datawave.query.attributes.Numeric;
import datawave.query.attributes.TimingMetadata;
import datawave.query.attributes.WaitWindowExceededMetadata;
import datawave.query.function.LogTiming;
import datawave.query.function.serializer.KryoDocumentSerializer;
import datawave.query.iterator.waitwindow.WaitWindowObserver;
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
    public void recordsWebserverProgressWhenUniqueOrGroupingConsumesACandidate() {
        transformer.setReturnScanProgress(true);
        for (DocumentTransform transform : List.of(mock(UniqueTransform.class), mock(GroupingTransform.class))) {
            when(transform.apply(any())).thenReturn(null);
            transformer.addTransform(transform);
            assertTrue(transformer.tracksWebserverProgress());
            long before = transformer.getWebserverProgress();
            assertThrows(EmptyObjectException.class, () -> transformer.transform(createEntry()));
            assertEquals(before + 1, transformer.getWebserverProgress());
            transformer = new DocumentTransformer(TableName.SHARD, query, markingFunctions, responseObjectFactory, true);
            transformer.setReturnScanProgress(true);
        }
    }

    @Test
    public void timingOnlyOverrunIsStillMetricsOnly() {
        transformer.setReturnScanProgress(true);
        document = WaitWindowObserver.getWaitWindowOverrunDocument();
        addYieldMetric();
        assertThrows(EmptyObjectException.class, () -> transformer.transform(createEntry()));
        assertEquals(1, transformer.getYieldCount());
    }

    @Test
    public void progressWithTimingRecordsMetricsAndBypassesWebserverTransforms() {
        transformer.setReturnScanProgress(true);
        UniqueTransform unique = mock(UniqueTransform.class);
        transformer.addTransform(unique);
        document = WaitWindowObserver.getWaitWindowOverrunDocument();
        document.put(WaitWindowObserver.SCAN_PROGRESS, new WaitWindowExceededMetadata());
        addYieldMetric();
        assertTrue(transformer.transform(createEntry()).isIntermediateResult());
        assertEquals(1, transformer.getYieldCount());
        assertEquals(0, transformer.getWebserverProgress());
        verify(unique, never()).apply(any());
    }

    @Test
    public void progressWithoutTimingOnlyReturnsAnIntermediateResultWhenEnabled() {
        document = WaitWindowObserver.getWaitWindowOverrunDocument();
        document.put(WaitWindowObserver.SCAN_PROGRESS, new WaitWindowExceededMetadata());
        assertThrows(EmptyObjectException.class, () -> transformer.transform(createEntry()));
        transformer.setReturnScanProgress(true);
        assertTrue(transformer.transform(createEntry()).isIntermediateResult());
        assertEquals(0, transformer.getYieldCount());
    }

    private void addYieldMetric() {
        TimingMetadata timing = new TimingMetadata();
        timing.setYieldCount(1);
        document.put(LogTiming.TIMING_METADATA, timing);
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
