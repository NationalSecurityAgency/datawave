package datawave.ingest.mapreduce;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.config.BaseNormalizedContent;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.data.config.ingest.MinimalistIngestHelperImpl;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.writer.ContextWriter;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Map;

import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This test class aims to test only the scenarios in which EventMapper encounters an error, to verify its behavior with the FieldSalvager interface. It's
 * modeled after EventMapperTest.
 */
public class EventMapperSalvageFieldsOnErrorTest {
    private static final String[] SALVAGED_FIELDS = {"ISBN", "author", "borrower", "dueDate", "libraryBranch"};
    private static final String[] SUPPLEMENTAL_FIELDS = {FieldHarvester.LOAD_DATE_FIELDNAME, FieldHarvester.RAW_FILE_FIELDNAME,
            FieldHarvester.SEQUENCE_FILE_FIELDNAME};
    
    @Rule
    public EasyMockRule easyMockRule = new EasyMockRule(this);
    
    @Mock
    private Mapper.Context mapContext;
    
    private Configuration conf;
    private SimpleRawRecord record;
    private EventMapper<LongWritable,RawRecordContainer,BulkIngestKey,Value> eventMapper;
    
    public void setupTest(String dataTypeHandler) throws Exception {
        eventMapper = new EventMapper<>();
        
        conf = new Configuration();
        conf.setClass(EventMapper.CONTEXT_WRITER_CLASS, TestContextWriter.class, ContextWriter.class);
        EventMapperTest.setupMapContextMock(mapContext, conf);
        
        record = IngestTestSetup.createRecord(dataTypeHandler, conf);
    }
    
    /**
     * SalvagingDataTypeHandler provides a FieldSalvager implementation that deserializes rawData as a Map&lt;String, String&gt;, then returns a Multimap
     * containing only the SALVAGED_FIELDS that were encountered, skipping all others.
     */
    public static class SalvagingDataTypeHandler extends SimpleDataTypeHandler {
        @Override
        public IngestHelperInterface getHelper(Type datatype) {
            MinimalistIngestHelperImpl fakeSalvagingIngestHelper = new MinimalistIngestHelperImpl() {
                @Override
                public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer value) {
                    throw new RuntimeException("Simulated exception while getting event fields for value.");
                }
                
                @Override
                public void getEventFields(RawRecordContainer value, Multimap<String,NormalizedContentInterface> fields) {
                    try {
                        fields.putAll(getEventFields(value));
                    } catch (Exception exception) {
                        // salvage fields implementation
                        byte[] rawData = value.getRawData();
                        
                        try (ByteArrayInputStream bytArrayInputStream = new ByteArrayInputStream(rawData);
                                        ObjectInputStream objectInputStream = new ObjectInputStream(bytArrayInputStream);) {
                            Map<String,String> deserializedData = (Map<String,String>) objectInputStream.readObject();
                            for (String fieldToSalvage : SALVAGED_FIELDS) {
                                String fieldValue = deserializedData.get(fieldToSalvage);
                                if (null != fieldValue) {
                                    try {
                                        fields.put(fieldToSalvage, new BaseNormalizedContent(fieldToSalvage, fieldValue));
                                    } catch (Exception fieldException) {
                                        // skip this field and proceed to the next
                                    }
                                }
                            }
                        } catch (IOException | ClassNotFoundException e) {
                            throw new IllegalStateException(e);
                        }
                        throw exception;
                    }
                }
            };
            return fakeSalvagingIngestHelper;
        }
    }
    
    /**
     * NoopSalvagingSimpleDataTypeHandler provides a MinimalistIngestHelperImpl implementation that does nothing to fields.
     */
    public static class NoopSalvagingSimpleDataTypeHandler extends SimpleDataTypeHandler {
        @Override
        public IngestHelperInterface getHelper(Type datatype) {
            return new MinimalistIngestHelperImpl() {
                @Override
                public void getEventFields(RawRecordContainer value, Multimap<String,NormalizedContentInterface> fields) {}
            };
        }
    }
    
    @After
    public void checkMock() {
        verify(mapContext);
    }
    
    @Test
    public void shouldSalvageAllFields() throws Exception {
        setupTest(SalvagingDataTypeHandler.class.getName());
        
        // Add both salvageable and unsalvageable fields as rawData
        HashMap<String,String> fieldValues = createMapOfSalveagableFieldValues();
        addUnsalvageableFieldsToMap(fieldValues);
        record.setRawData(IngestTestSetup.objectToRawBytes(fieldValues));
        
        runMapper(); // will throw error, calling ErrorDataTypeHandler. See FakeSalvagingIngestHelper.getEventFields
        Multimap<BulkIngestKey,Value> written = TestContextWriter.getWritten();
        
        // Expect only the salvageable fields, each exactly once
        assertEquals(written.toString(), SALVAGED_FIELDS.length + SUPPLEMENTAL_FIELDS.length, written.size());
        Map<String,Integer> fieldNameOccurrences = countPersistedFieldsByName(written);
        for (String expectedFieldName : SALVAGED_FIELDS) {
            assertTrue(fieldNameOccurrences.toString(), fieldNameOccurrences.containsKey(expectedFieldName));
            assertEquals(1, (int) fieldNameOccurrences.get(expectedFieldName));
        }
        verifyContainsSupplementalFields(fieldNameOccurrences);
    }
    
    private void verifyContainsSupplementalFields(Map<String,Integer> fieldNameOccurrences) {
        for (String expectedFieldName : SUPPLEMENTAL_FIELDS) {
            assertTrue(fieldNameOccurrences.toString(), fieldNameOccurrences.containsKey(expectedFieldName));
            assertEquals(1, (int) fieldNameOccurrences.get(expectedFieldName));
        }
    }
    
    @Test
    public void shouldTolerateNullSalvagedFieldsMap() throws Exception {
        // Use a DataTypeHandler that provides a FieldSalvager that always returns null
        setupTest(NoopSalvagingSimpleDataTypeHandler.class.getName());
        
        // Create a record with salvageable and unsalvageable fields
        HashMap<String,String> fieldValues = createMapOfSalveagableFieldValues();
        addUnsalvageableFieldsToMap(fieldValues);
        record.setRawData(IngestTestSetup.objectToRawBytes(fieldValues));
        
        runMapper(); // will throw error, calling ErrorDataTypeHandler. See FakeSalvagingIngestHelper.getEventFields
        Multimap<BulkIngestKey,Value> written = TestContextWriter.getWritten();
        
        // Expect all of the salvageable fields to be missing but supplemental fields to appear
        assertEquals(written.toString(), SUPPLEMENTAL_FIELDS.length, written.size());
        Map<String,Integer> fieldNameOccurrences = countPersistedFieldsByName(written);
        verifyContainsSupplementalFields(fieldNameOccurrences);
    }
    
    @Test
    public void shouldIgnoreNonSalvagedFields() throws Exception {
        setupTest(SalvagingDataTypeHandler.class.getName());
        
        // Add only unsalvagable fields
        HashMap<String,String> fieldValues = new HashMap<>();
        addUnsalvageableFieldsToMap(fieldValues);
        record.setRawData(IngestTestSetup.objectToRawBytes(fieldValues));
        
        runMapper(); // will throw error, calling ErrorDataTypeHandler. See FakeSalvagingIngestHelper.getEventFields
        Multimap<BulkIngestKey,Value> written = TestContextWriter.getWritten();
        
        // Expect all of the salvageable fields to be missing but supplemental fields to appear
        assertEquals(written.toString(), SUPPLEMENTAL_FIELDS.length, written.size());
        Map<String,Integer> fieldNameOccurrences = countPersistedFieldsByName(written);
        verifyContainsSupplementalFields(fieldNameOccurrences);
    }
    
    @Test
    public void shouldTolerateErrorInSalvager() throws Exception {
        setupTest(SalvagingDataTypeHandler.class.getName());
        
        // Set raw data to an invalid format, causing an error in the FieldSalvager implementation
        record.setRawData("Not a map".getBytes());
        
        runMapper(); // will throw error, calling ErrorDataTypeHandler. See FakeSalvagingIngestHelper.getEventFields
        Multimap<BulkIngestKey,Value> written = TestContextWriter.getWritten();
        
        // Expect all of the salvageable fields to be missing but supplemental fields to appear
        assertEquals(written.toString(), SUPPLEMENTAL_FIELDS.length, written.size());
        Map<String,Integer> fieldNameOccurrences = countPersistedFieldsByName(written);
        verifyContainsSupplementalFields(fieldNameOccurrences);
    }
    
    private void runMapper() throws IOException, InterruptedException {
        eventMapper.setup(mapContext);
        eventMapper.map(new LongWritable(1), record, mapContext);
        eventMapper.cleanup(mapContext);
    }
    
    private HashMap<String,String> createMapOfSalveagableFieldValues() {
        HashMap<String,String> fieldValues = new HashMap<>();
        fieldValues.put("ISBN", "0-123-00000-1");
        fieldValues.put("dueDate", "20211126");
        fieldValues.put("author", "Henry Hope Reed");
        fieldValues.put("borrower", "Edward Clark Potter");
        fieldValues.put("libraryBranch", "Grand Central");
        return fieldValues;
    }
    
    private void addUnsalvageableFieldsToMap(HashMap<String,String> fieldValues) {
        fieldValues.put("genre", "FICTION");
        fieldValues.put("format", "Hardcover");
    }
    
    /**
     * Extracts field names from persisted data, creating a map containing the number of occurrences per field name.
     */
    private Map<String,Integer> countPersistedFieldsByName(Multimap<BulkIngestKey,Value> entries) {
        Map<String,Integer> result = new HashMap<>();
        for (BulkIngestKey key : entries.keys()) {
            String fieldName = key.getKey().getColumnFamily().toString();
            // create or increment
            result.merge(fieldName, 1, Integer::sum);
        }
        return result;
    }
}
