package datawave.ingest.mapreduce;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.config.BaseNormalizedContent;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.ContentBaseIngestHelper;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
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
     * FakeSalvagingIngestHelper: - always throws an exception when getEventFields is called, to ensure error handling code path is reached within EventMapper.
     * - allows for anonymous inline helper creation.
     */
    public static abstract class FakeSalvagingIngestHelper extends ContentBaseIngestHelper implements FieldSalvager {
        @Override
        public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer value) {
            throw new RuntimeException("Simulated exception while getting event fields for value.");
        }
    }
    
    /**
     * SalvagingDataTypeHandler provides a FieldSalvager implementation that deserializes rawData as a Map&lt;String, String&gt;, then returns a Multimap
     * containing only the SALVAGED_FIELDS that were encountered, skipping all others.
     */
    public static class SalvagingDataTypeHandler extends SimpleDataTypeHandler {
        @Override
        public IngestHelperInterface getHelper(Type datatype) {
            FakeSalvagingIngestHelper fakeSalvagingIngestHelper = new FakeSalvagingIngestHelper() {
                @Override
                public Multimap<String,NormalizedContentInterface> getSalvageableEventFields(RawRecordContainer value) {
                    HashMultimap<String,NormalizedContentInterface> fields = HashMultimap.create();
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
                    } catch (Exception e) {
                        return fields;
                    }
                    return fields;
                }
                
                @Override
                public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer value) {
                    throw new RuntimeException("Simulated exception while getting event fields for value.");
                }
            };
            return fakeSalvagingIngestHelper;
        }
    }
    
    /**
     * NullSalvagingSimpleDataTypeHandler provides a FieldSalvager implementation that always returns null.
     */
    public static class NullSalvagingSimpleDataTypeHandler extends SimpleDataTypeHandler {
        @Override
        public IngestHelperInterface getHelper(Type datatype) {
            return new FakeSalvagingIngestHelper() {
                @Override
                public Multimap<String,NormalizedContentInterface> getSalvageableEventFields(RawRecordContainer value) {
                    return null;
                }
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
        assertEquals(written.toString(), SALVAGED_FIELDS.length, written.size());
        Map<String,Integer> fieldNameOccurrences = countPersistedFieldsByName(written);
        for (String expectedFieldName : SALVAGED_FIELDS) {
            assertTrue(fieldNameOccurrences.toString(), fieldNameOccurrences.containsKey(expectedFieldName));
            assertEquals(1, (int) fieldNameOccurrences.get(expectedFieldName));
        }
    }
    
    @Test
    public void shouldTolerateNullSalvagedFieldsMap() throws Exception {
        // Use a DataTypeHandler that provides a FieldSalvager that always returns null
        setupTest(NullSalvagingSimpleDataTypeHandler.class.getName());
        
        // Create a record with salvageable and unsalvageable fields
        HashMap<String,String> fieldValues = createMapOfSalveagableFieldValues();
        addUnsalvageableFieldsToMap(fieldValues);
        record.setRawData(IngestTestSetup.objectToRawBytes(fieldValues));
        
        runMapper(); // will throw error, calling ErrorDataTypeHandler. See FakeSalvagingIngestHelper.getEventFields
        Multimap<BulkIngestKey,Value> written = TestContextWriter.getWritten();
        
        // Expect nothing to be salvaged
        assertEquals(written.toString(), 0, written.size());
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
        
        // Expect all of the salvageable fields to occur once
        assertEquals(written.toString(), 0, written.size());
    }
    
    @Test
    public void shouldTolerateErrorInSalvager() throws Exception {
        setupTest(SalvagingDataTypeHandler.class.getName());
        
        // Set raw data to an invalid format, causing an error in the FieldSalvager implementation
        record.setRawData("Not a map".getBytes());
        
        runMapper(); // will throw error, calling ErrorDataTypeHandler. See FakeSalvagingIngestHelper.getEventFields
        Multimap<BulkIngestKey,Value> written = TestContextWriter.getWritten();
        
        // Expect all of the salvageable fields to occur once
        assertEquals(written.toString(), 0, written.size());
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
