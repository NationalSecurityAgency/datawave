package datawave.ingest.metadata;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.IdentityDataType;
import datawave.TestBaseIngestHelper;
import datawave.TestAbstractContentIngestHelper;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

// currently this only tests LoadDate counts
public class EventMetadataTest {
    private static final String FIELD_NAME = "FIELD_NAME";
    private static final String FIELD_TO_COUNT = "FIELD_1";
    private static final String FIELD_WITH_TOKEN_TO_COUNT = "FIELD_1_TOKEN";
    private static final String DATA_TYPE = "xyzabc";
    private static final String FIELD_NAME_FOR_LOAD_DATE = "LOAD_DATE";
    private static final String TF = "tf";
    private static final String T = "t";
    private static final String E = "e";
    private static final String I = "i";
    private static final String VALUE_FOR_LOAD_DATE = "20140404";
    private static final String NUMBER_TYPE = "datawave.data.type.NumberType";
    private static final Text METADATA_TABLE_NAME = new Text("table123456");
    private static final Text LOADDATES_TABLE_NAME = new Text("loaddates");
    private static final Text INDEX_TABLE_NAME = new Text("index");
    private static final Text RINDEX_TABLE_NAME = new Text("reverseIndex");
    private BaseIngestHelper helper;
    private RawRecordContainer event = EasyMock.createMock(RawRecordContainer.class);
    private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
    
    @Before
    public void setupIngestHelper() {
        helper = new TestBaseIngestHelper(createEventFields()) {
            @Override
            public List<datawave.data.type.Type<?>> getDataTypes(String fieldName) {
                return Arrays.asList(new IdentityDataType());
            }
        };
    }
    
    public void setupAbstractContentIngestHelper(boolean tokenIndex, boolean listIndex) {
        helper = new TestAbstractContentIngestHelper(createEventFields()) {
            
            private boolean isTokenIndex = tokenIndex;
            private boolean isListIndex = listIndex;
            
            @Override
            public boolean isDataTypeField(String fieldname) {
                return true;
            }
            
            @Override
            public List<datawave.data.type.Type<?>> getDataTypes(String fieldName) {
                datawave.data.type.Type<?> type[] = {datawave.data.type.Type.Factory.createType(NUMBER_TYPE)};
                return Arrays.asList(type);
            }
            
            @Override
            public boolean isContentIndexField(String field) {
                return isTokenIndex;
            }
            
            @Override
            public boolean isIndexListField(String field) {
                return isListIndex;
            };
            
        };
    }
    
    @Test
    public void testCreatesIndexedField() throws IOException {
        setupMocks();
        
        helper.addIndexedField(FIELD_TO_COUNT);
        RawRecordMetadata eventMetadata = new EventMetadata(null, METADATA_TABLE_NAME, LOADDATES_TABLE_NAME, INDEX_TABLE_NAME, RINDEX_TABLE_NAME, true);
        eventMetadata.addEvent(helper, event, createEventFields(), getLoadDateAsMillis());
        
        assertFieldNameCountEquals(1L, INDEX_TABLE_NAME, FIELD_TO_COUNT, eventMetadata);
        Assert.assertFalse(assertContainsKey(eventMetadata, RINDEX_TABLE_NAME, FIELD_TO_COUNT));
        assertNonIndexedFieldNameIsMissing(eventMetadata);
        
        Assert.assertTrue(assertEExists(FIELD_TO_COUNT, eventMetadata));
        Assert.assertTrue(assertIExists(FIELD_TO_COUNT, eventMetadata));
        
        EasyMock.verify(event);
    }
    
    @Test
    public void testCreatesTermFrequencyForTokenization() throws IOException {
        setupAbstractContentIngestHelper(true, false);
        setupMocks();
        
        helper.addIndexedField(FIELD_TO_COUNT);
        RawRecordMetadata eventMetadata = new EventMetadata(null, METADATA_TABLE_NAME, LOADDATES_TABLE_NAME, INDEX_TABLE_NAME, RINDEX_TABLE_NAME, true);
        eventMetadata.addEvent(helper, event, createEventFields(), getLoadDateAsMillis());
        
        assertFieldNameCountEquals(1L, INDEX_TABLE_NAME, FIELD_TO_COUNT, eventMetadata);
        assertFieldNameCountEquals(1L, INDEX_TABLE_NAME, FIELD_WITH_TOKEN_TO_COUNT, eventMetadata);
        assertTfCountEquals(0L, FIELD_WITH_TOKEN_TO_COUNT, eventMetadata);
        Assert.assertFalse(assertTExists(FIELD_WITH_TOKEN_TO_COUNT, eventMetadata));
        Assert.assertFalse(assertContainsKey(eventMetadata, RINDEX_TABLE_NAME, FIELD_TO_COUNT));
        assertNonIndexedFieldNameIsMissing(eventMetadata);
        
        EasyMock.verify(event);
    }
    
    @Test
    public void testCreatesTermFrequencyForLists() throws IOException {
        setupAbstractContentIngestHelper(false, true);
        setupMocks();
        
        RawRecordMetadata eventMetadata = new EventMetadata(null, METADATA_TABLE_NAME, LOADDATES_TABLE_NAME, INDEX_TABLE_NAME, RINDEX_TABLE_NAME, true);
        eventMetadata.addEvent(helper, event, createEventFields(), getLoadDateAsMillis());
        
        assertFieldNameCountEquals(1L, INDEX_TABLE_NAME, FIELD_TO_COUNT, eventMetadata);
        Assert.assertFalse(assertContainsKey(eventMetadata, INDEX_TABLE_NAME, FIELD_WITH_TOKEN_TO_COUNT));
        assertTfCountEquals(0L, FIELD_TO_COUNT, eventMetadata);
        assertTCountEquals(0L, FIELD_TO_COUNT, eventMetadata);
        Assert.assertFalse(assertContainsKey(eventMetadata, RINDEX_TABLE_NAME, FIELD_TO_COUNT));
        assertNonIndexedFieldNameIsMissing(eventMetadata);
        
        EasyMock.verify(event);
    }
    
    @Test
    public void testCreatesReverseIndexedField() throws IOException {
        setupMocks();
        
        helper.addReverseIndexedField(FIELD_TO_COUNT);
        RawRecordMetadata eventMetadata = new EventMetadata(null, METADATA_TABLE_NAME, LOADDATES_TABLE_NAME, INDEX_TABLE_NAME, RINDEX_TABLE_NAME, true);
        eventMetadata.addEvent(helper, event, createEventFields(), getLoadDateAsMillis());
        
        assertFieldNameCountEquals(1L, RINDEX_TABLE_NAME, FIELD_TO_COUNT, eventMetadata);
        Assert.assertFalse(assertContainsKey(eventMetadata, INDEX_TABLE_NAME, FIELD_TO_COUNT));
        assertNonIndexedFieldNameIsMissing(eventMetadata);
        
        EasyMock.verify(event);
    }
    
    @Test
    public void testCreatesBoth() throws IOException {
        setupMocks();
        
        helper.addReverseIndexedField(FIELD_TO_COUNT);
        helper.addIndexedField(FIELD_TO_COUNT);
        
        RawRecordMetadata eventMetadata = new EventMetadata(null, METADATA_TABLE_NAME, LOADDATES_TABLE_NAME, INDEX_TABLE_NAME, RINDEX_TABLE_NAME, true);
        eventMetadata.addEvent(helper, event, createEventFields(), getLoadDateAsMillis());
        
        assertFieldNameCountEquals(1L, INDEX_TABLE_NAME, FIELD_TO_COUNT, eventMetadata);
        assertFieldNameCountEquals(1L, RINDEX_TABLE_NAME, FIELD_TO_COUNT, eventMetadata);
        
        assertNonIndexedFieldNameIsMissing(eventMetadata);
        
        EasyMock.verify(event);
    }
    
    @Test
    public void testCountsTwo() throws IOException {
        setupMocks();
        
        helper.addIndexedField(FIELD_TO_COUNT);
        RawRecordMetadata eventMetadata = new EventMetadata(null, METADATA_TABLE_NAME, LOADDATES_TABLE_NAME, INDEX_TABLE_NAME, RINDEX_TABLE_NAME, true);
        eventMetadata.addEvent(helper, event, createEventFields(), getLoadDateAsMillis());
        eventMetadata.addEvent(helper, event, createEventFields(), getLoadDateAsMillis());
        
        assertFieldNameCountEquals(2L, INDEX_TABLE_NAME, FIELD_TO_COUNT, eventMetadata);
        
        EasyMock.verify(event);
    }
    
    @Test
    public void testWithExclusions() throws IOException {
        setupMocks();
        helper.addShardExclusionField(FIELD_TO_COUNT);
        RawRecordMetadata eventMetadata = new EventMetadata(null, METADATA_TABLE_NAME, LOADDATES_TABLE_NAME, INDEX_TABLE_NAME, RINDEX_TABLE_NAME, true);
        eventMetadata.addEvent(helper, event, createEventFields(), getLoadDateAsMillis());
        
        Assert.assertFalse(assertEExists(FIELD_TO_COUNT, eventMetadata));
        
        EasyMock.verify(event);
    }
    
    private void assertFieldNameCountEquals(long expectedCount, Text tableName, String fieldName, RawRecordMetadata eventMetadata) {
        Text expectedColumnFamily = new Text(FIELD_NAME + RawRecordMetadata.DELIMITER + tableName);
        Text expectedColumnQualifier = new Text(VALUE_FOR_LOAD_DATE + RawRecordMetadata.DELIMITER + DATA_TYPE);
        assertCountEquals(expectedCount, fieldName, eventMetadata, LOADDATES_TABLE_NAME, expectedColumnFamily, expectedColumnQualifier);
    }
    
    private boolean assertEExists(String fieldName, RawRecordMetadata eventMetadata) {
        Text expectedColumnFamily = new Text(E);
        Text expectedColumnQualifier = new Text(DATA_TYPE);
        return assertExists(fieldName, eventMetadata, METADATA_TABLE_NAME, expectedColumnFamily, expectedColumnQualifier);
    }
    
    private boolean assertIExists(String fieldName, RawRecordMetadata eventMetadata) {
        Text expectedColumnFamily = new Text(I);
        Text expectedColumnQualifier = new Text(DATA_TYPE);
        return assertExists(fieldName, eventMetadata, METADATA_TABLE_NAME, expectedColumnFamily, expectedColumnQualifier);
    }
    
    private void assertTfCountEquals(long expectedCount, String fieldName, RawRecordMetadata eventMetadata) {
        Text expectedColumnFamily = new Text(TF);
        Text expectedColumnQualifier = new Text(DATA_TYPE);
        assertCountEquals(expectedCount, fieldName, eventMetadata, METADATA_TABLE_NAME, expectedColumnFamily, expectedColumnQualifier);
    }
    
    private boolean assertTExists(String fieldName, RawRecordMetadata eventMetadata) {
        Text expectedColumnFamily = new Text(T);
        Text expectedColumnQualifier = new Text(DATA_TYPE + RawRecordMetadata.DELIMITER + NUMBER_TYPE);
        return assertExists(fieldName, eventMetadata, METADATA_TABLE_NAME, expectedColumnFamily, expectedColumnQualifier);
    }
    
    private void assertTCountEquals(long expectedCount, String fieldName, RawRecordMetadata eventMetadata) {
        Text expectedColumnFamily = new Text(T);
        Text expectedColumnQualifier = new Text(DATA_TYPE + RawRecordMetadata.DELIMITER + NUMBER_TYPE);
        assertCountEquals(expectedCount, fieldName, eventMetadata, METADATA_TABLE_NAME, expectedColumnFamily, expectedColumnQualifier);
    }
    
    private void assertCountEquals(long expectedCount, String rowId, RawRecordMetadata eventMetadata, Text tableName, Text expectedColumnFamily,
                    Text expectedColumnQualifier) {
        BulkIngestKey expectedBulkIngestKey = createExpectedBulkIngestKey(expectedColumnFamily, tableName, rowId, expectedColumnQualifier);
        Assert.assertTrue("Didn't have entry for " + expectedColumnFamily + ", " + rowId, assertContainsKey(eventMetadata, expectedBulkIngestKey));
        Iterator<Value> values = getCorrespondingValue(eventMetadata, expectedBulkIngestKey).iterator();
        Assert.assertTrue(values.hasNext());
        
        byte[] value = values.next().get();
        if (expectedCount > 0) {
            Assert.assertEquals(expectedCount, (long) SummingCombiner.VAR_LEN_ENCODER.decode(value));
        }
        
        Assert.assertFalse(values.hasNext());
    }
    
    private boolean assertExists(String rowId, RawRecordMetadata eventMetadata, Text tableName, Text expectedColumnFamily, Text expectedColumnQualifier) {
        BulkIngestKey expectedBulkIngestKey = createExpectedBulkIngestKey(expectedColumnFamily, tableName, rowId, expectedColumnQualifier);
        return assertContainsKey(eventMetadata, expectedBulkIngestKey);
    }
    
    private void assertNonIndexedFieldNameIsMissing(RawRecordMetadata eventMetadata) {
        Assert.assertFalse(assertContainsKey(eventMetadata, RINDEX_TABLE_NAME, FIELD_NAME_FOR_LOAD_DATE));
        Assert.assertFalse(assertContainsKey(eventMetadata, INDEX_TABLE_NAME, FIELD_NAME_FOR_LOAD_DATE));
    }
    
    private boolean assertContainsKey(RawRecordMetadata eventMetadata, Text columnFamily, String fieldName) {
        BulkIngestKey expectedBulkIngestKey = createExpectedBulkIngestKey(columnFamily, fieldName);
        return null != getCorrespondingValue(eventMetadata, expectedBulkIngestKey);
    }
    
    private boolean assertContainsKey(RawRecordMetadata eventMetadata, BulkIngestKey expectedBulkIngestKey) {
        return null != getCorrespondingValue(eventMetadata, expectedBulkIngestKey);
    }
    
    private Collection<Value> getCorrespondingValue(RawRecordMetadata eventMetadata, BulkIngestKey expectedBulkIngestKey) {
        Multimap<BulkIngestKey,Value> bulkMetadata = eventMetadata.getBulkMetadata();
        for (BulkIngestKey actualBulkIngestKey : bulkMetadata.keySet()) {
            if (actualBulkIngestKey.getTableName().equals(expectedBulkIngestKey.getTableName())
                            && actualBulkIngestKey.getKey().equals(expectedBulkIngestKey.getKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS)) {
                return bulkMetadata.get(actualBulkIngestKey);
            }
        }
        return null;
    }
    
    private void setupMocks() {
        try {
            EasyMock.expect(event.getDataType()).andReturn(new Type(DATA_TYPE, helper.getClass(), null, null, 4, null)).anyTimes();
            EasyMock.expect(event.getDate()).andReturn(new Date().getTime()).anyTimes();
            EasyMock.expect(event.getRawData()).andReturn(new byte[] {}).anyTimes();
            EasyMock.replay(event);
        } catch (Exception e) {
            throw new RuntimeException(e); // make the test fail if the mock wasn't created properly
        }
    }
    
    private BulkIngestKey createExpectedBulkIngestKey(Text columnFamily, String fieldName) {
        return createExpectedBulkIngestKey(columnFamily, LOADDATES_TABLE_NAME, fieldName, new Text(VALUE_FOR_LOAD_DATE + RawRecordMetadata.DELIMITER
                        + DATA_TYPE));
    }
    
    private BulkIngestKey createExpectedBulkIngestKey(Text columnFamily, Text tableName, String fieldName, Text columnQualifier) {
        Key k = new Key(new Text(fieldName), columnFamily, columnQualifier);
        return new BulkIngestKey(new Text(tableName), k);
    }
    
    private Multimap<String,NormalizedContentInterface> createEventFields() {
        Multimap<String,NormalizedContentInterface> eventFields = HashMultimap.create();
        addToEventFields(eventFields, FIELD_NAME_FOR_LOAD_DATE, "" + getLoadDateAsMillis());
        addToEventFields(eventFields, FIELD_TO_COUNT, "HEY HO HEY HO");
        return eventFields;
    }
    
    private long getLoadDateAsMillis() {
        try {
            return simpleDateFormat.parse(VALUE_FOR_LOAD_DATE).getTime();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
    
    private void addToEventFields(Multimap<String,NormalizedContentInterface> eventFields, String fieldName, String value) {
        NormalizedContentInterface loadDateContent = new NormalizedFieldAndValue(fieldName, value.getBytes());
        eventFields.put(fieldName, loadDateContent);
    }
}
