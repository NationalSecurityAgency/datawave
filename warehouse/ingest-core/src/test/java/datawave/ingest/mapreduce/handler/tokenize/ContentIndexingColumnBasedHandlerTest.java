package datawave.ingest.mapreduce.handler.tokenize;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import datawave.data.hash.UID;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NumberType;
import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.ingest.data.config.ingest.ContentBaseIngestHelper;
import datawave.ingest.input.reader.EventRecordReader;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.policy.IngestPolicyEnforcer;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.lucene.analysis.Analyzer;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.DataInput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

public class ContentIndexingColumnBasedHandlerTest {
    
    private Configuration conf = null;
    private TaskAttemptContext ctx = null;
    private final String LIST_DELIMITERS = ",;";
    private static final String TOKEN_DESIGNATOR = "_TOKEN";
    private static final String TEST_TYPE = "test";
    private static final String TEST_UUID = "test_uuid";
    private static final long CURRENT_TIME = System.currentTimeMillis();
    private static final char INTRA_COL_DELIMETER = '\u0000';
    
    private static final String NUMERIC_LIST = "NUMERIC_LIST";
    private static final String ALPHANUM_LIST = "APLHANUM_LIST";
    private static final String LIST_VALUE = "12.34,56.78";
    private static final String LIST_VALUE_WITH_SPACE = "12.34, 56.78";
    private static final String LIST_VALUE_WITH_EMPTY_ENTRY = "12.34, , 56.78";
    private static final String SHARD_ID = "SHARD1";
    private static final Text SHARD_TABLE_NAME = new Text("shard");
    private static final String TF = "tf";
    
    private static String[] tokenizeAlphanumResults = {"12", "34", "56", "78", "12.34,56.78"};
    private static String[] tokenizeAlphanumResultsWithSpace = {"12", "34", "56", "78", "12.34", "56.78"};
    private static String[] tokenizeAlphanumReverseResults = {"21", "43", "65", "87", "87.65,43.21"};
    private static String[] tokenizeAlphanumReverseResultsWithSpace = {"21", "43", "65", "87", "87.65", "43.21"};
    private static String[] listAlphanumResults = {"12.34", "56.78"};
    private static String[] listAlphanumReverseResults = {"43.21", "87.65"};
    private static String[] listNumericResults = {"+bE1.234", "+bE5.678"};
    private static String[] listNumericReverseResults = {"+bE4.321", "+bE8.765"};
    
    private static Multimap<String,NormalizedContentInterface> tokenizedExpectedFields = HashMultimap.create();
    private static Multimap<String,NormalizedContentInterface> tokenizedExpectedIndex = HashMultimap.create();
    private static Multimap<String,NormalizedContentInterface> tokenizedExpectedReverse = HashMultimap.create();
    private static Multimap<String,Pair<String,Integer>> tokenizedExpectedTfValues = HashMultimap.create();
    
    private static Multimap<String,NormalizedContentInterface> tokenizedExpectedFieldsWithSpace = HashMultimap.create();
    private static Multimap<String,NormalizedContentInterface> tokenizedExpectedIndexWithSpace = HashMultimap.create();
    private static Multimap<String,NormalizedContentInterface> tokenizedExpectedReverseWithSpace = HashMultimap.create();
    private static Multimap<String,Pair<String,Integer>> tokenizedExpectedTfValuesWithSpace = HashMultimap.create();
    
    private static Multimap<String,NormalizedContentInterface> listExpectedNumericFields = HashMultimap.create();
    private static Multimap<String,NormalizedContentInterface> listExpectedNumericIndex = HashMultimap.create();
    private static Multimap<String,NormalizedContentInterface> listExpectedNumericReverse = HashMultimap.create();
    private static Multimap<String,Pair<String,Integer>> listExpectedNumericTfValues = HashMultimap.create();
    
    private static Multimap<String,NormalizedContentInterface> listExpectedAlpahnumFields = HashMultimap.create();
    private static Multimap<String,NormalizedContentInterface> listExpectedAlpahnumIndex = HashMultimap.create();
    private static Multimap<String,NormalizedContentInterface> listExpectedAlpahnumReverse = HashMultimap.create();
    private static Multimap<String,Pair<String,Integer>> listExpectedAlphanumTfValues = HashMultimap.create();
    
    private RawRecordContainer event = EasyMock.createMock(RawRecordContainer.class);
    private TestContentBaseIngestHelper helper;
    private ColumnVisibility colVis;
    
    private UID uid = new UID() {
        @Override
        public int compareTo(UID o) {
            return 0;
        }
        
        @Override
        public int getTime() {
            return 0;
        }
        
        @Override
        public String getShardedPortion() {
            return null;
        }
        
        @Override
        public String getBaseUid() {
            return null;
        }
        
        @Override
        public void readFields(DataInput in) throws IOException {
            
        }
        
        @Override
        public String toString() {
            return TEST_UUID;
        }
    };
    
    @Before
    public void setUp() throws Exception {
        
        conf = new Configuration();
        conf.addResource("config/all-config.xml");
        ctx = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        ctx.getConfiguration().setInt(ContentIndexingColumnBasedHandler.NUM_SHARDS, 131);
        ctx.getConfiguration().set(ContentIndexingColumnBasedHandler.SHARD_TNAME, "shard");
        ctx.getConfiguration().set(ContentIndexingColumnBasedHandler.SHARD_GIDX_TNAME, "shardIndex");
        ctx.getConfiguration().set(ContentIndexingColumnBasedHandler.SHARD_GRIDX_TNAME, "shardIndex");
        ctx.getConfiguration().set(TypeRegistry.INGEST_DATA_TYPES, "test");
        ctx.getConfiguration().set("data.name", "test");
        ctx.getConfiguration().set("test.data.auth.id.mode", "NEVER");
        ctx.getConfiguration().set("test" + BaseIngestHelper.DEFAULT_TYPE, LcNoDiacriticsType.class.getName());
        ctx.getConfiguration().set("test" + TypeRegistry.HANDLER_CLASSES, TestContentIndexingColumnBasedHandler.class.getName());
        ctx.getConfiguration().set("test" + TypeRegistry.RAW_READER, TestEventRecordReader.class.getName());
        ctx.getConfiguration().set("test" + TypeRegistry.INGEST_HELPER, TestContentBaseIngestHelper.class.getName());
        ctx.getConfiguration().set(TypeRegistry.EXCLUDED_HANDLER_CLASSES, "FAKE_HANDLER_CLASS"); // it will die if this field is not faked
        
        helper = new TestContentBaseIngestHelper();
        colVis = new ColumnVisibility("");
    }
    
    private void setupMocks() {
        try {
            EasyMock.expect(event.getVisibility()).andReturn(colVis).anyTimes();
            EasyMock.expect(event.getDataType()).andReturn(TypeRegistry.getType(TEST_TYPE)).anyTimes();
            EasyMock.expect(event.getId()).andReturn(uid).anyTimes();
            EasyMock.expect(event.getDate()).andReturn(CURRENT_TIME).anyTimes();
            EasyMock.replay(event);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    @BeforeClass
    public static void setUpExpectedResults() {
        SetExpectedMap(ALPHANUM_LIST + TOKEN_DESIGNATOR, tokenizeAlphanumResults, tokenizeAlphanumReverseResults, tokenizedExpectedFields,
                        tokenizedExpectedIndex, tokenizedExpectedReverse, tokenizedExpectedTfValues);
        SetExpectedMap(ALPHANUM_LIST + TOKEN_DESIGNATOR, tokenizeAlphanumResultsWithSpace, tokenizeAlphanumReverseResultsWithSpace,
                        tokenizedExpectedFieldsWithSpace, tokenizedExpectedIndexWithSpace, tokenizedExpectedReverseWithSpace,
                        tokenizedExpectedTfValuesWithSpace);
        SetExpectedMap(ALPHANUM_LIST, listAlphanumResults, listAlphanumReverseResults, listExpectedAlpahnumFields, listExpectedAlpahnumIndex,
                        listExpectedAlpahnumReverse, listExpectedAlphanumTfValues);
        SetExpectedMap(NUMERIC_LIST, listNumericResults, listNumericReverseResults, listExpectedNumericFields, listExpectedNumericIndex,
                        listExpectedNumericReverse, listExpectedNumericTfValues);
    }
    
    public static void SetExpectedMap(String fieldName, String[] expectedResults, String[] expectedReversResults,
                    Multimap<String,NormalizedContentInterface> fields, Multimap<String,NormalizedContentInterface> index,
                    Multimap<String,NormalizedContentInterface> reverse, Multimap<String,Pair<String,Integer>> tfValues) {
        NormalizedContentInterface template = new NormalizedFieldAndValue();
        template.setFieldName(fieldName);
        
        for (int i = 0; i < expectedResults.length; i++) {
            template.setIndexedFieldValue(expectedResults[i]);
            template.setEventFieldValue(null);
            fields.put(fieldName, new NormalizedFieldAndValue(template));
            index.put(fieldName, new NormalizedFieldAndValue(template));
            
            template.setIndexedFieldValue(expectedReversResults[i]);
            template.setEventFieldValue(expectedReversResults[i]);
            reverse.put(fieldName, new NormalizedFieldAndValue(template));
            
            tfValues.put(fieldName, new ImmutablePair<>(expectedResults[i], i));
        }
    }
    
    @Test
    public void testHandlerNormalizedTokenizedFieldNoSpace() throws Exception {
        
        TypeRegistry.reset();
        TypeRegistry.getInstance(ctx.getConfiguration());
        
        setupMocks();
        
        TestContentIndexingColumnBasedHandler handler = new TestContentIndexingColumnBasedHandler();
        handler.setup(ctx);
        
        helper.setup(ctx.getConfiguration());
        testProcessing(handler, ALPHANUM_LIST, LIST_VALUE, tokenizedExpectedFields, tokenizedExpectedIndex, tokenizedExpectedReverse,
                        tokenizedExpectedTfValues, true);
    }
    
    @Test
    public void testHandlerNormalizedTokenizedFieldWithSpace() throws Exception {
        
        TypeRegistry.reset();
        TypeRegistry.getInstance(ctx.getConfiguration());
        
        setupMocks();
        
        TestContentIndexingColumnBasedHandler handler = new TestContentIndexingColumnBasedHandler();
        handler.setup(ctx);
        
        helper.setup(ctx.getConfiguration());
        testProcessing(handler, ALPHANUM_LIST, LIST_VALUE_WITH_SPACE, tokenizedExpectedFieldsWithSpace, tokenizedExpectedIndexWithSpace,
                        tokenizedExpectedReverseWithSpace, tokenizedExpectedTfValuesWithSpace, true);
    }
    
    @Test
    public void testHandlerListNormalizedNumericsNoSpace() throws Exception {
        
        ctx.getConfiguration().set("test" + "." + NUMERIC_LIST + BaseIngestHelper.FIELD_TYPE, NumberType.class.getName());
        ctx.getConfiguration().set("test" + "." + ContentBaseIngestHelper.LIST_DELIMITERS, LIST_DELIMITERS);
        
        TypeRegistry.reset();
        TypeRegistry.getInstance(ctx.getConfiguration());
        
        setupMocks();
        
        TestContentIndexingColumnBasedHandler handler = new TestContentIndexingColumnBasedHandler();
        handler.setup(ctx);
        
        helper.setup(ctx.getConfiguration());
        
        testProcessing(handler, NUMERIC_LIST, LIST_VALUE, listExpectedNumericFields, listExpectedNumericIndex, listExpectedNumericReverse,
                        listExpectedNumericTfValues, false);
    }
    
    @Test
    public void testHandlerListNormalizedNumericWithSpace() throws Exception {
        
        ctx.getConfiguration().set("test" + "." + NUMERIC_LIST + BaseIngestHelper.FIELD_TYPE, NumberType.class.getName());
        ctx.getConfiguration().set("test" + "." + ContentBaseIngestHelper.LIST_DELIMITERS, LIST_DELIMITERS);
        
        TypeRegistry.reset();
        TypeRegistry.getInstance(ctx.getConfiguration());
        
        setupMocks();
        
        TestContentIndexingColumnBasedHandler handler = new TestContentIndexingColumnBasedHandler();
        handler.setup(ctx);
        
        helper.setup(ctx.getConfiguration());
        
        testProcessing(handler, NUMERIC_LIST, LIST_VALUE_WITH_SPACE, listExpectedNumericFields, listExpectedNumericIndex, listExpectedNumericReverse,
                        listExpectedNumericTfValues, false);
    }
    
    @Test
    public void testHandlerListNormalizedAlphanumNoSpace() throws Exception {
        
        ctx.getConfiguration().set("test" + "." + ALPHANUM_LIST + BaseIngestHelper.FIELD_TYPE, LcNoDiacriticsType.class.getName());
        ctx.getConfiguration().set("test" + "." + ContentBaseIngestHelper.LIST_DELIMITERS, LIST_DELIMITERS);
        
        TypeRegistry.reset();
        TypeRegistry.getInstance(ctx.getConfiguration());
        
        setupMocks();
        
        TestContentIndexingColumnBasedHandler handler = new TestContentIndexingColumnBasedHandler();
        handler.setup(ctx);
        
        helper.setup(ctx.getConfiguration());
        
        testProcessing(handler, ALPHANUM_LIST, LIST_VALUE, listExpectedAlpahnumFields, listExpectedAlpahnumIndex, listExpectedAlpahnumReverse,
                        listExpectedAlphanumTfValues, false);
    }
    
    @Test
    public void testHandlerListNormalizedAlphanumWithSpace() throws Exception {
        
        ctx.getConfiguration().set("test" + "." + ALPHANUM_LIST + BaseIngestHelper.FIELD_TYPE, LcNoDiacriticsType.class.getName());
        ctx.getConfiguration().set("test" + "." + ContentBaseIngestHelper.LIST_DELIMITERS, LIST_DELIMITERS);
        
        TypeRegistry.reset();
        TypeRegistry.getInstance(ctx.getConfiguration());
        
        setupMocks();
        
        TestContentIndexingColumnBasedHandler handler = new TestContentIndexingColumnBasedHandler();
        handler.setup(ctx);
        
        helper.setup(ctx.getConfiguration());
        
        testProcessing(handler, ALPHANUM_LIST, LIST_VALUE_WITH_SPACE, listExpectedAlpahnumFields, listExpectedAlpahnumIndex, listExpectedAlpahnumReverse,
                        listExpectedAlphanumTfValues, false);
    }
    
    @Test
    public void testHandlerListNormalizedAlphanumWithEmptyEntry() throws Exception {
        
        ctx.getConfiguration().set("test" + "." + ALPHANUM_LIST + BaseIngestHelper.FIELD_TYPE, LcNoDiacriticsType.class.getName());
        ctx.getConfiguration().set("test" + "." + ContentBaseIngestHelper.LIST_DELIMITERS, LIST_DELIMITERS);
        
        TypeRegistry.reset();
        TypeRegistry.getInstance(ctx.getConfiguration());
        
        setupMocks();
        
        TestContentIndexingColumnBasedHandler handler = new TestContentIndexingColumnBasedHandler();
        handler.setup(ctx);
        
        helper.setup(ctx.getConfiguration());
        
        testProcessing(handler, ALPHANUM_LIST, LIST_VALUE_WITH_EMPTY_ENTRY, listExpectedAlpahnumFields, listExpectedAlpahnumIndex, listExpectedAlpahnumReverse,
                        listExpectedAlphanumTfValues, false);
    }
    
    private boolean equalNciMaps(Multimap<String,NormalizedContentInterface> first, Multimap<String,NormalizedContentInterface> second) {
        Multimap<String,NormalizedContentInterface> firstToSecondDiff = Multimaps.filterEntries(first, e -> !second.containsEntry(e.getKey(), e.getValue()));
        Multimap<String,NormalizedContentInterface> secondToFirstDiff = Multimaps.filterEntries(first, e -> !second.containsEntry(e.getKey(), e.getValue()));
        
        if (firstToSecondDiff.isEmpty() && secondToFirstDiff.isEmpty()) {
            return true;
        } else {
            return false;
        }
    }
    
    private void testProcessing(TestContentIndexingColumnBasedHandler handler, String fieldname, String valueList,
                    Multimap<String,NormalizedContentInterface> expectedFields, Multimap<String,NormalizedContentInterface> expectedIndex,
                    Multimap<String,NormalizedContentInterface> expectedReverse, Multimap<String,Pair<String,Integer>> expectedTfValues, boolean tokenTest)
                    throws Exception {
        
        NormalizedContentInterface field = new NormalizedFieldAndValue(fieldname, valueList);
        
        if (tokenTest) {
            Analyzer analyzer = handler.tokenHelper.getAnalyzer();
            handler.tokenizeField(analyzer, field, true, true, null);
        } else {
            handler.indexListEntries(field, true, true, null);
        }
        
        Assert.assertTrue("Actual fields results do not match expected.\n Expected: " + expectedFields.toString() + "\nActual: "
                        + handler.getFields().toString(), equalNciMaps(expectedFields, handler.getFields()));
        Assert.assertTrue("Actual index results do not match expected\n Expected: " + expectedIndex.toString() + "\nActual: " + handler.getIndex().toString(),
                        equalNciMaps(expectedIndex, handler.getIndex()));
        Assert.assertTrue("Actual reverse results do not match expected\n Expected: " + expectedReverse.toString() + "\nActual: "
                        + handler.getReverse().toString(), equalNciMaps(expectedReverse, handler.getReverse()));
        
        // test t entries
        handler.shardId = SHARD_ID.getBytes();
        handler.eventDataTypeName = TEST_TYPE;
        handler.eventUid = TEST_UUID;
        Multimap<BulkIngestKey,Value> tfEntries = HashMultimap.create();
        handler.flushTokenOffsetCache(event, tfEntries);
        
        StringBuilder errorMessage = new StringBuilder();
        boolean found = assertExpectedTfRecords(expectedTfValues, tfEntries, errorMessage);
        Assert.assertTrue(errorMessage.toString() + "\nActual" + tfEntries.toString(), found);
        
        errorMessage = new StringBuilder();
        found = assertExpectedCountTfRecord(expectedTfValues, tfEntries, errorMessage);
        Assert.assertTrue(errorMessage.toString() + "\nActual" + tfEntries.toString(), found);
    }
    
    private boolean assertExpectedTfRecords(Multimap<String,Pair<String,Integer>> expectedTfValues, Multimap<BulkIngestKey,Value> tfEntries,
                    StringBuilder errorMessage) {
        for (Map.Entry<String,Pair<String,Integer>> entry : expectedTfValues.entries()) {
            Text expectedColf = new Text(TF);
            
            Text expectedColq = new Text(TEST_TYPE + INTRA_COL_DELIMETER + TEST_UUID + INTRA_COL_DELIMETER + entry.getValue().getLeft() + INTRA_COL_DELIMETER
                            + entry.getKey());
            int count = entry.getValue().getRight().intValue();
            byte[] expectedValue = {(byte) 24, (byte) count};
            
            Collection<Value> values = assertContainsKey(SHARD_ID, SHARD_TABLE_NAME, expectedColf, expectedColq, tfEntries);
            if (values != null) {
                for (Value value : values) {
                    if (Arrays.equals(value.get(), expectedValue)) {
                        return true;
                    }
                }
                errorMessage.append("Expected TF record ").append(expectedColf).append(":").append(expectedColq).append(" with value: ").append(count)
                                .append(" not found.");
                return false;
            }
        }
        
        errorMessage.append("No expected TF records found. Expected: ").append(expectedTfValues.toString());
        return false;
    }
    
    private boolean assertExpectedCountTfRecord(Multimap<String,Pair<String,Integer>> expectedTfValues, Multimap<BulkIngestKey,Value> tfEntries,
                    StringBuilder errorMessage) {
        int count = expectedTfValues.size();
        Text expectedColf = new Text(TEST_TYPE + INTRA_COL_DELIMETER + TEST_UUID);
        Text expectedColq = new Text("TERM_COUNT" + INTRA_COL_DELIMETER + count);
        
        Collection<Value> values = assertContainsKey(SHARD_ID, SHARD_TABLE_NAME, expectedColf, expectedColq, tfEntries);
        if (values != null) {
            return true;
        } else {
            errorMessage.append("Expected TF Count record ").append(expectedColf).append(":").append(expectedColq).append(" not found.");
            return false;
        }
    }
    
    private Collection<Value> assertContainsKey(String rowId, Text tableName, Text expectedColf, Text expectedColq, Multimap<BulkIngestKey,Value> tfEntries) {
        BulkIngestKey expectedBulkIngestKey = createExpectedBulkIngestKey(tableName, rowId, expectedColf, expectedColq);
        return getCorrespondingValue(tfEntries, expectedBulkIngestKey);
    }
    
    private Collection<Value> getCorrespondingValue(Multimap<BulkIngestKey,Value> entries, BulkIngestKey expectedBulkIngestKey) {
        for (BulkIngestKey actualBulkIngestKey : entries.keySet()) {
            if (actualBulkIngestKey.getTableName().equals(expectedBulkIngestKey.getTableName())
                            && actualBulkIngestKey.getKey().equals(expectedBulkIngestKey.getKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS)) {
                return entries.get(actualBulkIngestKey);
            }
        }
        return null;
    }
    
    private BulkIngestKey createExpectedBulkIngestKey(Text tableName, String rowId, Text columnFamily, Text columnQualifier) {
        Key k = new Key(new Text(rowId), columnFamily, columnQualifier);
        return new BulkIngestKey(new Text(tableName), k);
    }
    
    public static class TestContentIndexingColumnBasedHandler extends ContentIndexingColumnBasedHandler<Text> {
        
        @Override
        public ContentBaseIngestHelper getContentIndexingDataTypeHelper() {
            return (ContentBaseIngestHelper) this.helper;
        }
        
        Multimap<String,NormalizedContentInterface> getFields() {
            return fields;
        }
        
        Multimap<String,NormalizedContentInterface> getIndex() {
            return index;
        }
        
        Multimap<String,NormalizedContentInterface> getReverse() {
            return reverse;
        }
        
    }
    
    public static class TestContentBaseIngestHelper extends ContentBaseIngestHelper {
        @Override
        public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer event) {
            Multimap<String,NormalizedContentInterface> fields = HashMultimap.create();
            return fields;
        }
    }
    
    public static class TestEventRecordReader implements EventRecordReader {
        
        @Override
        public void initializeEvent(Configuration conf) throws IOException {}
        
        @Override
        public RawRecordContainer getEvent() {
            return new RawRecordContainerImpl();
        }
        
        @Override
        public String getRawInputFileName() {
            return "";
        }
        
        @Override
        public long getRawInputFileTimestamp() {
            return 0;
        };
        
        @Override
        public RawRecordContainer enforcePolicy(RawRecordContainer event) {
            return event;
        }
        
        @Override
        public void setInputDate(long time) {}
        
    }
    
    public static class TestIngestPolicyEnforcer extends IngestPolicyEnforcer {
        @Override
        public void validate(RawRecordContainer event) {}
    }
}
