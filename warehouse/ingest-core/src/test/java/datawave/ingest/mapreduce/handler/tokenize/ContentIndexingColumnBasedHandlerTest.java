package datawave.ingest.mapreduce.handler.tokenize;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
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
import datawave.policy.IngestPolicyEnforcer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.lucene.analysis.Analyzer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class ContentIndexingColumnBasedHandlerTest {
    
    private Configuration conf = null;
    private TaskAttemptContext ctx = null;
    private final String LIST_DELIMITERS = ",;";
    private static final String TOKEN_DESIGNATOR = "_TOKEN";
    
    private static final String NUMERIC_LIST = "NUMERIC_LIST";
    private static final String ALPHANUM_LIST = "APLHANUM_LIST";
    private static final String LIST_VALUE = "12.34,56.78";
    
    public static String[] tokenizeAlphanumResults = {"12", "34", "56", "78"};
    public static String[] tokenizeAlphanumReverseResults = {"21", "43", "65", "87"};
    public static String[] listAlphanumResults = {"12.34", "56.78"};
    public static String[] listAlphanumReverseResults = {"43.21", "87.65"};
    public static String[] listNumericResults = {"+bE1.234", "+bE5.678"};
    public static String[] listNumericReverseResults = {"+bE4.321", "+bE8.765"};
    
    public static Multimap<String,NormalizedContentInterface> tokenizedExpectedFields = HashMultimap.create();
    public static Multimap<String,NormalizedContentInterface> tokenizedExpectedIndex = HashMultimap.create();
    public static Multimap<String,NormalizedContentInterface> tokenizedExpectedReverse = HashMultimap.create();
    
    public static Multimap<String,NormalizedContentInterface> listExpectedNumericFields = HashMultimap.create();
    public static Multimap<String,NormalizedContentInterface> listExpectedNumericIndex = HashMultimap.create();
    public static Multimap<String,NormalizedContentInterface> listExpectedNumericReverse = HashMultimap.create();
    
    public static Multimap<String,NormalizedContentInterface> listExpectedAlpahnumFields = HashMultimap.create();
    public static Multimap<String,NormalizedContentInterface> listExpectedAlpahnumIndex = HashMultimap.create();
    public static Multimap<String,NormalizedContentInterface> listExpectedAlpahnumReverse = HashMultimap.create();
    
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
        
    }
    
    @BeforeClass
    public static void setUpExpectedResults() {
        SetExpectedMap(ALPHANUM_LIST + TOKEN_DESIGNATOR, tokenizeAlphanumResults, tokenizeAlphanumReverseResults, tokenizedExpectedFields,
                        tokenizedExpectedIndex, tokenizedExpectedReverse);
        SetExpectedMap(ALPHANUM_LIST, listAlphanumResults, listAlphanumReverseResults, listExpectedAlpahnumFields, listExpectedAlpahnumIndex,
                        listExpectedAlpahnumReverse);
        SetExpectedMap(NUMERIC_LIST, listNumericResults, listNumericReverseResults, listExpectedNumericFields, listExpectedNumericIndex,
                        listExpectedNumericReverse);
    }
    
    public static void SetExpectedMap(String fieldName, String[] expectedResults, String[] expectedReversResults,
                    Multimap<String,NormalizedContentInterface> fields, Multimap<String,NormalizedContentInterface> index,
                    Multimap<String,NormalizedContentInterface> reverse) {
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
        }
    }
    
    @Test
    public void testHandlerNormalizedTokenizedField() throws Exception {
        
        TestContentIndexingColumnBasedHandler handler = new TestContentIndexingColumnBasedHandler();
        
        TypeRegistry.reset();
        TypeRegistry.getInstance(ctx.getConfiguration());
        
        handler.setup(ctx);
        
        TestContentBaseIngestHelper helper = new TestContentBaseIngestHelper();
        helper.setup(ctx.getConfiguration());
        
        Analyzer analyzer = handler.tokenHelper.getAnalyzer();
        
        NormalizedContentInterface field = new NormalizedFieldAndValue(ALPHANUM_LIST, LIST_VALUE);
        handler.tokenizeField(analyzer, field, true, true, null);
        
        Assert.assertTrue("Actual fields results do not match expected.\n Expected: " + tokenizedExpectedFields.toString() + "\nActual: "
                        + handler.getFields().toString(), equalNciMaps(tokenizedExpectedFields, handler.getFields()));
        Assert.assertTrue("Actual index results do not match expected\n Expected: " + tokenizedExpectedIndex.toString() + "\nActual: "
                        + handler.getIndex().toString(), equalNciMaps(tokenizedExpectedIndex, handler.getIndex()));
        Assert.assertTrue("Actual reverse results do not match expected\n Expected: " + tokenizedExpectedReverse.toString() + "\nActual: "
                        + handler.getReverse().toString(), equalNciMaps(tokenizedExpectedReverse, handler.getReverse()));
        
    }
    
    @Test
    public void testHandlerListNormalizedNumbers() {
        
        ctx.getConfiguration().set("test" + "." + NUMERIC_LIST + BaseIngestHelper.FIELD_TYPE, NumberType.class.getName());
        ctx.getConfiguration().set("test" + "." + ContentBaseIngestHelper.LIST_DELIMITERS, LIST_DELIMITERS);
        
        TestContentIndexingColumnBasedHandler handler = new TestContentIndexingColumnBasedHandler();
        
        TypeRegistry.reset();
        TypeRegistry.getInstance(ctx.getConfiguration());
        
        handler.setup(ctx);
        
        TestContentBaseIngestHelper helper = new TestContentBaseIngestHelper();
        helper.setup(ctx.getConfiguration());
        
        NormalizedContentInterface field = new NormalizedFieldAndValue(NUMERIC_LIST, LIST_VALUE);
        handler.indexListEntries(field, true, true, null);
        
        Assert.assertTrue("Actual fields results do not match expected.\n Expected: " + listExpectedNumericFields.toString() + "\nActual: "
                        + handler.getFields().toString(), equalNciMaps(listExpectedNumericFields, handler.getFields()));
        Assert.assertTrue("Actual index results do not match expected\n Expected: " + listExpectedNumericIndex.toString() + "\nActual: "
                        + handler.getIndex().toString(), equalNciMaps(listExpectedNumericIndex, handler.getIndex()));
        Assert.assertTrue("Actual reverse results do not match expected\n Expected: " + listExpectedNumericReverse.toString() + "\nActual: "
                        + handler.getReverse().toString(), equalNciMaps(listExpectedNumericReverse, handler.getReverse()));
    }
    
    @Test
    public void testHandlerListNormalizedAlphanum() {
        
        ctx.getConfiguration().set("test" + "." + ALPHANUM_LIST + BaseIngestHelper.FIELD_TYPE, LcNoDiacriticsType.class.getName());
        ctx.getConfiguration().set("test" + "." + ContentBaseIngestHelper.LIST_DELIMITERS, LIST_DELIMITERS);
        
        TestContentIndexingColumnBasedHandler handler = new TestContentIndexingColumnBasedHandler();
        
        TypeRegistry.reset();
        TypeRegistry.getInstance(ctx.getConfiguration());
        
        handler.setup(ctx);
        
        TestContentBaseIngestHelper helper = new TestContentBaseIngestHelper();
        helper.setup(ctx.getConfiguration());
        
        NormalizedContentInterface field = new NormalizedFieldAndValue(ALPHANUM_LIST, LIST_VALUE);
        handler.indexListEntries(field, true, true, null);
        
        Assert.assertTrue("Actual fields results do not match expected.\n Expected: " + listExpectedAlpahnumFields.toString() + "\nActual: "
                        + handler.getFields().toString(), equalNciMaps(listExpectedAlpahnumFields, handler.getFields()));
        Assert.assertTrue("Actual index results do not match expected\n Expected: " + listExpectedAlpahnumIndex.toString() + "\nActual: "
                        + handler.getIndex().toString(), equalNciMaps(listExpectedAlpahnumIndex, handler.getIndex()));
        Assert.assertTrue("Actual reverse results do not match expected\n Expected: " + listExpectedAlpahnumReverse.toString() + "\nActual: "
                        + handler.getReverse().toString(), equalNciMaps(listExpectedAlpahnumReverse, handler.getReverse()));
    }
    
    boolean equalNciMaps(Multimap<String,NormalizedContentInterface> first, Multimap<String,NormalizedContentInterface> second) {
        Multimap<String,NormalizedContentInterface> firstToSecondDiff = Multimaps.filterEntries(first, e -> !second.containsEntry(e.getKey(), e.getValue()));
        Multimap<String,NormalizedContentInterface> secondToFirstDiff = Multimaps.filterEntries(first, e -> !second.containsEntry(e.getKey(), e.getValue()));
        
        if (firstToSecondDiff.isEmpty() && secondToFirstDiff.isEmpty()) {
            return true;
        } else {
            return false;
        }
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
