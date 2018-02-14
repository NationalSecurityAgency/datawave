package datawave.ingest.mapreduce.handler.shard;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.data.type.GeometryType;
import datawave.data.type.NoOpType;
import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.BaseNormalizedContent;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.mapreduce.handler.DataTypeHandler;
import datawave.ingest.mapreduce.handler.MockStatusReporter;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.table.config.ShardTableConfigHelper;
import datawave.ingest.table.config.TableConfigHelper;
import datawave.policy.IngestPolicyEnforcer;
import io.protostuff.ProtobufIOUtil;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.util.StringUtils;

import java.text.SimpleDateFormat;
import java.util.Collection;

public class ShardedDataTypeHandlerTest {
    private static final int NUM_SHARDS = 241;
    private static final String SHARD_TABLE_NAME = "shard";
    private static final String KNOWLEDGE_SHARD_TABLE_NAME = "knowledgeShard";
    private static final String ERROR_SHARD_TABLE_NAME = "errorShard";
    private static final String SHARD_INDEX_TABLE_NAME = "shardIndex";
    private static final String SHARD_REVERSE_INDEX_TABLE_NAME = "shardReverseIndex";
    private static final String METADATA_TABLE_NAME = "DatawaveMetadata";
    
    private static final String FIELD_NAME = "TEST_FIELD";
    private static final String[] MAPPED_FIELDS = new String[] {"FIELD_LENGTH, OTHER_FIELD"};
    private static final String DATA_TYPE = "someType";
    
    private static final String AUTHS = "ALL";
    
    private static final String formatPattern = "yyyyMMdd HHmmss.SSS";
    private static final SimpleDateFormat formatter = new SimpleDateFormat(formatPattern);
    
    private static final String BEGIN_DATE = "20000101 000000.000";
    
    private static Configuration conf = null;
    
    public static class TestIngestHelper extends BaseIngestHelper {
        StringBuilder sb = new StringBuilder();
        
        @Override
        public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer record) {
            Multimap<String,NormalizedContentInterface> eventFields = HashMultimap.create();
            NormalizedContentInterface nci = new BaseNormalizedContent();
            nci.setFieldName(FIELD_NAME);
            nci.setEventFieldValue(new String(record.getRawData()));
            nci.setIndexedFieldValue(new String(record.getRawData()));
            eventFields.put(FIELD_NAME, nci);
            
            sb.setLength(0);
            sb.append(record.getRawData().length);
            
            NormalizedFieldAndValue nfv = new NormalizedFieldAndValue("FIELD_LENGTH", sb.toString());
            eventFields.put(nfv.getEventFieldName(), nfv);
            return normalizeMap(eventFields);
        }
    }
    
    public static class TestShardedDataTypeHandler extends ShardedDataTypeHandler {
        
        private Multimap<String,NormalizedContentInterface> eventFields;
        
        public TestShardedDataTypeHandler(Multimap<String,NormalizedContentInterface> eventFields) {
            this.eventFields = eventFields;
        }
        
        @Override
        protected Multimap<String,NormalizedContentInterface> getShardNamesAndValues(RawRecordContainer event, Multimap eventFields,
                        boolean createGlobalIndexTerms, boolean createGlobalReverseIndexTerms, StatusReporter reporter) {
            return eventFields;
        }
        
        @Override
        protected Multimap<String,NormalizedContentInterface> getGlobalIndexTerms() {
            Multimap<String,NormalizedContentInterface> indexTerms = HashMultimap.create();
            indexTerms.putAll(FIELD_NAME, eventFields.get(FIELD_NAME));
            return indexTerms;
        }
        
        @Override
        protected Multimap<String,NormalizedContentInterface> getGlobalReverseIndexTerms() {
            Multimap<String,NormalizedContentInterface> indexTerms = HashMultimap.create();
            indexTerms.putAll(FIELD_NAME, eventFields.get(FIELD_NAME));
            return indexTerms;
        }
        
        @Override
        protected boolean hasIndexTerm(String fieldName) {
            return (fieldName.equals(FIELD_NAME));
        }
        
        @Override
        protected boolean hasReverseIndexTerm(String fieldName) {
            return fieldName.equals(FIELD_NAME);
        }
        
        @Override
        public IngestHelperInterface getHelper(Type type) {
            TestIngestHelper ingestHelper = new TestIngestHelper();
            ingestHelper.setup(conf);
            return ingestHelper;
        }
    }
    
    @Before
    public void setup() {
        conf = new Configuration();
        conf.set(DataTypeHelper.Properties.DATA_NAME, DATA_TYPE);
        conf.set(DATA_TYPE + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS, IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
        conf.set(DATA_TYPE + TypeRegistry.INGEST_HELPER, TestIngestHelper.class.getName());
        conf.set(DATA_TYPE + BaseIngestHelper.DEFAULT_TYPE, NoOpType.class.getName());
        conf.set(DATA_TYPE + "." + FIELD_NAME + BaseIngestHelper.FIELD_TYPE, GeometryType.class.getName());
        conf.set(ShardedDataTypeHandler.NUM_SHARDS, Integer.toString(NUM_SHARDS));
        conf.set(ShardedDataTypeHandler.METADATA_TABLE_NAME, METADATA_TABLE_NAME);
        conf.set(ShardedDataTypeHandler.SHARDED_TNAMES, SHARD_TABLE_NAME + "," + KNOWLEDGE_SHARD_TABLE_NAME + "," + ERROR_SHARD_TABLE_NAME);
        conf.set(ShardedDataTypeHandler.SHARD_TNAME, SHARD_TABLE_NAME);
        conf.set(ShardedDataTypeHandler.SHARD_GIDX_TNAME, SHARD_INDEX_TABLE_NAME);
        conf.set(ShardedDataTypeHandler.SHARD_GIDX_LPRIORITY, Integer.toString(30));
        conf.set(SHARD_INDEX_TABLE_NAME + TableConfigHelper.TABLE_CONFIG_CLASS_SUFFIX, ShardTableConfigHelper.class.getName());
        conf.set(ShardedDataTypeHandler.SHARD_GRIDX_TNAME, SHARD_REVERSE_INDEX_TABLE_NAME);
        conf.set(ShardedDataTypeHandler.SHARD_GRIDX_LPRIORITY, Integer.toString(30));
        conf.set(SHARD_REVERSE_INDEX_TABLE_NAME + TableConfigHelper.TABLE_CONFIG_CLASS_SUFFIX, ShardTableConfigHelper.class.getName());
        TypeRegistry.reset();
        TypeRegistry.getInstance(conf);
    }
    
    @Test
    public void testFieldIndexFilterEnabled() throws Exception {
        conf.set(ShardedDataTypeHandler.SHARD_FIELD_INDEX_FILTER_ENABLED, Boolean.toString(true));
        conf.set(DATA_TYPE + "." + FIELD_NAME + BaseIngestHelper.FIELD_INDEX_FILTER_MAPPING, StringUtils.arrayToCommaDelimitedString(MAPPED_FIELDS));
        
        String fieldValue = "this is the field value";
        
        RawRecordContainer record = new RawRecordContainerImpl();
        record.clear();
        record.setDataType(new Type(DATA_TYPE, TestIngestHelper.class, (Class) null, (String[]) null, 1, (String[]) null));
        record.setRawFileName("someData.dat");
        record.setRawRecordNumber(1);
        record.setDate(formatter.parse(BEGIN_DATE).getTime());
        record.setRawDataAndGenerateId(fieldValue.getBytes("UTF8"));
        record.setVisibility(new ColumnVisibility(AUTHS));
        
        TestIngestHelper helper = new TestIngestHelper();
        helper.setup(conf);
        
        final Multimap<String,NormalizedContentInterface> eventFields = helper.getEventFields(record);
        
        ShardedDataTypeHandler dataTypeHandler = new TestShardedDataTypeHandler(eventFields);
        dataTypeHandler.setup(new TaskAttemptContextImpl(conf, new TaskAttemptID()));
        
        Assert.assertTrue(dataTypeHandler.isFieldIndexFilterEnabled());
        
        Multimap<BulkIngestKey,Value> kvPairs = dataTypeHandler.processBulk(new Text(), record, eventFields, new MockStatusReporter());
        boolean bikFound = false;
        for (BulkIngestKey bik : kvPairs.keySet()) {
            if (bik.getTableName().toString().equals(SHARD_TABLE_NAME) && bik.getKey().getColumnFamily().toString().startsWith("fi")) {
                Collection<Value> values = kvPairs.get(bik);
                Assert.assertEquals(1, values.size());
                
                Value value = values.iterator().next();
                Assert.assertNotNull(value.get());
                Assert.assertTrue(value.get().length > 0);
                
                FieldIndexData fieldIndexData = new FieldIndexData();
                ProtobufIOUtil.mergeFrom(value.get(), fieldIndexData, FieldIndexData.SCHEMA);
                
                Collection<String> fieldValues = fieldIndexData.getFilterData().getFieldValueMapping().get("FIELD_LENGTH");
                Assert.assertEquals(1, fieldValues.size());
                Assert.assertEquals(Integer.toString(fieldValue.length()), fieldValues.iterator().next());
                bikFound = true;
            }
        }
        Assert.assertTrue(bikFound);
    }
    
    @Test
    public void testFieldIndexFilterDisabled() throws Exception {
        conf.set(ShardedDataTypeHandler.SHARD_FIELD_INDEX_FILTER_ENABLED, Boolean.toString(false));
        conf.set(DATA_TYPE + "." + FIELD_NAME + BaseIngestHelper.FIELD_INDEX_FILTER_MAPPING, StringUtils.arrayToCommaDelimitedString(MAPPED_FIELDS));
        
        String fieldValue = "this is the field value";
        
        RawRecordContainer record = new RawRecordContainerImpl();
        record.clear();
        record.setDataType(new Type(DATA_TYPE, TestIngestHelper.class, (Class) null, (String[]) null, 1, (String[]) null));
        record.setRawFileName("someData.dat");
        record.setRawRecordNumber(1);
        record.setDate(formatter.parse(BEGIN_DATE).getTime());
        record.setRawDataAndGenerateId(fieldValue.getBytes("UTF8"));
        record.setVisibility(new ColumnVisibility(AUTHS));
        
        TestIngestHelper helper = new TestIngestHelper();
        helper.setup(conf);
        
        final Multimap<String,NormalizedContentInterface> eventFields = helper.getEventFields(record);
        
        ShardedDataTypeHandler dataTypeHandler = new TestShardedDataTypeHandler(eventFields);
        dataTypeHandler.setup(new TaskAttemptContextImpl(conf, new TaskAttemptID()));
        
        Assert.assertFalse(dataTypeHandler.isFieldIndexFilterEnabled());
        
        Multimap<BulkIngestKey,Value> kvPairs = dataTypeHandler.processBulk(new Text(), record, eventFields, new MockStatusReporter());
        boolean bikFound = false;
        for (BulkIngestKey bik : kvPairs.keySet()) {
            if (bik.getTableName().toString().equals(SHARD_TABLE_NAME) && bik.getKey().getColumnFamily().toString().startsWith("fi")) {
                Collection<Value> values = kvPairs.get(bik);
                Assert.assertEquals(1, values.size());
                
                Value value = values.iterator().next();
                Assert.assertEquals(DataTypeHandler.NULL_VALUE, value.get());
                bikFound = true;
            }
        }
        Assert.assertTrue(bikFound);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testSetup() {
        Configuration conf = new Configuration();
        ShardedDataTypeHandler<Text> handler = new AbstractColumnBasedHandler<>();
        handler.setup(new TaskAttemptContextImpl(conf, new TaskAttemptID()));
    }
}
