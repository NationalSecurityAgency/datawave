package datawave.ingest.mapreduce.handler.shard;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.data.type.GeometryType;
import datawave.data.type.NumberType;
import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.ingest.data.config.MaskedFieldHelper;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.ingest.data.config.ingest.ContentBaseIngestHelper;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.partition.BalancedShardPartitioner;
import datawave.ingest.protobuf.Uid;
import datawave.ingest.table.config.ShardTableConfigHelper;
import datawave.ingest.table.config.TableConfigHelper;
import datawave.policy.IngestPolicyEnforcer;
import datawave.util.TableName;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertTrue;

public class ShardedDataTypeHandlerTest {
    
    ShardedDataTypeHandler<Text> handler;
    AbstractColumnBasedHandler<Text> dataTypeHandler;
    TestIngestHelper ingestHelper;
    TestMaskedHelper maskedFieldHelper;
    private static final int NUM_SHARDS = 241;
    private static final String DATA_TYPE_NAME = "wkt";
    private static final String WKT_BYTE_LENGTH_FIELD = "WKT_BYTE_LENGTH";
    private static final String GEO_FIELD = "GEO";
    private static final String INGEST_HELPER_CLASS = TestIngestHelper.class.getName();
    
    Configuration configuration;
    
    public static class TestIngestHelper extends ContentBaseIngestHelper {
        @Override
        public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer record) {
            Multimap<String,NormalizedContentInterface> eventFields = HashMultimap.create();
            String rawRecord = new String(record.getRawData());
            for (String value : rawRecord.split(";")) {
                NormalizedContentInterface geo_nci = new NormalizedFieldAndValue("FIELD_NAME", value);
                eventFields.put("FIELD_NAME", geo_nci);
            }
            return normalizeMap(eventFields);
        }
        
        public String getNormalizedMaskedValue(final String key) {
            return "FIELD_NAME";
        }
    }
    
    public static class TestMaskedHelper implements MaskedFieldHelper {
        @Override
        public void setup(Configuration config) {
            
        }
        
        @Override
        public boolean hasMappings() {
            return true;
        }
        
        @Override
        public boolean contains(String key) {
            return true;
        }
        
        @Override
        public String get(String key) {
            return "FIELD_NAME";
        }
    }
    
    public static void setupConfiguration(Configuration conf) {
        String compositeFieldName = GEO_FIELD;
        conf.set(DATA_TYPE_NAME + "." + compositeFieldName + BaseIngestHelper.COMPOSITE_FIELD_MAP, GEO_FIELD + "," + WKT_BYTE_LENGTH_FIELD);
        conf.set(DATA_TYPE_NAME + "." + compositeFieldName + BaseIngestHelper.COMPOSITE_FIELD_SEPARATOR, " ");
        conf.set(DATA_TYPE_NAME + BaseIngestHelper.INDEX_FIELDS, GEO_FIELD + ((!compositeFieldName.equals(GEO_FIELD)) ? "," + compositeFieldName : ""));
        conf.set(DATA_TYPE_NAME + "." + GEO_FIELD + BaseIngestHelper.FIELD_TYPE, GeometryType.class.getName());
        conf.set(DATA_TYPE_NAME + "." + WKT_BYTE_LENGTH_FIELD + BaseIngestHelper.FIELD_TYPE, NumberType.class.getName());
        
        conf.set(DATA_TYPE_NAME + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS, IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
        conf.set(DataTypeHelper.Properties.DATA_NAME, DATA_TYPE_NAME);
        conf.set(TypeRegistry.INGEST_DATA_TYPES, DATA_TYPE_NAME);
        conf.set(DATA_TYPE_NAME + TypeRegistry.INGEST_HELPER, INGEST_HELPER_CLASS);
        
        conf.set(ShardedDataTypeHandler.METADATA_TABLE_NAME, TableName.METADATA);
        conf.set(ShardedDataTypeHandler.NUM_SHARDS, Integer.toString(NUM_SHARDS));
        conf.set(ShardedDataTypeHandler.SHARDED_TNAMES, TableName.SHARD + "," + TableName.ERROR_SHARD);
        conf.set(ShardedDataTypeHandler.SHARD_TNAME, TableName.SHARD);
        conf.set(ShardedDataTypeHandler.SHARD_LPRIORITY, "30");
        conf.set(TableName.SHARD + TableConfigHelper.TABLE_CONFIG_CLASS_SUFFIX, ShardTableConfigHelper.class.getName());
        conf.set(ShardedDataTypeHandler.SHARD_GIDX_TNAME, TableName.SHARD_INDEX);
        conf.set(ShardedDataTypeHandler.SHARD_GIDX_LPRIORITY, "30");
        conf.set(TableName.SHARD_INDEX + TableConfigHelper.TABLE_CONFIG_CLASS_SUFFIX, ShardTableConfigHelper.class.getName());
        conf.set(ShardedDataTypeHandler.SHARD_GRIDX_TNAME, TableName.SHARD_RINDEX);
        conf.set(ShardedDataTypeHandler.SHARD_GRIDX_LPRIORITY, "30");
        conf.set(TableName.SHARD_RINDEX + TableConfigHelper.TABLE_CONFIG_CLASS_SUFFIX, ShardTableConfigHelper.class.getName());
        conf.set(ShardTableConfigHelper.MARKINGS_SETUP_ITERATOR_ENABLED, "false");
        conf.set(ShardTableConfigHelper.MARKINGS_SETUP_ITERATOR_CONFIG, "");
        conf.set("partitioner.category.shardedTables", BalancedShardPartitioner.class.getName());
        conf.set("partitioner.category.member." + TableName.SHARD, "shardedTables");
    }
    
    @Before
    public void setUp() throws Exception {
        configuration = new Configuration();
        setupConfiguration(configuration);
        handler = new AbstractColumnBasedHandler<>();
        handler.setup(new TaskAttemptContextImpl(configuration, new TaskAttemptID()));
        handler.setShardIndexTableName(new Text("shardIndex"));
        handler.setShardReverseIndexTableName(new Text("shardReverseIndex"));
        
        dataTypeHandler = new AbstractColumnBasedHandler<>();
        dataTypeHandler.setup(new TaskAttemptContextImpl(configuration, new TaskAttemptID()));
        
        ingestHelper = new TestIngestHelper();
        ingestHelper.setup(configuration);
        
        maskedFieldHelper = new TestMaskedHelper();
        maskedFieldHelper.setup(configuration);
    }
    
    @Test
    public void testCreateTermIndex() {
        Type dataType = new Type(DATA_TYPE_NAME, TestIngestHelper.class, null, null, 10, null);
        String entry = "testingtesting";
        RawRecordContainer record = new RawRecordContainerImpl();
        record.setDataType(dataType);
        record.setRawFileName("data_" + 0 + ".dat");
        record.setRawRecordNumber(1);
        record.setRawData(entry.getBytes(StandardCharsets.UTF_8));
        
        Uid.List uid = Uid.List.newBuilder().setIGNORE(false).setCOUNT(1).addUID("d8zay2.-3pnndm.-anolok").build();
        byte[] visibility = new byte[] {65, 76, 76};
        byte[] shardId = new byte[] {50, 48, 48, 48, 48, 49, 48, 49, 95, 54, 57};
        
        Multimap<BulkIngestKey,Value> termIndex = handler.createTermIndexColumn(record, "FIELD_NAME", "FIELD_VALUE", visibility, null, null, shardId,
                        handler.getShardIndexTableName(), new Value(uid.toByteArray()), Direction.FORWARD);
        
        assertTrue(termIndex.size() == 1);
    }
    
    @Test
    public void testCreateTermReverseIndex() {
        Type dataType = new Type(DATA_TYPE_NAME, TestIngestHelper.class, null, null, 10, null);
        String entry = "testingtesting";
        RawRecordContainer record = new RawRecordContainerImpl();
        record.setDataType(dataType);
        record.setRawFileName("data_" + 0 + ".dat");
        record.setRawRecordNumber(1);
        record.setRawData(entry.getBytes(StandardCharsets.UTF_8));
        
        Uid.List uid = Uid.List.newBuilder().setIGNORE(false).setCOUNT(1).addUID("d8zay2.-3pnndm.-anolok").build();
        byte[] visibility = new byte[] {65, 76, 76};
        byte[] maskVisibility = new byte[] {67, 76, 76};
        byte[] shardId = new byte[] {50, 48, 48, 48, 48, 49, 48, 49, 95, 54, 57};
        
        Multimap<BulkIngestKey,Value> termIndex = handler.createTermIndexColumn(record, "FIELD_NAME", "FIELD_VALUE", visibility, maskVisibility,
                        maskedFieldHelper, shardId, handler.getShardIndexTableName(), new Value(uid.toByteArray()), Direction.REVERSE);
        
        assertTrue(termIndex.size() == 2);
        boolean foundReverse = false;
        for (BulkIngestKey k : termIndex.keySet()) {
            Text row = k.getKey().getRow();
            if (row.toString().contains("EMAN_DLEIF")) {
                foundReverse = true;
            }
        }
        assertTrue(foundReverse);
    }
    
}
