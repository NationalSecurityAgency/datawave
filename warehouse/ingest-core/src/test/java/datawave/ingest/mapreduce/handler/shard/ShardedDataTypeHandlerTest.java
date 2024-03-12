package datawave.ingest.mapreduce.handler.shard;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.data.hash.HashUID;
import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.ingest.data.config.MaskedFieldHelper;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.data.config.ingest.ContentBaseIngestHelper;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.protobuf.Uid;
import datawave.ingest.table.config.ShardTableConfigHelper;
import datawave.ingest.table.config.TableConfigHelper;
import datawave.policy.IngestPolicyEnforcer;
import datawave.query.model.Direction;
import datawave.util.CompositeTimestamp;
import datawave.util.TableName;

public class ShardedDataTypeHandlerTest {

    ShardedDataTypeHandler<Text> handler;
    AbstractColumnBasedHandler<Text> dataTypeHandler;
    TestIngestHelper ingestHelper;
    TestMaskedHelper maskedFieldHelper;
    private static final int NUM_SHARDS = 241;
    private static final String DATA_TYPE_NAME = "wkt";
    private static final String INGEST_HELPER_CLASS = TestIngestHelper.class.getName();

    private static final long MS_PER_DAY = TimeUnit.DAYS.toMillis(1);

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
            return "MASKED_VALUE";
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
            if (key.equals("TEST_COL")) {
                return true;
            } else {
                return false;
            }

        }

        @Override
        public String get(String key) {
            return "MASKED_VALUE";
        }
    }

    public static void setupConfiguration(Configuration conf) {
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
        record.setDate(System.currentTimeMillis());

        Uid.List uid = Uid.List.newBuilder().setIGNORE(false).setCOUNT(1).addUID("d8zay2.-3pnndm.-anolok").build();
        byte[] visibility = new byte[] {65, 76, 76};
        byte[] shardId = new byte[] {50, 48, 48, 48, 48, 49, 48, 49, 95, 54, 57};

        Multimap<BulkIngestKey,Value> termIndex = handler.createTermIndexColumn(record, "TEST_COL", "FIELD_VALUE", visibility, null, null, shardId,
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
        record.setDate(System.currentTimeMillis());

        Uid.List uid = Uid.List.newBuilder().setIGNORE(false).setCOUNT(1).addUID("d8zay2.-3pnndm.-anolok").build();
        byte[] visibility = new byte[] {65, 76, 76};
        byte[] maskVisibility = new byte[] {67, 76, 76};
        byte[] shardId = new byte[] {50, 48, 48, 48, 48, 49, 48, 49, 95, 54, 57};

        Multimap<BulkIngestKey,Value> termIndex = handler.createTermIndexColumn(record, "TEST_COL", "FIELD_VALUE", visibility, maskVisibility,
                        maskedFieldHelper, shardId, handler.getShardIndexTableName(), new Value(uid.toByteArray()), Direction.REVERSE);

        assertTrue(termIndex.size() == 2);
        boolean foundValue = false;
        for (BulkIngestKey k : termIndex.keySet()) {
            Text row = k.getKey().getRow();
            if (row.toString().contains("EULAV_DEKSAM")) {
                foundValue = true;
            }
        }
        assertTrue(foundValue);
    }

    @Test
    public void testMaskedForward() {
        Type dataType = new Type(DATA_TYPE_NAME, TestIngestHelper.class, null, null, 10, null);
        String entry = "testingtesting";
        RawRecordContainer record = new RawRecordContainerImpl();
        record.setDataType(dataType);
        record.setRawFileName("data_" + 0 + ".dat");
        record.setRawRecordNumber(1);
        record.setRawData(entry.getBytes(StandardCharsets.UTF_8));
        record.setDate(System.currentTimeMillis());

        Uid.List uid = Uid.List.newBuilder().setIGNORE(false).setCOUNT(1).addUID("d8zay2.-3pnndm.-anolok").build();
        byte[] visibility = new byte[] {65, 76, 76};
        byte[] maskVisibility = new byte[] {67, 76, 76};
        byte[] shardId = new byte[] {50, 48, 48, 48, 48, 49, 48, 49, 95, 54, 57};

        Multimap<BulkIngestKey,Value> termIndex = handler.createTermIndexColumn(record, "TEST_COL", "FIELD_VALUE", visibility, maskVisibility,
                        maskedFieldHelper, shardId, handler.getShardIndexTableName(), new Value(uid.toByteArray()), Direction.FORWARD);

        assertTrue(termIndex.size() == 2);
        boolean foundValue = false;
        for (BulkIngestKey k : termIndex.keySet()) {
            Text row = k.getKey().getRow();
            if (row.toString().contains("MASKED_VALUE")) {
                foundValue = true;
            }
        }
        assertTrue(foundValue);
    }

    @Test
    public void testNonMaskedReverseIndex() {
        Type dataType = new Type(DATA_TYPE_NAME, TestIngestHelper.class, null, null, 10, null);
        String entry = "testingtesting";
        RawRecordContainer record = new RawRecordContainerImpl();
        record.setDataType(dataType);
        record.setRawFileName("data_" + 0 + ".dat");
        record.setRawRecordNumber(1);
        record.setRawData(entry.getBytes(StandardCharsets.UTF_8));
        record.setDate(System.currentTimeMillis());

        Uid.List uid = Uid.List.newBuilder().setIGNORE(false).setCOUNT(1).addUID("d8zay2.-3pnndm.-anolok").build();
        byte[] visibility = new byte[] {65, 76, 76};
        byte[] shardId = new byte[] {50, 48, 48, 48, 48, 49, 48, 49, 95, 54, 57};

        Multimap<BulkIngestKey,Value> termIndex = handler.createTermIndexColumn(record, "TEST_COL", "FIELD_VALUE", visibility, null, null, shardId,
                        handler.getShardIndexTableName(), new Value(uid.toByteArray()), Direction.REVERSE);

        assertTrue(termIndex.size() == 1);
        boolean foundValue = false;
        for (BulkIngestKey k : termIndex.keySet()) {
            Text row = k.getKey().getRow();
            if (row.toString().contains("FIELD_VALUE")) {
                foundValue = true;
            }
        }
        assertTrue(foundValue);
    }

    @Test
    public void testNonMaskedVisibility() {
        Type dataType = new Type(DATA_TYPE_NAME, TestIngestHelper.class, null, null, 10, null);
        String entry = "testingtesting";
        RawRecordContainer record = new RawRecordContainerImpl();
        record.setDataType(dataType);
        record.setRawFileName("data_" + 0 + ".dat");
        record.setRawRecordNumber(1);
        record.setRawData(entry.getBytes(StandardCharsets.UTF_8));
        record.setDate(System.currentTimeMillis());

        Uid.List uid = Uid.List.newBuilder().setIGNORE(false).setCOUNT(1).addUID("d8zay2.-3pnndm.-anolok").build();
        byte[] visibility = new byte[] {65, 76, 76};
        byte[] maskVisibility = new byte[] {67, 76, 76};
        byte[] shardId = new byte[] {50, 48, 48, 48, 48, 49, 48, 49, 95, 54, 57};

        Multimap<BulkIngestKey,Value> termIndex = handler.createTermIndexColumn(record, "OTHER_COL", "FIELD_VALUE", visibility, maskVisibility,
                        maskedFieldHelper, shardId, handler.getShardIndexTableName(), new Value(uid.toByteArray()), Direction.REVERSE);

        assertTrue(termIndex.size() == 1);
        for (BulkIngestKey k : termIndex.keySet()) {
            byte[] keyBytes = k.getKey().getColumnVisibility().getBytes();
            assertTrue(Arrays.equals(keyBytes, maskVisibility));
        }
    }

    @Test
    public void testAgeOffDate() {
        Type dataType = new Type(DATA_TYPE_NAME, TestIngestHelper.class, null, null, 10, null);
        String entry = "testingtesting";
        RawRecordContainer record = new RawRecordContainerImpl();
        record.setDataType(dataType);
        record.setRawFileName("data_" + 0 + ".dat");
        record.setRawRecordNumber(1);
        record.setRawData(entry.getBytes(StandardCharsets.UTF_8));
        record.setId(HashUID.builder().newId(record.getRawData()));
        record.setVisibility(new ColumnVisibility("PUBLIC"));
        long expectedEventDate = System.currentTimeMillis();
        record.setDate(expectedEventDate);
        Multimap<String,NormalizedContentInterface> fields = ingestHelper.getEventFields(record);

        assertEquals(expectedEventDate, record.getDate());
        assertEquals(expectedEventDate, record.getAgeOffDate());
        assertEquals(expectedEventDate, record.getTimestamp());

        Multimap<BulkIngestKey,Value> data = handler.processBulk(null, record, fields, null);

        long expectedTimestamp = CompositeTimestamp.getCompositeTimeStamp(expectedEventDate, expectedEventDate);
        long tsToDay = (expectedEventDate / MS_PER_DAY) * MS_PER_DAY;
        long expectedIndexTimestamp = CompositeTimestamp.getCompositeTimeStamp(tsToDay, tsToDay);
        for (BulkIngestKey key : data.keySet()) {
            if (key.getTableName().toString().toUpperCase().contains("INDEX")) {
                assertEquals(key.toString(), expectedIndexTimestamp, key.getKey().getTimestamp());
            } else {
                assertEquals(key.toString(), expectedTimestamp, key.getKey().getTimestamp());
            }
        }

        // now get an ageoff date (must be a multiple of MS_PER_DAY past the event date)
        long expectedAgeOffDate = expectedEventDate + (MS_PER_DAY * 11);
        expectedTimestamp = CompositeTimestamp.getCompositeTimeStamp(expectedEventDate, expectedAgeOffDate);
        record.setTimestamp(expectedTimestamp);

        assertEquals(expectedEventDate, record.getDate());
        assertEquals(expectedAgeOffDate, record.getAgeOffDate());
        assertEquals(expectedTimestamp, record.getTimestamp());

        data = handler.processBulk(null, record, fields, null);

        long ageOffToDay = (expectedAgeOffDate / MS_PER_DAY) * MS_PER_DAY;
        expectedIndexTimestamp = CompositeTimestamp.getCompositeTimeStamp(tsToDay, ageOffToDay);
        for (BulkIngestKey key : data.keySet()) {
            if (key.getTableName().toString().toUpperCase().contains("INDEX")) {
                assertEquals(key.toString(), expectedIndexTimestamp, key.getKey().getTimestamp());
            } else {
                assertEquals(key.toString(), expectedTimestamp, key.getKey().getTimestamp());
            }
        }
    }

}
