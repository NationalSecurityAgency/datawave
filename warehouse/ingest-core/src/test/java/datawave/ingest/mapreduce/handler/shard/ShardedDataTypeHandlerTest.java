package datawave.ingest.mapreduce.handler.shard;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.Key;
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

    // constants to reduce verbosity
    private final byte[] visibility = "PUBLIC".getBytes();
    private final byte[] maskedVisibility = "PRIVATE".getBytes();
    private final byte[] shardId = "20250606_23".getBytes();
    private final long timestamp = System.currentTimeMillis();

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

        private final Set<String> maskedFields = Set.of("TEST_COL", "FIELD");

        @Override
        public void setup(Configuration config) {

        }

        @Override
        public boolean hasMappings() {
            return true;
        }

        @Override
        public boolean contains(String key) {
            return maskedFields.contains(key);
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
        record.setTimestamp(System.currentTimeMillis());

        Uid.List uid = Uid.List.newBuilder().setIGNORE(false).setCOUNT(1).addUID("d8zay2.-3pnndm.-anolok").build();
        byte[] visibility = new byte[] {65, 76, 76};
        byte[] shardId = new byte[] {50, 48, 48, 48, 48, 49, 48, 49, 95, 54, 57};

        Multimap<BulkIngestKey,Value> termIndex = handler.createTermIndexColumn(record, "TEST_COL", "FIELD_VALUE", visibility, null, null, shardId,
                        handler.getShardIndexTableName(), new Value(uid.toByteArray()), Direction.FORWARD);

        assertEquals(1, termIndex.size());
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
        record.setTimestamp(System.currentTimeMillis());

        Uid.List uid = Uid.List.newBuilder().setIGNORE(false).setCOUNT(1).addUID("d8zay2.-3pnndm.-anolok").build();
        byte[] visibility = new byte[] {65, 76, 76};
        byte[] maskVisibility = new byte[] {67, 76, 76};
        byte[] shardId = new byte[] {50, 48, 48, 48, 48, 49, 48, 49, 95, 54, 57};

        Multimap<BulkIngestKey,Value> termIndex = handler.createTermIndexColumn(record, "TEST_COL", "FIELD_VALUE", visibility, maskVisibility,
                        maskedFieldHelper, shardId, handler.getShardIndexTableName(), new Value(uid.toByteArray()), Direction.REVERSE);

        assertEquals(2, termIndex.size());
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
        record.setTimestamp(System.currentTimeMillis());

        Uid.List uid = Uid.List.newBuilder().setIGNORE(false).setCOUNT(1).addUID("d8zay2.-3pnndm.-anolok").build();
        byte[] visibility = new byte[] {65, 76, 76};
        byte[] maskVisibility = new byte[] {67, 76, 76};
        byte[] shardId = new byte[] {50, 48, 48, 48, 48, 49, 48, 49, 95, 54, 57};

        Multimap<BulkIngestKey,Value> termIndex = handler.createTermIndexColumn(record, "TEST_COL", "FIELD_VALUE", visibility, maskVisibility,
                        maskedFieldHelper, shardId, handler.getShardIndexTableName(), new Value(uid.toByteArray()), Direction.FORWARD);

        assertEquals(2, termIndex.size());
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
        record.setTimestamp(System.currentTimeMillis());

        Uid.List uid = Uid.List.newBuilder().setIGNORE(false).setCOUNT(1).addUID("d8zay2.-3pnndm.-anolok").build();
        byte[] visibility = new byte[] {65, 76, 76};
        byte[] shardId = new byte[] {50, 48, 48, 48, 48, 49, 48, 49, 95, 54, 57};

        Multimap<BulkIngestKey,Value> termIndex = handler.createTermIndexColumn(record, "TEST_COL", "FIELD_VALUE", visibility, null, null, shardId,
                        handler.getShardIndexTableName(), new Value(uid.toByteArray()), Direction.REVERSE);

        assertEquals(1, termIndex.size());
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
        record.setTimestamp(System.currentTimeMillis());

        Uid.List uid = Uid.List.newBuilder().setIGNORE(false).setCOUNT(1).addUID("d8zay2.-3pnndm.-anolok").build();
        byte[] visibility = new byte[] {65, 76, 76};
        byte[] maskVisibility = new byte[] {67, 76, 76};
        byte[] shardId = new byte[] {50, 48, 48, 48, 48, 49, 48, 49, 95, 54, 57};

        Multimap<BulkIngestKey,Value> termIndex = handler.createTermIndexColumn(record, "OTHER_COL", "FIELD_VALUE", visibility, maskVisibility,
                        maskedFieldHelper, shardId, handler.getShardIndexTableName(), new Value(uid.toByteArray()), Direction.REVERSE);

        assertEquals(1, termIndex.size());
        for (BulkIngestKey k : termIndex.keySet()) {
            byte[] keyBytes = k.getKey().getColumnVisibility().getBytes();
            assertArrayEquals(keyBytes, maskVisibility);
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
        record.setTimestamp(expectedEventDate);
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

    @Test
    public void testSimpleForwardIndex() {
        RawRecordContainer record = createRawRecordContainer();
        Value value = createValueWithUid();
        Multimap<BulkIngestKey,Value> values = handler.createTermIndexColumn(record, "FIELD", "value", visibility, null, null, shardId,
                        handler.getShardIndexTableName(), value, Direction.FORWARD);

        assertEquals(1, values.size());
        assertBulkIngestKey(values, createExpectedBulkIngestKey(), createValueWithUid());
        assertTrue(values.isEmpty());
    }

    @Test
    public void testSimpleForwardIndexWithDayIndex() {
        RawRecordContainer record = createRawRecordContainer();
        Value value = createValueWithUid();
        try {
            enableDayIndex();
            Multimap<BulkIngestKey,Value> values = handler.createTermIndexColumn(record, "FIELD", "value", visibility, null, null, shardId,
                            handler.getShardIndexTableName(), value, Direction.FORWARD);

            assertEquals(2, values.size());
            assertBulkIngestKey(values, createExpectedBulkIngestKey(), createValueWithUid());
            assertBulkIngestKey(values, createExpectedBulkIngestKeyForDayIndex(), createDayIndexValue());
            assertTrue(values.isEmpty());
        } finally {
            disableDayIndex();
        }
    }

    @Test
    public void testSimpleForwardIndexWithYearIndex() {
        RawRecordContainer record = createRawRecordContainer();
        Value value = createValueWithUid();
        try {
            enableYearIndex();
            Multimap<BulkIngestKey,Value> values = handler.createTermIndexColumn(record, "FIELD", "value", visibility, null, null, shardId,
                            handler.getShardIndexTableName(), value, Direction.FORWARD);

            assertEquals(2, values.size());
            assertBulkIngestKey(values, createExpectedBulkIngestKey(), createValueWithUid());
            assertBulkIngestKey(values, createExpectedBulkIngestKeyForYearIndex(), createYearIndexValue());
            assertTrue(values.isEmpty());
        } finally {
            disableYearIndex();
        }
    }

    @Test
    public void testSimpleForwardIndexWithDayAndYearIndex() {
        // there is no 'overkill', only 'open fire' and 'reload'
        RawRecordContainer record = createRawRecordContainer();
        Value value = createValueWithUid();
        try {
            enableDayIndex();
            enableYearIndex();
            Multimap<BulkIngestKey,Value> values = handler.createTermIndexColumn(record, "FIELD", "value", visibility, null, null, shardId,
                            handler.getShardIndexTableName(), value, Direction.FORWARD);

            assertEquals(3, values.size());
            assertBulkIngestKey(values, createExpectedBulkIngestKey(), createValueWithUid());
            assertBulkIngestKey(values, createExpectedBulkIngestKeyForDayIndex(), createDayIndexValue());
            assertBulkIngestKey(values, createExpectedBulkIngestKeyForYearIndex(), createYearIndexValue());
            assertTrue(values.isEmpty());
        } finally {
            disableDayIndex();
            disableYearIndex();
        }
    }

    @Test
    public void testMaskedSimpleForwardIndex() {
        RawRecordContainer record = createRawRecordContainer();
        Value value = createValueWithUid();
        Multimap<BulkIngestKey,Value> values = handler.createTermIndexColumn(record, "FIELD", "value", visibility, maskedVisibility, maskedFieldHelper, shardId,
                        handler.getShardIndexTableName(), value, Direction.FORWARD);

        assertEquals(2, values.size());
        assertBulkIngestKey(values, createExpectedBulkIngestKey(), createValueWithUid());
        assertBulkIngestKey(values, createExpectedBulkIngestKeyMasked(), createValueWithUid());
        assertTrue(values.isEmpty());
    }

    @Test
    public void testMaskedSimpleForwardIndexWithDayIndex() {
        RawRecordContainer record = createRawRecordContainer();
        Value value = createValueWithUid();
        try {
            enableDayIndex();
            Multimap<BulkIngestKey,Value> values = handler.createTermIndexColumn(record, "FIELD", "value", visibility, maskedVisibility, maskedFieldHelper,
                            shardId, handler.getShardIndexTableName(), value, Direction.FORWARD);

            assertEquals(4, values.size());
            assertBulkIngestKey(values, createExpectedBulkIngestKey(), createValueWithUid());
            assertBulkIngestKey(values, createExpectedBulkIngestKeyMasked(), createValueWithUid());
            assertBulkIngestKey(values, createExpectedBulkIngestKeyForDayIndex(), createDayIndexValue());
            assertBulkIngestKey(values, createExpectedBulkIngestKeyForDayIndexMasked(), createDayIndexValue());
            assertTrue(values.isEmpty());
        } finally {
            disableDayIndex();
        }
    }

    @Test
    public void testMaskedSimpleForwardIndexWithYearIndex() {
        RawRecordContainer record = createRawRecordContainer();
        Value value = createValueWithUid();
        try {
            enableYearIndex();
            Multimap<BulkIngestKey,Value> values = handler.createTermIndexColumn(record, "FIELD", "value", visibility, maskedVisibility, maskedFieldHelper,
                            shardId, handler.getShardIndexTableName(), value, Direction.FORWARD);

            assertEquals(4, values.size());
            assertBulkIngestKey(values, createExpectedBulkIngestKey(), createValueWithUid());
            assertBulkIngestKey(values, createExpectedBulkIngestKeyMasked(), createValueWithUid());
            assertBulkIngestKey(values, createExpectedBulkIngestKeyForYearIndex(), createYearIndexValue());
            assertBulkIngestKey(values, createExpectedBulkIngestKeyForYearIndexMasked(), createYearIndexValue());
            assertTrue(values.isEmpty());
        } finally {
            disableYearIndex();
        }
    }

    @Test
    public void testMaskedSimpleForwardIndexWithDayAndYearIndex() {
        // there is no 'overkill', only 'open fire' and 'reload'
        RawRecordContainer record = createRawRecordContainer();
        Value value = createValueWithUid();
        try {
            enableDayIndex();
            enableYearIndex();
            Multimap<BulkIngestKey,Value> values = handler.createTermIndexColumn(record, "FIELD", "value", visibility, maskedVisibility, maskedFieldHelper,
                            shardId, handler.getShardIndexTableName(), value, Direction.FORWARD);

            assertEquals(6, values.size());
            assertBulkIngestKey(values, createExpectedBulkIngestKey(), createValueWithUid());
            assertBulkIngestKey(values, createExpectedBulkIngestKeyMasked(), createValueWithUid());
            assertBulkIngestKey(values, createExpectedBulkIngestKeyForDayIndex(), createDayIndexValue());
            assertBulkIngestKey(values, createExpectedBulkIngestKeyForDayIndexMasked(), createDayIndexValue());
            assertBulkIngestKey(values, createExpectedBulkIngestKeyForYearIndex(), createYearIndexValue());
            assertBulkIngestKey(values, createExpectedBulkIngestKeyForYearIndexMasked(), createYearIndexValue());
            assertTrue(values.isEmpty());
        } finally {
            disableDayIndex();
            disableYearIndex();
        }
    }

    @Test
    public void testSimpleReverseIndex() {
        RawRecordContainer record = createRawRecordContainer();
        Value value = createValueWithUid();
        Multimap<BulkIngestKey,Value> values = handler.createTermIndexColumn(record, "FIELD", reverse("value"), visibility, null, null, shardId,
                        handler.getShardReverseIndexTableName(), value, Direction.REVERSE);

        assertEquals(1, values.size());
        assertBulkIngestKey(values, createExpectedBulkIngestKeyReversed(), createValueWithUid());
        assertTrue(values.isEmpty());
    }

    @Test
    public void testSimpleReverseIndexWithDayIndex() {

    }

    @Test
    public void testSimpleReverseIndexWithYearIndex() {

    }

    @Test
    public void testSimpleReverseIndexWithDayAndYearIndex() {
        // there is no 'overkill', only 'open fire' and 'reload'
    }

    @Test
    public void testMaskedSimpleReverseIndex() {

    }

    @Test
    public void testMaskedSimpleReverseIndexWithDayIndex() {

    }

    @Test
    public void testMaskedSimpleReverseIndexWithYearIndex() {

    }

    @Test
    public void testMaskedSimpleReverseIndexWithDayAndYearIndex() {
        // there is no 'overkill', only 'open fire' and 'reload'
    }

    private RawRecordContainer createRawRecordContainer() {
        RawRecordContainer record = new RawRecordContainerImpl();
        record.setDataType(createType());
        record.setRawFileName("data_0.dat");
        record.setRawRecordNumber(1);
        record.setRawData("raw-data".getBytes(StandardCharsets.UTF_8));
        // time stamp is hard coded so the test framework can reliably remove keys with timestamps
        record.setTimestamp(getTimestamp());
        return record;
    }

    private void enableDayIndex() {
        handler.setDayIndexEnabled(true);
        handler.setShardDayIndexTableName(new Text(TableName.SHARD_DAY_INDEX));
    }

    private void disableDayIndex() {
        handler.setDayIndexEnabled(false);
        handler.setShardDayIndexTableName(null);
    }

    private void enableYearIndex() {
        handler.setYearIndexEnabled(true);
        handler.setShardYearIndexTableName(new Text(TableName.SHARD_YEAR_INDEX));
    }

    private void disableYearIndex() {
        handler.setYearIndexEnabled(false);
        handler.setShardYearIndexTableName(null);
    }

    private long getTimestamp() {
        return ShardedDataTypeHandler.getIndexTimestamp(timestamp);
    }

    private Type createType() {
        return new Type(DATA_TYPE_NAME, TestIngestHelper.class, null, null, 10, null);
    }

    private BulkIngestKey createExpectedBulkIngestKey() {
        Key key = new Key("value", "FIELD", "20250606_23\0wkt", "PUBLIC", getTimestamp());
        return new BulkIngestKey(new Text("shardIndex"), key);
    }

    private BulkIngestKey createExpectedBulkIngestKeyMasked() {
        Key key = new Key("MASKED_VALUE", "FIELD", "20250606_23\0wkt", "PRIVATE", getTimestamp());
        return new BulkIngestKey(new Text("shardIndex"), key);
    }

    private BulkIngestKey createExpectedBulkIngestKeyReversed() {
        Key key = new Key("eulav", "FIELD", "20250606_23\0wkt", "PUBLIC", getTimestamp());
        return new BulkIngestKey(new Text("shardReverseIndex"), key);
    }

    private BulkIngestKey createExpectedBulkIngestKeyForDayIndex() {
        Key key = new Key("20250606\0value", "FIELD", "wkt", "PUBLIC", getTimestamp());
        return new BulkIngestKey(new Text(TableName.SHARD_DAY_INDEX), key);
    }

    private BulkIngestKey createExpectedBulkIngestKeyForDayIndexMasked() {
        Key key = new Key("20250606\0MASKED_VALUE", "FIELD", "wkt", "PRIVATE", getTimestamp());
        return new BulkIngestKey(new Text(TableName.SHARD_DAY_INDEX), key);
    }

    private BulkIngestKey createExpectedBulkIngestKeyForYearIndex() {
        Key key = new Key("2025\0value", "FIELD", "wkt", "PUBLIC", getTimestamp());
        return new BulkIngestKey(new Text(TableName.SHARD_YEAR_INDEX), key);
    }

    private BulkIngestKey createExpectedBulkIngestKeyForYearIndexMasked() {
        Key key = new Key("2025\0MASKED_VALUE", "FIELD", "wkt", "PRIVATE", getTimestamp());
        return new BulkIngestKey(new Text(TableName.SHARD_YEAR_INDEX), key);
    }

    private Value createValueWithUid() {
        return createValueWithUid("d8zay2.-3pnndm.-anolok");
    }

    private Value createValueWithUid(String uid) {
        Uid.List list = Uid.List.newBuilder().setIGNORE(false).setCOUNT(1).addUID(uid).build();
        return new Value(list.toByteArray());
    }

    private Value createDayIndexValue() {
        BitSet bits = new BitSet();
        bits.set(23);
        return new Value(bits.toByteArray());
    }

    private Value createYearIndexValue() {
        BitSet bits = new BitSet();
        bits.set(157); // 6 June 2025 is the 157th day of the year
        return new Value(bits.toByteArray());
    }

    private final StringBuilder sb = new StringBuilder();

    private String reverse(String value) {
        sb.setLength(0);
        sb.append(value);
        sb.reverse();
        return sb.toString();
    }

    private void assertBulkIngestKey(Multimap<BulkIngestKey,Value> multimap, BulkIngestKey expectedKey, Value expectedValue) {
        assertTrue("did not find expected BulkIngestKey", multimap.containsKey(expectedKey));
        Collection<Value> values = multimap.get(expectedKey);
        boolean found = values.remove(expectedValue);
        assertTrue("did not find expected value", found);
        if (!values.isEmpty()) {
            multimap.putAll(expectedKey, values);
        }
    }
}
