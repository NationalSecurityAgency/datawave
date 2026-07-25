package datawave.query.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

import datawave.data.type.GeoType;
import datawave.data.type.PointType;
import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.ingest.data.config.ingest.ContentBaseIngestHelper;
import datawave.ingest.mapreduce.handler.shard.AbstractColumnBasedHandler;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.partition.BalancedShardPartitioner;
import datawave.ingest.table.config.ShardTableConfigHelper;
import datawave.ingest.table.config.TableConfigHelper;
import datawave.policy.IngestPolicyEnforcer;
import datawave.query.index.day.IndexIngestUtil;
import datawave.query.testframework.MockStatusReporter;
import datawave.table.constants.TableName;

/**
 * Write tests data for the {@link datawave.query.MixedGeoAndGeoWaveTest}
 */
public class MixedGeoAndGeoWaveIngest {

    private static final Logger log = LoggerFactory.getLogger(MixedGeoAndGeoWaveIngest.class);

    private static final int NUM_SHARDS = 1;
    private static final String DATA_TYPE_NAME = "MixedGeo";
    private static final String INGEST_HELPER_CLASS = TestIngestHelper.class.getName();

    public static final String GEO_FIELD = "GEO";
    public static final String POINT_FIELD = "POINT";
    public static final String POLY_POINT_FIELD = "POLY_POINT";

    private static final String formatPattern = "yyyyMMdd HHmmss.SSS";
    private static final SimpleDateFormat formatter = new SimpleDateFormat(formatPattern);

    public static final String BEGIN_DATE = "20000101 000000.000";
    private static final String MID_DATE = "20010101 000000.000";
    public static final String END_DATE = "20020101 000000.000";

    public static final String GEO_1 = "0_0";
    public static final String GEO_2 = "3_0";
    public static final String GEO_3 = "2_0";
    public static final String GEO_4 = "1_0";
    public static final String GEO_5 = "1_1";
    public static final String GEO_6 = "2_1";

    public static final String POINT_1 = "POINT (2 2)";
    public static final String POINT_2 = "POINT (2 1)";
    public static final String POINT_3 = "POINT (2 3)";
    public static final String POINT_4 = "POINT (1 3)";
    public static final String POINT_5 = "POINT (2 0)";
    public static final String POINT_6 = "POINT (1 0)";

    public static final String POLY_1 = "POLYGON((-4 -4, 0 -4, 0 0, -4 0, -4 -4))";
    public static final String POLY_2 = "POLYGON((0 -4, 4 -4, 4 0, -4 0, 0 -4))";
    public static final String POLY_3 = "POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))";
    public static final String POLY_4 = "POLYGON((-4 0, 0 0, 0 4, -4 4, -4 0))";
    public static final String POLY_5 = "POLYGON((-2 -2, 2 -2, 2 2, -2 2, -2 -2))";

    // @formatter:off
    public static final String[] geoData = {
            GEO_1,
            GEO_2,
            GEO_3,
            GEO_4,
            GEO_5,
            GEO_6};
    // @formatter:on

    // @formatter:off
    public static final String[] pointData = {
            POINT_1,
            POINT_2,
            POINT_3,
            POINT_4,
            POINT_5,
            POINT_6};
    // @formatter:on

    // @formatter:off
    public static final String[] polyData = {
            POLY_1,
            POLY_2,
            POLY_3,
            POLY_4,
            POLY_5};
    // @formatter:on

    private static final String AUTHS = "ALL";

    private static final Configuration conf = new Configuration();

    private MixedGeoAndGeoWaveIngest() {
        // enforce static access
    }

    public static void write(AccumuloClient client, Authorizations auths) throws Exception {
        setupConfiguration(conf);

        int recNum = 0;
        recNum = ingestData(client, GEO_FIELD, geoData, recNum, BEGIN_DATE);
        recNum = ingestData(client, POINT_FIELD, pointData, recNum, MID_DATE);
        recNum = ingestData(client, POLY_POINT_FIELD, polyData, recNum, MID_DATE);

        log.info("Ingested {} records", recNum);

        // write model fields
        try (BatchWriter bw = client.createBatchWriter(TableName.METADATA)) {
            Mutation m = new Mutation(GEO_FIELD);
            m.put("DATAWAVE", GEO_FIELD + "\0forward", "ALL");
            m.put("DATAWAVE", POINT_FIELD + "\0forward", "ALL");
            bw.addMutation(m);
        }

        IndexIngestUtil ingestUtil = new IndexIngestUtil();
        ingestUtil.write(client, auths);
    }

    public static int ingestData(AccumuloClient client, String fieldName, String[] data, int startRecNum, String ingestDate) throws Exception {
        AbstractColumnBasedHandler<Text> dataTypeHandler = new AbstractColumnBasedHandler<>();
        dataTypeHandler.setup(new TaskAttemptContextImpl(conf, new TaskAttemptID()));

        TestIngestHelper ingestHelper = new TestIngestHelper();
        ingestHelper.setup(conf);

        // create and process events with WKT data
        RawRecordContainer record = new RawRecordContainerImpl();
        Multimap<BulkIngestKey,Value> keyValues = HashMultimap.create();
        int recNum = startRecNum;

        for (String datum : data) {
            record.clear();
            record.setDataType(new Type(DATA_TYPE_NAME, TestIngestHelper.class, null, null, 1, null));
            record.setRawFileName("geodata_" + recNum + ".dat");
            record.setRawRecordNumber(recNum++);
            record.setTimestamp(formatter.parse(ingestDate).getTime());
            record.setRawData((fieldName + datum).getBytes(UTF_8));
            record.generateId(null);
            record.setVisibility(new ColumnVisibility(AUTHS));

            final Multimap<String,NormalizedContentInterface> fields = LinkedListMultimap.create();
            for (Map.Entry<String,NormalizedContentInterface> entry : ingestHelper.getEventFields(record).entries()) {
                if (entry.getValue().getError() == null) {
                    fields.put(entry.getKey(), entry.getValue());
                }
            }

            Multimap<BulkIngestKey,Value> kvPairs = dataTypeHandler.processBulk(new Text(), record, fields, new MockStatusReporter());

            keyValues.putAll(kvPairs);

            dataTypeHandler.getMetadata().addEvent(ingestHelper, record, fields);
        }
        keyValues.putAll(dataTypeHandler.getMetadata().getBulkMetadata());

        writeKeyValues(client, keyValues);
        return recNum;
    }

    private static void writeKeyValues(AccumuloClient client, Multimap<BulkIngestKey,Value> keyValues) throws Exception {
        final TableOperations tops = client.tableOperations();
        final Set<BulkIngestKey> biKeys = keyValues.keySet();

        Set<String> loadedShards = new HashSet<>();

        for (final BulkIngestKey biKey : biKeys) {
            final String tableName = biKey.getTableName().toString();
            if (!tops.exists(tableName))
                tops.create(tableName);

            try (BatchWriter bw = client.createBatchWriter(tableName, new BatchWriterConfig())) {
                for (final Value val : keyValues.get(biKey)) {
                    Key key = biKey.getKey();
                    final Mutation m = new Mutation(key.getRow());
                    m.put(key.getColumnFamily(), key.getColumnQualifier(), key.getColumnVisibilityParsed(), key.getTimestamp(), val);
                    bw.addMutation(m);

                    if (biKey.getTableName().toString().equals("shard")) {
                        loadedShards.add(key.getRow().toString());
                    }
                }
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("Loaded data into {} shards", loadedShards.size());
        }

        try (BatchWriter bw = client.createBatchWriter(TableName.METADATA)) {
            Mutation m = new Mutation("num_shards");
            m.put("ns", "20000101_" + NUM_SHARDS, new Value());
            bw.addMutation(m);
        }
    }

    public static void setupConfiguration(Configuration conf) {
        conf.clear();

        conf.set(DATA_TYPE_NAME + BaseIngestHelper.INDEX_FIELDS, GEO_FIELD + "," + POINT_FIELD + "," + POLY_POINT_FIELD);
        conf.set(DATA_TYPE_NAME + "." + GEO_FIELD + BaseIngestHelper.FIELD_TYPE, GeoType.class.getName());
        conf.set(DATA_TYPE_NAME + "." + POINT_FIELD + BaseIngestHelper.FIELD_TYPE, PointType.class.getName());
        conf.set(DATA_TYPE_NAME + "." + POLY_POINT_FIELD + BaseIngestHelper.FIELD_TYPE, PointType.class.getName());

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

    public static class TestIngestHelper extends ContentBaseIngestHelper {
        @Override
        public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer record) {
            Multimap<String,NormalizedContentInterface> eventFields = HashMultimap.create();
            String rawData = new String(record.getRawData());
            String prefix = null;
            if (rawData.startsWith(GEO_FIELD)) {
                prefix = GEO_FIELD;
            } else if (rawData.startsWith(POINT_FIELD)) {
                prefix = POINT_FIELD;
            } else if (rawData.startsWith(POLY_POINT_FIELD)) {
                prefix = POLY_POINT_FIELD;
            }

            assertNotNull(prefix);
            NormalizedContentInterface geo_nci = new NormalizedFieldAndValue(prefix, rawData.substring(prefix.length()));
            eventFields.put(prefix, geo_nci);
            return normalizeMap(eventFields);
        }
    }
}
