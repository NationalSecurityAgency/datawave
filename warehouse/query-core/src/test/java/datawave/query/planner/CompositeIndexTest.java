package datawave.query.planner;

import static java.nio.charset.StandardCharsets.UTF_8;

import static datawave.query.testframework.RawDataManager.JEXL_AND_OP;
import static datawave.query.testframework.RawDataManager.JEXL_OR_OP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.core.query.configuration.QueryData;
import datawave.data.type.GeometryType;
import datawave.data.type.NumberType;
import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.ingest.data.config.ingest.CompositeIngest;
import datawave.ingest.data.config.ingest.ContentBaseIngestHelper;
import datawave.ingest.mapreduce.handler.shard.AbstractColumnBasedHandler;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.partition.BalancedShardPartitioner;
import datawave.ingest.table.config.ShardTableConfigHelper;
import datawave.ingest.table.config.TableConfigHelper;
import datawave.microservice.query.QueryImpl;
import datawave.policy.IngestPolicyEnforcer;
import datawave.query.composite.CompositeMetadataHelper;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.index.day.IndexIngestUtil;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.testframework.MockStatusReporter;
import datawave.query.util.AbstractQueryTest;
import datawave.table.constants.MetadataColumnFamilyConstants;
import datawave.table.constants.TableName;
import datawave.webservice.query.result.event.DefaultEvent;
import datawave.webservice.query.result.event.DefaultField;

@ExtendWith(SpringExtension.class)
@ComponentScan(basePackages = {"datawave.configuration.spring", "datawave.query"})
// @formatter:off
@ContextConfiguration(locations = {
        "classpath:datawave/query/QueryLogicFactory.xml",
        "classpath:beanRefContext.xml",
        "classpath:MarkingFunctionsContext.xml",
        "classpath:MetadataHelperContext.xml",
        "classpath:CacheContext.xml"})
// @formatter:on
public class CompositeIndexTest extends AbstractQueryTest {

    private static final int NUM_SHARDS = 3;
    private static final String DATA_TYPE_NAME = "wkt";
    private static final String INGEST_HELPER_CLASS = TestIngestHelper.class.getName();

    private static final String GEO_FIELD = "GEO";
    private static final String WKT_BYTE_LENGTH_FIELD = "WKT_BYTE_LENGTH";

    private static final String AUTHS = "ALL";
    private static final Authorizations auths = new Authorizations(AUTHS);

    private static final String formatPattern = "yyyyMMdd HHmmss.SSS";
    private static final SimpleDateFormat formatter = new SimpleDateFormat(formatPattern);

    // the legacy data sits on the three days immediately before the composite transition date, and the composite data on the three days from it, so that a
    // query spanning the transition covers only six shard days rather than two years' worth
    private static final String LEGACY_BEGIN_DATE = "20001229 000000.000";
    private static final String COMPOSITE_BEGIN_DATE = "20010101 000000.000";

    private static final String BEGIN_DATE = "20001229 000000.000";
    private static final String END_DATE = "20010103 235959.999";

    private static final Configuration conf = new Configuration();

    // @formatter:off
    private static final String[] wktLegacyData = {
            "POINT (0 0)",

            "POLYGON ((10 10, -10 10, -10 -10, 10 -10, 10 10))",

            "POLYGON ((45 45, -45 45, -45 -45, 45 -45, 45 45))",

            "POLYGON ((90 90, -90 90, -90 -90, 90 -90, 90 90))"};

    private static final Integer[] wktByteLengthLegacyData = {
            wktLegacyData[0].length(),
            wktLegacyData[1].length(),
            null,
            wktLegacyData[3].length()};

    private static final long[] legacyDates = {
            0,
            TimeUnit.DAYS.toMillis(1),
            TimeUnit.DAYS.toMillis(2),
            0};

    private static final String[] wktCompositeData = {
            "POINT (30 -85)",
            "POINT (-45 17)",

            "POLYGON ((25 25, 5 25, 5 5, 25 5, 25 25))",
            "POLYGON ((-20 -20, -40 -20, -40 -40, -20 -40, -20 -20))",

            "POLYGON ((90 45, 0 45, 0 -45, 90 -45, 90 45))",
            "POLYGON ((45 15, -45 15, -45 -60, 45 -60, 45 15))",

            "POLYGON ((180 90, 0 90, 0 -90, 180 -90, 180 90))",
            "POLYGON ((90 0, -90 0, -90 -180, 90 -180, 90 0))"};

    private static final Integer[] wktByteLengthCompositeData = {
            wktCompositeData[0].length(),
            wktCompositeData[1].length(),

            null,
            wktCompositeData[3].length(),

            wktCompositeData[4].length(),
            null,

            wktCompositeData[6].length(),
            wktCompositeData[7].length()};

    private static final long[] compositeDates = {
            0,
            TimeUnit.DAYS.toMillis(1),

            TimeUnit.DAYS.toMillis(2),
            0,

            TimeUnit.DAYS.toMillis(1),
            TimeUnit.DAYS.toMillis(2),

            0,
            TimeUnit.DAYS.toMillis(1)};
    // @formatter:on

    @Autowired
    @Qualifier("EventQuery")
    protected ShardQueryLogic logic;

    private static AccumuloClient clientForTest;
    private static List<IvaratorCacheDirConfig> ivaratorCacheDirConfigs;
    private static final IndexIngestUtil ingestUtil = new IndexIngestUtil();

    private ShardQueryLogic currentLogic;
    private List<DefaultEvent> events = new ArrayList<>();
    private int expectedEventCount = -1;

    @Override
    public ShardQueryLogic getLogic() {
        return currentLogic;
    }

    @Override
    public Authorizations getAuths() {
        return auths;
    }

    @Override
    protected void extraConfigurations() {
        disableQueryPlanAssertion();
    }

    @Override
    protected QueryImpl getSettings() throws Exception {
        QueryImpl settings = new QueryImpl();
        settings.setBeginDate(formatter.parse(BEGIN_DATE));
        settings.setEndDate(formatter.parse(END_DATE));
        settings.setPagesize(Integer.MAX_VALUE);
        settings.setQueryAuthorizations(getAuths().serialize());
        settings.setQuery(getQuery());
        settings.setParameters(getParameters());
        settings.setId(UUID.randomUUID());
        return settings;
    }

    @Override
    protected void executeQuery(ShardQueryLogic logic) throws Exception {
        try {
            Iterator<?> iter = logic.getTransformIterator(logic.getConfig().getQuery());
            events = new ArrayList<>();
            while (iter.hasNext()) {
                events.add((DefaultEvent) iter.next());
            }
        } finally {
            logic.close();
        }
    }

    @Override
    protected void extraAssertions() {
        assertEquals(expectedEventCount, events.size());

        List<String> wktList = new ArrayList<>();
        wktList.addAll(Arrays.asList(wktLegacyData));
        wktList.addAll(Arrays.asList(wktCompositeData));

        List<Integer> wktByteLengthList = new ArrayList<>();
        wktByteLengthList.addAll(Arrays.asList(wktByteLengthLegacyData));
        wktByteLengthList.addAll(Arrays.asList(wktByteLengthCompositeData));

        for (DefaultEvent event : events) {
            String wkt = null;
            Integer wktByteLength = null;

            for (DefaultField field : event.getFields()) {
                if (field.getName().equals(GEO_FIELD))
                    wkt = field.getValueString();
                else if (field.getName().equals(WKT_BYTE_LENGTH_FIELD))
                    wktByteLength = Integer.parseInt(field.getValueString());
            }

            // shouldn't get back a null wktByteLength
            assertNotNull(wktByteLength);

            // ensure that this is one of the ingested events
            assertTrue(wktList.remove(wkt));
            assertTrue(wktByteLengthList.remove(wktByteLength));
        }

        assertEquals(wktLegacyData.length + wktCompositeData.length - expectedEventCount, wktList.size());
        assertEquals(wktByteLengthLegacyData.length + wktByteLengthCompositeData.length - expectedEventCount, wktByteLengthList.size());
    }

    @BeforeAll
    public static void setupClass(@TempDir Path tempDir) throws Exception {
        System.setProperty("subject.dn.pattern", "(?:^|,)\\s*OU\\s*=\\s*My Department\\s*(?:,|$)");

        setupConfiguration(conf);

        AbstractColumnBasedHandler<Text> dataTypeHandler = new AbstractColumnBasedHandler<>();
        dataTypeHandler.setup(new TaskAttemptContextImpl(conf, new TaskAttemptID()));

        TestIngestHelper ingestHelper = new TestIngestHelper();
        ingestHelper.setup(conf);

        // create and process events with WKT data
        RawRecordContainer record = new RawRecordContainerImpl();
        Multimap<BulkIngestKey,Value> keyValues = HashMultimap.create();
        int recNum = 1;
        for (int dataIdx = 0; dataIdx < 2; dataIdx++) {

            String beginDate;
            String[] wktData;
            Integer[] wktByteLengthData;
            long[] dates;
            boolean useCompositeIngest;

            if (dataIdx == 0) {
                beginDate = LEGACY_BEGIN_DATE;
                wktData = wktLegacyData;
                wktByteLengthData = wktByteLengthLegacyData;
                dates = legacyDates;
                useCompositeIngest = false;
            } else {
                beginDate = COMPOSITE_BEGIN_DATE;
                wktData = wktCompositeData;
                wktByteLengthData = wktByteLengthCompositeData;
                dates = compositeDates;
                useCompositeIngest = true;
            }

            for (int i = 0; i < wktData.length; i++) {
                record.clear();
                record.setDataType(new Type(DATA_TYPE_NAME, TestIngestHelper.class, (Class) null, (String[]) null, 1, (String[]) null));
                record.setRawFileName("geodata_" + recNum + ".dat");
                record.setRawRecordNumber(recNum++);
                record.setTimestamp(formatter.parse(beginDate).getTime() + dates[i]);
                record.setRawData((wktData[i] + "|" + ((wktByteLengthData[i] != null) ? Integer.toString(wktByteLengthData[i]) : "")).getBytes(UTF_8));
                record.generateId(null);
                record.setVisibility(new ColumnVisibility(AUTHS));

                final Multimap<String,NormalizedContentInterface> fields = ingestHelper.getEventFields(record);

                if (useCompositeIngest && ingestHelper instanceof CompositeIngest) {
                    Multimap<String,NormalizedContentInterface> compositeFields = ingestHelper.getCompositeFields(fields);
                    for (String fieldName : compositeFields.keySet()) {
                        // if this is an overloaded event field, we are replacing the existing data
                        if (ingestHelper.isOverloadedCompositeField(fieldName))
                            fields.removeAll(fieldName);
                        fields.putAll(fieldName, compositeFields.get(fieldName));
                    }
                }

                Multimap kvPairs = dataTypeHandler.processBulk(new Text(), record, fields, new MockStatusReporter());

                keyValues.putAll(kvPairs);

                dataTypeHandler.getMetadata().addEvent(ingestHelper, record, fields);
            }
        }
        keyValues.putAll(dataTypeHandler.getMetadata().getBulkMetadata());

        // Write the composite transition date manually
        Key tdKey = new Key(new Text(GEO_FIELD), new Text(MetadataColumnFamilyConstants.COLF_CITD), new Text(DATA_TYPE_NAME + "\0" + COMPOSITE_BEGIN_DATE),
                        new Text(), new SimpleDateFormat(CompositeMetadataHelper.transitionDateFormat).parse(COMPOSITE_BEGIN_DATE).getTime());
        keyValues.put(new BulkIngestKey(new Text(TableName.METADATA), tdKey), new Value());

        // write these values to their respective tables
        InMemoryInstance instance = new InMemoryInstance();
        AccumuloClient client = new InMemoryAccumuloClient("root", instance);
        client.securityOperations().changeUserAuthorizations("root", new Authorizations(AUTHS));

        writeKeyValues(client, keyValues);
        clientForTest = client;

        Path ivaratorDir = tempDir.resolve("ivarator");
        Files.createDirectories(ivaratorDir);
        ivaratorCacheDirConfigs = Collections.singletonList(new IvaratorCacheDirConfig(ivaratorDir.toUri().toString()));

        ingestUtil.write(client, new Authorizations(AUTHS));
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

    private static void writeKeyValues(AccumuloClient client, Multimap<BulkIngestKey,Value> keyValues) throws Exception {
        final TableOperations tops = client.tableOperations();
        final Set<BulkIngestKey> biKeys = keyValues.keySet();
        for (final BulkIngestKey biKey : biKeys) {
            final String tableName = biKey.getTableName().toString();
            if (!tops.exists(tableName))
                tops.create(tableName);

            final BatchWriter writer = client.createBatchWriter(tableName, new BatchWriterConfig());
            for (final Value val : keyValues.get(biKey)) {
                final Mutation mutation = new Mutation(biKey.getKey().getRow());
                mutation.put(biKey.getKey().getColumnFamily(), biKey.getKey().getColumnQualifier(), biKey.getKey().getColumnVisibilityParsed(),
                                biKey.getKey().getTimestamp(), val);
                writer.addMutation(mutation);
            }
            writer.close();
        }

        try (BatchWriter bw = client.createBatchWriter(TableName.METADATA)) {
            Mutation m = new Mutation("num_shards");
            m.put("ns", "20000101_" + NUM_SHARDS, new Value());
            bw.addMutation(m);
        }
    }

    private void runGeoQuery(ShardQueryLogic queryLogic, String query, int expectedCount) throws Exception {
        setClientForTest(clientForTest);
        currentLogic = queryLogic;
        this.expectedEventCount = expectedCount;
        givenQuery(query);
        planAndExecuteQuery();
    }

    @Test
    public void compositeWithoutIvaratorTest() throws Exception {
        // @formatter:off
        String query = "(((_Bounded_ = true) && (" + GEO_FIELD + " >= '0202'" + JEXL_AND_OP + GEO_FIELD + " <= '020d'))" + JEXL_OR_OP +
                "((_Bounded_ = true) && (" + GEO_FIELD + " >= '030a'" + JEXL_AND_OP + GEO_FIELD + " <= '0335'))" + JEXL_OR_OP +
                "((_Bounded_ = true) && (" + GEO_FIELD + " >= '0428'" + JEXL_AND_OP + GEO_FIELD + " <= '0483'))" + JEXL_OR_OP +
                "((_Bounded_ = true) && (" + GEO_FIELD + " >= '0500aa'" + JEXL_AND_OP + GEO_FIELD + " <= '050355'))" + JEXL_OR_OP +
                "((_Bounded_ = true) && (" + GEO_FIELD + " >= '1f0aaaaaaaaaaaaaaa'" + JEXL_AND_OP + GEO_FIELD + " <= '1f36c71c71c71c71c7')))" + JEXL_AND_OP +
                "((_Bounded_ = true) && (" + WKT_BYTE_LENGTH_FIELD + " >= 0" + JEXL_AND_OP + WKT_BYTE_LENGTH_FIELD + " < 80))";
        // @formatter:on

        ShardQueryLogic rangeLogic = getShardQueryLogic(false);
        rangeLogic.setIntermediateMaxTermThreshold(50);
        rangeLogic.setIndexedMaxTermThreshold(50);
        rangeLogic.setFinalMaxTermThreshold(50);

        if (!rangeLogic.isUseDocumentScheduler()) {
            List<QueryData> queries = getQueryRanges(rangeLogic, query);
            if (rangeLogic.isUseShardedIndex()) {
                assertEquals(17, queries.size());
            } else {
                assertEquals(10, queries.size());
            }
        }

        runGeoQuery(rangeLogic, query, 9);
    }

    // the bounded range is fixed by the QueryPropertyMarkerSourceConsolidator
    // if ASTValidation is enabled the query will fail on the first visitor, InvertSwappedNodes
    @Disabled
    @Test
    public void testRecordOfIncorrectQueryStringWorking() throws Exception {
        // original "((_Bounded_ = true) && (GEO >= '0500aa' && GEO <= '050355'))";
        String query = "(((_Bounded_ = true) && GEO >= '0500aa' && GEO <= '050355'))";
        ShardQueryLogic rangeLogic = getShardQueryLogic(false);
        if (!rangeLogic.isUseDocumentScheduler()) {
            List<QueryData> queries = getQueryRanges(rangeLogic, query);
            assertEquals(1, queries.size());
        }

        runGeoQuery(rangeLogic, query, 1);
    }

    @Test
    public void compositeWithIvaratorTest() throws Exception {
        // @formatter:off
        String query = "(((_Bounded_ = true) && (" + GEO_FIELD + " >= '0202'" + JEXL_AND_OP + GEO_FIELD + " <= '020d'))" + JEXL_OR_OP +
                "((_Bounded_ = true) && (" + GEO_FIELD + " >= '030a'" + JEXL_AND_OP + GEO_FIELD + " <= '0335'))" + JEXL_OR_OP +
                "((_Bounded_ = true) && (" + GEO_FIELD + " >= '0428'" + JEXL_AND_OP + GEO_FIELD + " <= '0483'))" + JEXL_OR_OP +
                "((_Bounded_ = true) && (" + GEO_FIELD + " >= '0500aa'" + JEXL_AND_OP + GEO_FIELD + " <= '050355'))" + JEXL_OR_OP +
                "((_Bounded_ = true) && (" + GEO_FIELD + " >= '1f0aaaaaaaaaaaaaaa'" + JEXL_AND_OP + GEO_FIELD + " <= '1f36c71c71c71c71c7')))" + JEXL_AND_OP +
                "((_Bounded_ = true) && (" + WKT_BYTE_LENGTH_FIELD + " >= 0" + JEXL_AND_OP + WKT_BYTE_LENGTH_FIELD + " < 80))";
        // @formatter:on

        ShardQueryLogic rangeLogic = getShardQueryLogic(true);
        if (!rangeLogic.isUseDocumentScheduler()) {
            // the ivarator forces a full scan of the query's date range: six shard days at NUM_SHARDS shards apiece
            List<QueryData> queries = getQueryRanges(rangeLogic, query);
            assertEquals(6 * NUM_SHARDS, queries.size());
        }

        runGeoQuery(rangeLogic, query, 9);
    }

    private List<QueryData> getQueryRanges(ShardQueryLogic rangeLogic, String queryString) throws Exception {
        QueryImpl query = new QueryImpl();
        query.setBeginDate(formatter.parse(BEGIN_DATE));
        query.setEndDate(formatter.parse(END_DATE));
        query.setPagesize(Integer.MAX_VALUE);
        query.setQueryAuthorizations(auths.serialize());
        query.setQuery(queryString);
        query.setId(UUID.randomUUID());

        ShardQueryConfiguration config = ShardQueryConfiguration.create(rangeLogic, query);

        rangeLogic.initialize(config, clientForTest, query, Collections.singleton(auths));
        rangeLogic.setupQuery(config);

        List<QueryData> queryData = new ArrayList<>();
        Iterator<QueryData> iter = config.getQueriesIter();
        while (iter.hasNext()) {
            queryData.add(iter.next());
        }
        return queryData;
    }

    private ShardQueryLogic getShardQueryLogic(boolean useIvarator) {
        ShardQueryLogic clonedLogic = new ShardQueryLogic(this.logic);

        // increase the depth threshold
        clonedLogic.setMaxDepthThreshold(20);
        clonedLogic.setInitialMaxTermThreshold(15);
        clonedLogic.setIntermediateMaxTermThreshold(15);
        clonedLogic.setFinalMaxTermThreshold(15);

        // set the pushdown threshold really high to avoid collapsing uids into shards (overrides setCollapseUids if #terms is greater than this threshold)
        ((DefaultQueryPlanner) (clonedLogic.getQueryPlanner())).setPushdownThreshold(1000000);

        URL hdfsSiteConfig = this.getClass().getResource("/testhadoop.config");
        clonedLogic.setHdfsSiteConfigURLs(hdfsSiteConfig.toExternalForm());
        clonedLogic.setIvaratorCacheDirConfigs(ivaratorCacheDirConfigs);

        if (useIvarator)
            setupIvarator(clonedLogic);

        return clonedLogic;
    }

    private void setupIvarator(ShardQueryLogic logic) {
        // Set these to ensure ivarator runs
        logic.setMaxUnfieldedExpansionThreshold(1);
        logic.setMaxValueExpansionThreshold(1);
        logic.setMaxOrExpansionThreshold(1);
        logic.setMaxOrExpansionFstThreshold(1);
        logic.setIvaratorCacheScanPersistThreshold(1);
    }

    public static class TestIngestHelper extends ContentBaseIngestHelper {
        @Override
        public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer record) {
            Multimap<String,NormalizedContentInterface> eventFields = HashMultimap.create();

            String[] values = new String(record.getRawData()).split("\\|");

            NormalizedContentInterface geo_nci = new NormalizedFieldAndValue(GEO_FIELD, values[0]);
            eventFields.put(GEO_FIELD, geo_nci);

            if (values.length > 1) {
                NormalizedContentInterface wktByteLength_nci = new NormalizedFieldAndValue(WKT_BYTE_LENGTH_FIELD, values[1]);
                eventFields.put(WKT_BYTE_LENGTH_FIELD, wktByteLength_nci);
            }

            return normalizeMap(eventFields);
        }
    }
}
