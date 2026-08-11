package datawave.query.planner;

import static java.nio.charset.StandardCharsets.UTF_8;

import static datawave.query.testframework.RawDataManager.JEXL_AND_OP;
import static datawave.query.testframework.RawDataManager.JEXL_OR_OP;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.jupiter.api.BeforeAll;
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
import datawave.ingest.data.config.ingest.ContentBaseIngestHelper;
import datawave.ingest.mapreduce.handler.shard.AbstractColumnBasedHandler;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.partition.BalancedShardPartitioner;
import datawave.ingest.table.config.ShardTableConfigHelper;
import datawave.ingest.table.config.TableConfigHelper;
import datawave.microservice.query.QueryImpl;
import datawave.policy.IngestPolicyEnforcer;
import datawave.query.CloseableIterable;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.index.day.IndexIngestUtil;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.testframework.MockStatusReporter;
import datawave.query.util.AbstractQueryTest;
import datawave.table.constants.TableName;
import datawave.webservice.query.result.event.DefaultEvent;
import datawave.webservice.query.result.event.DefaultField;

@ExtendWith(SpringExtension.class)
@ComponentScan(basePackages = "datawave.query")
// @formatter:off
@ContextConfiguration(locations = {
        "classpath:datawave/query/QueryLogicFactory.xml",
        "classpath:beanRefContext.xml",
        "classpath:MarkingFunctionsContext.xml",
        "classpath:MetadataHelperContext.xml",
        "classpath:CacheContext.xml"})
// @formatter:on
public class MultiValueCompositeIndexTest extends AbstractQueryTest {

    private static final int NUM_SHARDS = 3;
    private static final String DATA_TYPE_NAME = "wkt";
    private static final String INGEST_HELPER_CLASS = TestIngestHelper.class.getName();

    private static final String GEO_FIELD = "GEO";
    private static final String WKT_BYTE_LENGTH_FIELD = "WKT_BYTE_LENGTH";

    private static final String AUTHS = "ALL";
    private static final Authorizations auths = new Authorizations(AUTHS);

    private static final String formatPattern = "yyyyMMdd HHmmss.SSS";
    private static final SimpleDateFormat formatter = new SimpleDateFormat(formatPattern);

    private static final String COMPOSITE_BEGIN_DATE = "20010101 000000.000";

    // every event is ingested at COMPOSITE_BEGIN_DATE, so the query only needs to cover that single shard day
    private static final String BEGIN_DATE = "20010101 000000.000";
    private static final String END_DATE = "20010101 235959.999";

    private static final Configuration conf = new Configuration();

    private static List<IvaratorCacheDirConfig> ivaratorCacheDirConfigs;

    private static final IndexIngestUtil ingestUtil = new IndexIngestUtil();

    private static class TestData {
        public TestData(List<String> wktData, List<Integer> numData) {
            this.wktData = wktData;
            this.numData = numData;
        }

        public List<String> wktData;
        public List<Integer> numData;

        public String toString() {
            return String.join("|", wktData) + "||" + String.join("|", numData.stream().map(x -> x.toString()).collect(Collectors.toList()));
        }

        public static TestData fromString(String td) {
            String[] splitData = td.split("\\|\\|");
            return new TestData((splitData.length >= 1) ? Arrays.asList(splitData[0].split("\\|")) : new ArrayList<>(),
                            (splitData.length >= 2)
                                            ? Arrays.asList(splitData[1].split("\\|")).stream().map(x -> Integer.parseInt(x)).collect(Collectors.toList())
                                            : new ArrayList<>());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            TestData testData = (TestData) o;

            if (!wktData.containsAll(testData.wktData))
                return false;
            if (!testData.wktData.containsAll(wktData))
                return false;
            if (!numData.containsAll(testData.numData))
                return false;
            return testData.numData.containsAll(numData);
        }

        @Override
        public int hashCode() {
            int result = wktData.hashCode();
            result = 31 * result + numData.hashCode();
            return result;
        }
    }

    private static final List<TestData> testData = new ArrayList<>();

    @Autowired
    @Qualifier("EventQuery")
    protected ShardQueryLogic logic;

    private static AccumuloClient clientForTest;

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

        for (DefaultEvent event : events) {
            List<String> wkt = new ArrayList<>();
            List<Integer> wktByteLength = new ArrayList<>();

            for (DefaultField field : event.getFields()) {
                if (field.getName().equals(GEO_FIELD))
                    wkt.add(field.getValueString());
                else if (field.getName().equals(WKT_BYTE_LENGTH_FIELD))
                    wktByteLength.add(Integer.parseInt(field.getValueString()));
            }

            TestData result = new TestData(wkt, wktByteLength);
            assertTrue(testData.contains(result));
        }
    }

    public static void createTestData() {
        // Test Data with 2 wkt and 2 numbers
        testData.add(new TestData(Arrays.asList("POLYGON ((-120 20, -100 20, -100 60, -120 60, -120 20))", "POINT (45 -45)"), Arrays.asList(55, 15)));

        // Test Data with 2 wkt and 1 number
        testData.add(new TestData(Arrays.asList("POLYGON ((-110 -15, -105 -15, -105 -10, -110 -10, -110 -15))", "POINT (45 45)"),
                        Collections.singletonList(60)));

        // Test Data with 1 wkt and 2 numbers
        testData.add(new TestData(Collections.singletonList("POINT (0 0)"), Arrays.asList(11, 22)));

    }

    @BeforeAll
    public static void setupClass(@TempDir Path tempDir) throws Exception {
        System.setProperty("subject.dn.pattern", "(?:^|,)\\s*OU\\s*=\\s*My Department\\s*(?:,|$)");

        createTestData();

        setupConfiguration(conf);

        AbstractColumnBasedHandler<Text> dataTypeHandler = new AbstractColumnBasedHandler<>();
        dataTypeHandler.setup(new TaskAttemptContextImpl(conf, new TaskAttemptID()));

        TestIngestHelper ingestHelper = new TestIngestHelper();
        ingestHelper.setup(conf);

        // create and process events with WKT data
        RawRecordContainer record = new RawRecordContainerImpl();
        Multimap<BulkIngestKey,Value> keyValues = HashMultimap.create();
        int recNum = 1;

        for (int i = 0; i < testData.size(); i++) {
            TestData entry = testData.get(i);

            record.clear();
            record.setDataType(new Type(DATA_TYPE_NAME, TestIngestHelper.class, (Class) null, (String[]) null, 1, (String[]) null));
            record.setRawFileName("geodata_" + recNum + ".dat");
            record.setRawRecordNumber(recNum++);
            record.setTimestamp(formatter.parse(COMPOSITE_BEGIN_DATE).getTime());
            record.setRawData(entry.toString().getBytes(UTF_8));
            record.generateId(null);
            record.setVisibility(new ColumnVisibility(AUTHS));

            final Multimap<String,NormalizedContentInterface> fields = ingestHelper.getEventFields(record);

            Multimap<String,NormalizedContentInterface> compositeFields = ingestHelper.getCompositeFields(fields);
            for (String fieldName : compositeFields.keySet()) {
                // if this is an overloaded event field, we are replacing the existing data
                if (ingestHelper.isOverloadedCompositeField(fieldName))
                    fields.removeAll(fieldName);
                fields.putAll(fieldName, compositeFields.get(fieldName));
            }

            Multimap kvPairs = dataTypeHandler.processBulk(new Text(), record, fields, new MockStatusReporter());

            keyValues.putAll(kvPairs);

            dataTypeHandler.getMetadata().addEvent(ingestHelper, record, fields);
        }

        keyValues.putAll(dataTypeHandler.getMetadata().getBulkMetadata());

        // write these values to their respective tables
        InMemoryInstance instance = new InMemoryInstance();
        AccumuloClient client = new InMemoryAccumuloClient("root", instance);
        client.securityOperations().changeUserAuthorizations("root", new Authorizations(AUTHS));

        writeKeyValues(client, keyValues);
        clientForTest = client;
        ingestUtil.write(client, new Authorizations(AUTHS));

        Path ivaratorDir = tempDir.resolve("ivarator");
        Files.createDirectories(ivaratorDir);
        ivaratorCacheDirConfigs = Collections.singletonList(new IvaratorCacheDirConfig(ivaratorDir.toUri().toString()));
    }

    public static void setupConfiguration(Configuration conf) {
        String compositeFieldName = GEO_FIELD;
        conf.set(DATA_TYPE_NAME + "." + compositeFieldName + BaseIngestHelper.COMPOSITE_FIELD_MAP,
                        String.join(",", new String[] {GEO_FIELD, WKT_BYTE_LENGTH_FIELD}));

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
            if (!tops.exists(tableName)) {
                tops.create(tableName);
            }

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

    private void runGeoQuery(boolean useIvarator, String query, int expectedCount) throws Exception {
        setClientForTest(clientForTest);
        currentLogic = getShardQueryLogic(useIvarator);
        this.expectedEventCount = expectedCount;
        givenQuery(query);
        planAndExecuteQuery();
    }

    @Test
    public void compositeWithoutIvaratorTest() throws Exception {
        // @formatter:off
        String query = "(((_Bounded_ = true) && (" + GEO_FIELD + " >= '0311'" + JEXL_AND_OP + GEO_FIELD + " <= '0312'))" + JEXL_AND_OP +
                WKT_BYTE_LENGTH_FIELD + " == 15)" + JEXL_OR_OP +
                "(" + GEO_FIELD + " == '1f20aaaaaaaaaaaaaa'" + JEXL_AND_OP +
                "((_Bounded_ = true) && (" + WKT_BYTE_LENGTH_FIELD + " >= 59" + JEXL_AND_OP + WKT_BYTE_LENGTH_FIELD + " <= 61)))" + JEXL_OR_OP +
                "(" + GEO_FIELD + " == '1f0aaaaaaaaaaaaaaa'" + JEXL_AND_OP + WKT_BYTE_LENGTH_FIELD + " >= 22)";
        // @formatter:on

        if (!logic.isUseDocumentScheduler()) {
            // the underlying call to getQueryRanges creates the DocumentScheduler which begins pulling ranges
            // immediately from the ThreadedRangeBundler. The test framework also begins pulling ranges and the
            // final count is obviously incorrect
            List<QueryData> queries = getQueryRanges(query, false);
            assertEquals(2, queries.size());
        }

        runGeoQuery(false, query, 3);
    }

    @Test
    public void compositeWithIvaratorTest() throws Exception {
        // @formatter:off
        String query = "(((_Bounded_ = true) && (" + GEO_FIELD + " >= '0311'" + JEXL_AND_OP + GEO_FIELD + " <= '0312'))" + JEXL_AND_OP +
                       WKT_BYTE_LENGTH_FIELD + " == 15)" + JEXL_OR_OP +
                       "(" + GEO_FIELD + " == '1f20aaaaaaaaaaaaaa'" + JEXL_AND_OP +
                       "((_Bounded_ = true) && (" + WKT_BYTE_LENGTH_FIELD + " >= 59" + JEXL_AND_OP + WKT_BYTE_LENGTH_FIELD + " <= 61)))" + JEXL_OR_OP +
                       "(" + GEO_FIELD + " == '1f0aaaaaaaaaaaaaaa'" + JEXL_AND_OP + WKT_BYTE_LENGTH_FIELD + " >= 22)";
        // @formatter:on

        if (!logic.isUseDocumentScheduler()) {
            // the underlying call to getQueryRanges creates the DocumentScheduler which begins pulling ranges
            // immediately from the ThreadedRangeBundler. The test framework also begins pulling ranges and the
            // final count is obviously incorrect
            List<QueryData> queries = getQueryRanges(query, true);

            if (logic.isUseShardedIndex()) {
                assertEquals(2, queries.size());
            } else {
                // the ivarator forces a full scan of the query's date range: one shard day at NUM_SHARDS shards
                assertEquals(NUM_SHARDS, queries.size());
            }
        }

        runGeoQuery(true, query, 3);
    }

    private List<QueryData> getQueryRanges(String queryString, boolean useIvarator) throws Exception {
        ShardQueryLogic rangeLogic = getShardQueryLogic(useIvarator);

        Iterator<QueryData> iter = getQueryRangesIterator(queryString, rangeLogic);
        List<QueryData> queryData = new ArrayList<>();
        while (iter.hasNext()) {
            queryData.add(iter.next());
        }

        if (iter instanceof CloseableIterable) {
            ((CloseableIterable<?>) iter).close();
        }

        return queryData;
    }

    private Iterator<QueryData> getQueryRangesIterator(String queryString, ShardQueryLogic rangeLogic) throws Exception {
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

        return config.getQueriesIter();
    }

    private ShardQueryLogic getShardQueryLogic(boolean useIvarator) {
        ShardQueryLogic clonedLogic = new ShardQueryLogic(this.logic);

        // increase the depth threshold
        clonedLogic.setMaxDepthThreshold(20);

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
            TestData entry = TestData.fromString(new String(record.getRawData(), UTF_8));

            for (int i = 0; i < entry.wktData.size(); i++) {
                NormalizedContentInterface geo_nci = new NormalizedFieldAndValue(GEO_FIELD, entry.wktData.get(i), Integer.toString(i), null);
                eventFields.put(GEO_FIELD, geo_nci);
            }

            for (int i = 0; i < entry.numData.size(); i++) {
                NormalizedContentInterface wktByteLength_nci = new NormalizedFieldAndValue(WKT_BYTE_LENGTH_FIELD, Integer.toString(entry.numData.get(i)),
                                Integer.toString(i), null);
                eventFields.put(WKT_BYTE_LENGTH_FIELD, wktByteLength_nci);
            }

            return normalizeMap(eventFields);
        }
    }
}
