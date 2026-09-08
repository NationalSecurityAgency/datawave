package datawave.query.jexl.nodes;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
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

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.jexl3.parser.ParseException;
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
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.data.type.GeometryType;
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
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.index.day.IndexIngestUtil;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.PushdownLargeFieldedListsVisitor;
import datawave.query.planner.DefaultQueryPlanner;
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
public class ExceededOrThresholdMarkerJexlNodeTest extends AbstractQueryTest {

    private static final int NUM_SHARDS = 1;
    private static final String DATA_TYPE_NAME = "wkt";
    private static final String INGEST_HELPER_CLASS = TestIngestHelper.class.getName();

    private static final String GEO_FIELD = "0GEO";
    private static final String GEO_QUERY_FIELD = JexlASTHelper.rebuildIdentifier(GEO_FIELD);

    private static final String AUTHS = "ALL";
    private static final Authorizations auths = new Authorizations(AUTHS);

    private static final String formatPattern = "yyyyMMdd HHmmss.SSS";
    private static final SimpleDateFormat formatter = new SimpleDateFormat(formatPattern);

    private static final String BEGIN_DATE = "20000101 000000.000";
    private static final String END_DATE = "20000101 000001.000";

    private static final Configuration conf = new Configuration();

    private static final String POINT_1 = "POINT (0 0)";
    private static final String POINT_2 = "POINT (0 3)";
    private static final String POINT_3 = "POINT (0 2)";
    private static final String POINT_4 = "POINT (0 1)";
    private static final String POINT_5 = "POINT (1 1)";
    private static final String POINT_6 = "POINT (1 2)";
    private static final String POINT_7 = "POINT (2 2)";
    private static final String POINT_8 = "POINT (2 1)";
    private static final String POINT_9 = "POINT (2 3)";
    private static final String POINT_10 = "POINT (1 3)";
    private static final String POINT_11 = "POINT (2 0)";
    private static final String POINT_12 = "POINT (1 0)";
    private static final String POINT_13 = "POINT (20 20);POINT (20 30)";

    private static final String INDEX_1 = "1f0aaaaaaaaaaaaaaa";
    private static final String INDEX_2 = "1f1ffc54fefc54fefc";
    private static final String INDEX_3 = "1f1fffb0ebff104155";
    private static final String INDEX_4 = "1f1fffc410554eb0ff";
    private static final String INDEX_5 = "1f2000228a00228a00";
    private static final String INDEX_6 = "1f2000747900de7300";
    private static final String INDEX_7 = "1f20008a28008a2800";
    private static final String INDEX_8 = "1f2000de7300747900";
    private static final String INDEX_9 = "1f200364bda9c63d03";
    private static final String INDEX_10 = "1f200398c60112ee03";
    private static final String INDEX_11 = "1f35553ac3ffb0ebff";
    private static final String INDEX_12 = "1f35554eb0ffec3aff";
    private static final String INDEX_13_1 = "1f202a02a02a02a02a";
    private static final String INDEX_13_2 = "1f2088888888888888";

    // @formatter:off
    private static final String[] wktData = {
            POINT_1,
            POINT_2,
            POINT_3,
            POINT_4,
            POINT_5,
            POINT_6,
            POINT_7,
            POINT_8,
            POINT_9,
            POINT_10,
            POINT_11,
            POINT_12,
            POINT_13};
    // @formatter:on

    private int maxOrExpansionThreshold = 1;
    private int maxOrFstThreshold = 1;
    private int maxOrRangeThreshold = 1;
    private int maxOrRangeIvarators = 1;
    private int maxRangesPerRangeIvarator = 1;
    private boolean collapseUids = true;

    @Autowired
    @Qualifier("EventQuery")
    protected ShardQueryLogic logic;

    private static AccumuloClient clientForTest;
    private static String fstUri;
    private static List<IvaratorCacheDirConfig> ivaratorCacheDirConfigs;
    private static final IndexIngestUtil ingestUtil = new IndexIngestUtil();

    private ShardQueryLogic currentLogic;
    private List<DefaultEvent> events = new ArrayList<>();
    private int expectedEventCount = -1;
    private List<String> expectedPoints = new ArrayList<>();

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

        // planAndExecuteQuery() invokes extraAssertions() once per index table variant, so match against a
        // local copy rather than destructively consuming the shared expectedPoints list.
        List<String> remaining = new ArrayList<>(expectedPoints);

        for (DefaultEvent event : events) {
            List<String> wkt = new ArrayList<>();

            for (DefaultField field : event.getFields()) {
                if (field.getName().equals(GEO_FIELD))
                    wkt.add(field.getValueString());
            }

            // ensure that this is one of the ingested events
            assertTrue(remaining.removeAll(wkt));
        }

        assertEquals(0, remaining.size());
    }

    @BeforeAll
    public static void setupClass(@TempDir Path tempDir) throws Exception {
        System.setProperty("subject.dn.pattern", "(?:^|,)\\s*OU\\s*=\\s*My Department\\s*(?:,|$)");

        Path fstDir = tempDir.resolve("fst");
        Files.createDirectories(fstDir);
        fstUri = fstDir.toUri().toString();

        Path ivaratorDir = tempDir.resolve("ivarator");
        Files.createDirectories(ivaratorDir);
        ivaratorCacheDirConfigs = Collections.singletonList(new IvaratorCacheDirConfig(ivaratorDir.toUri().toString()));

        setupConfiguration(conf);

        AbstractColumnBasedHandler<Text> dataTypeHandler = new AbstractColumnBasedHandler<>();
        dataTypeHandler.setup(new TaskAttemptContextImpl(conf, new TaskAttemptID()));

        TestIngestHelper ingestHelper = new TestIngestHelper();
        ingestHelper.setup(conf);

        // create and process events with WKT data
        RawRecordContainer record = new RawRecordContainerImpl();
        Multimap<BulkIngestKey,Value> keyValues = HashMultimap.create();
        int recNum = 1;

        for (int i = 0; i < wktData.length; i++) {
            record.clear();
            record.setDataType(new Type(DATA_TYPE_NAME, TestIngestHelper.class, (Class) null, (String[]) null, 1, (String[]) null));
            record.setRawFileName("geodata_" + recNum + ".dat");
            record.setRawRecordNumber(recNum++);
            record.setTimestamp(formatter.parse(BEGIN_DATE).getTime());
            record.setRawData((wktData[i]).getBytes(UTF_8));
            record.generateId(null);
            record.setVisibility(new ColumnVisibility(AUTHS));

            final Multimap<String,NormalizedContentInterface> fields = ingestHelper.getEventFields(record);

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
    }

    public static void setupConfiguration(Configuration conf) {
        conf.set(DATA_TYPE_NAME + BaseIngestHelper.INDEX_FIELDS, GEO_FIELD);
        conf.set(DATA_TYPE_NAME + "." + GEO_FIELD + BaseIngestHelper.FIELD_TYPE, GeometryType.class.getName());

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
            m.put("ns", "20000101_1", new Value());
            bw.addMutation(m);
        }

        // write with the document range only; IndexIngestUtil derives the other shard index table variants
        // that AbstractQueryTest iterates over.
        ingestUtil.write(client, new Authorizations(AUTHS));
    }

    private void runGeoQuery(String query, int expectedCount, List<String> expectedPoints) throws Exception {
        setClientForTest(clientForTest);
        currentLogic = getShardQueryLogic();
        this.expectedEventCount = expectedCount;
        this.expectedPoints = expectedPoints;
        givenQuery(query);
        planAndExecuteQuery();
    }

    @Test
    public void combinedRangesOneIvaratorTest() throws Exception {
        // @formatter:off
        String query = "((_Bounded_ = true) && (" + GEO_QUERY_FIELD + " >= '" + INDEX_1 + "' && " + GEO_QUERY_FIELD + " <= '" + INDEX_3 + "')) || " +
                "((_Bounded_ = true) && (" + GEO_QUERY_FIELD + " >= '" + INDEX_5 + "' && " + GEO_QUERY_FIELD + " <= '" + INDEX_7 + "')) || " +
                "((_Bounded_ = true) && (" + GEO_QUERY_FIELD + " >= '" + INDEX_9 + "' && " + GEO_QUERY_FIELD + " <= '" + INDEX_11 + "'))";
        // @formatter:on

        maxOrExpansionThreshold = 100;
        maxOrFstThreshold = 100;
        maxOrRangeThreshold = 1;
        maxOrRangeIvarators = 1;
        maxRangesPerRangeIvarator = 1;

        if (!logic.isUseDocumentScheduler()) {
            // this call is not compatible with the document scheduler
            List<String> queryRanges = getQueryRanges(query);

            assertEquals(1, queryRanges.size());
            String id = queryRanges.get(0).substring(queryRanges.get(0).indexOf("id = '") + 6,
                            queryRanges.get(0).indexOf("') && (field = '" + GEO_QUERY_FIELD + "')"));
            assertEquals("((_List_ = true) && ((id = '" + id + "') && (field = '" + GEO_QUERY_FIELD
                            + "') && (params = '{\"ranges\":[[\"[1f0aaaaaaaaaaaaaaa\",\"1f1fffb0ebff104155]\"],[\"[1f2000228a00228a00\",\"1f20008a28008a2800]\"],[\"[1f200364bda9c63d03\",\"1f35553ac3ffb0ebff]\"]]}')))",
                            queryRanges.get(0));
        }

        List<String> pointList = new ArrayList<>();
        pointList.addAll(Arrays.asList(POINT_1, POINT_2, POINT_3, POINT_5, POINT_6, POINT_7, POINT_9, POINT_10, POINT_11));
        pointList.addAll(Arrays.asList(POINT_13.split(";")));

        runGeoQuery(query, 10, pointList);
    }

    @Test
    public void combinedRangesTwoIvaratorsTest() throws Exception {
        // @formatter:off
        String query = "((_Bounded_ = true) && (" + GEO_QUERY_FIELD + " >= '" + INDEX_1 + "' && " + GEO_QUERY_FIELD + " <= '" + INDEX_3 + "')) || " +
                "((_Bounded_ = true) && (" + GEO_QUERY_FIELD + " >= '" + INDEX_5 + "' && " + GEO_QUERY_FIELD + " <= '" + INDEX_7 + "')) || " +
                "((_Bounded_ = true) && (" + GEO_QUERY_FIELD + " >= '" + INDEX_9 + "' && " + GEO_QUERY_FIELD + " <= '" + INDEX_11 + "'))";
        // @formatter:on

        maxOrExpansionThreshold = 100;
        maxOrFstThreshold = 100;
        maxOrRangeIvarators = 10;
        maxOrRangeThreshold = 1;
        maxRangesPerRangeIvarator = 2;

        if (!logic.isUseDocumentScheduler()) {
            // this call is not compatible with the document scheduler
            List<String> queryRanges = getQueryRanges(query);

            assertEquals(1, queryRanges.size());
            String id = queryRanges.get(0).substring(queryRanges.get(0).indexOf("id = '") + 6,
                            queryRanges.get(0).indexOf("') && (field = '" + GEO_QUERY_FIELD + "')"));
            assertEquals("((_Value_ = true) && ((_Bounded_ = true) && (" + GEO_QUERY_FIELD + " >= '1f200364bda9c63d03' && " + GEO_QUERY_FIELD
                            + " <= '1f35553ac3ffb0ebff'))) || ((_List_ = true) && ((id = '" + id + "') && (field = '" + GEO_QUERY_FIELD
                            + "') && (params = '{\"ranges\":[[\"[1f0aaaaaaaaaaaaaaa\",\"1f1fffb0ebff104155]\"],[\"[1f2000228a00228a00\",\"1f20008a28008a2800]\"]]}')))",
                            queryRanges.get(0));
        }

        List<String> pointList = new ArrayList<>();
        pointList.addAll(Arrays.asList(POINT_1, POINT_2, POINT_3, POINT_5, POINT_6, POINT_7, POINT_9, POINT_10, POINT_11));
        pointList.addAll(Arrays.asList(POINT_13.split(";")));

        runGeoQuery(query, 10, pointList);
    }

    @Test
    public void combinedRangesWithNegationTest() throws Exception {
        // @formatter:off
        String query = "((_Bounded_ = true) && (" + GEO_QUERY_FIELD + " >= '" + INDEX_1 + "' && " + GEO_QUERY_FIELD + " <= '" + INDEX_12 + "')) && " +
                "not(((_Bounded_ = true) && (" + GEO_QUERY_FIELD + " >= '" + INDEX_1 + "' && " + GEO_QUERY_FIELD + " <= '" + INDEX_3 + "')) || " +
                "((_Bounded_ = true) && (" + GEO_QUERY_FIELD + " >= '" + INDEX_5 + "' && " + GEO_QUERY_FIELD + " <= '" + INDEX_7 + "')) || " +
                "((_Bounded_ = true) && (" + GEO_QUERY_FIELD + " >= '" + INDEX_9 + "' && " + GEO_QUERY_FIELD + " <= '" + INDEX_11 + "')))";
        // @formatter:on

        maxOrExpansionThreshold = 100;
        maxOrFstThreshold = 100;
        maxOrRangeThreshold = 1;
        maxOrRangeIvarators = 10;
        maxRangesPerRangeIvarator = 1;

        List<String> pointList = new ArrayList<>(Arrays.asList(POINT_4, POINT_8, POINT_12));

        runGeoQuery(query, 3, pointList);
    }

    @Test
    public void valueListTest() throws Exception {
        // @formatter:off
        String query = "(" + GEO_QUERY_FIELD + " == '" + INDEX_1 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_2 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_3 + "' || " +
                GEO_QUERY_FIELD + " == '" + INDEX_5 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_6 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_7 + "' || " +
                GEO_QUERY_FIELD + " == '" + INDEX_9 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_10 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_11 + "')";
        // @formatter:on

        maxOrExpansionThreshold = 1;
        maxOrFstThreshold = 100;
        maxOrRangeThreshold = 100;
        maxOrRangeIvarators = 1;
        maxRangesPerRangeIvarator = 1;

        List<String> pointList = new ArrayList<>(Arrays.asList(POINT_1, POINT_2, POINT_3, POINT_5, POINT_6, POINT_7, POINT_9, POINT_10, POINT_11));

        runGeoQuery(query, 9, pointList);
    }

    @Test
    public void docSpecificValueListTest() throws Exception {
        // @formatter:off
        String query = "(" + GEO_QUERY_FIELD + " == '" + INDEX_13_1 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_13_2 + "')";
        // @formatter:on

        maxOrExpansionThreshold = 1;
        maxOrFstThreshold = 100;
        maxOrRangeThreshold = 100;
        maxOrRangeIvarators = 1;
        maxRangesPerRangeIvarator = 1;
        collapseUids = false;

        List<String> pointList = new ArrayList<>(Arrays.asList(POINT_13.split(";")));

        runGeoQuery(query, 1, pointList);
    }

    @Test
    public void valueListWithNegationTest() throws Exception {
        // @formatter:off
        String query = "((_Bounded_ = true) && (" + GEO_QUERY_FIELD + " >= '" + INDEX_1 + "' && " + GEO_QUERY_FIELD + " <= '" + INDEX_12 + "')) && " +
                "not(" + GEO_QUERY_FIELD + " == '" + INDEX_1 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_2 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_3 + "' || " +
                GEO_QUERY_FIELD + " == '" + INDEX_5 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_6 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_7 + "' || " +
                GEO_QUERY_FIELD + " == '" + INDEX_9 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_10 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_11 + "')";
        // @formatter:on

        maxOrExpansionThreshold = 1;
        maxOrFstThreshold = 100;
        maxOrRangeThreshold = 100;
        maxOrRangeIvarators = 1;
        maxRangesPerRangeIvarator = 1;

        List<String> pointList = new ArrayList<>();
        pointList.addAll(Arrays.asList(POINT_4, POINT_8, POINT_12));
        pointList.addAll(Arrays.asList(POINT_13.split(";")));

        runGeoQuery(query, 4, pointList);
    }

    @Test
    public void fstTest() throws Exception {
        // @formatter:off
        String query = "(" + GEO_QUERY_FIELD + " == '" + INDEX_1 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_2 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_3 + "' || " +
                "" + GEO_QUERY_FIELD + " == '" + INDEX_5 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_6 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_7 + "' || " +
                "" + GEO_QUERY_FIELD + " == '" + INDEX_9 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_10 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_11 + "')";
        // @formatter:on

        maxOrExpansionThreshold = 100;
        maxOrFstThreshold = 1;
        maxOrRangeThreshold = 100;
        maxOrRangeIvarators = 1;
        maxRangesPerRangeIvarator = 1;

        List<String> pointList = new ArrayList<>(Arrays.asList(POINT_1, POINT_2, POINT_3, POINT_5, POINT_6, POINT_7, POINT_9, POINT_10, POINT_11));

        runGeoQuery(query, 9, pointList);
    }

    @Test
    public void fstWithNegationTest() throws Exception {
        // @formatter:off
        String query = "((_Bounded_ = true) && (" + GEO_QUERY_FIELD + " >= '" + INDEX_1 + "' && " + GEO_QUERY_FIELD + " <= '" + INDEX_12 + "')) && " +
                "not(" + GEO_QUERY_FIELD + " == '" + INDEX_1 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_2 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_3 + "' || " +
                "" + GEO_QUERY_FIELD + " == '" + INDEX_5 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_6 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_7 + "' || " +
                "" + GEO_QUERY_FIELD + " == '" + INDEX_9 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_10 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_11 + "')";
        // @formatter:on

        maxOrExpansionThreshold = 100;
        maxOrFstThreshold = 1;
        maxOrRangeThreshold = 100;
        maxOrRangeIvarators = 1;
        maxRangesPerRangeIvarator = 1;

        List<String> pointList = new ArrayList<>();
        pointList.addAll(Arrays.asList(POINT_4, POINT_8, POINT_12));
        pointList.addAll(Arrays.asList(POINT_13.split(";")));

        runGeoQuery(query, 4, pointList);
    }

    private List<String> getQueryRanges(String queryString) throws Exception {
        ShardQueryLogic rangeLogic = getShardQueryLogic();

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

        Iterator<String> iter = Iterators.transform(config.getQueriesIter(), queryData -> {
            try {
                return JexlStringBuildingVisitor
                                .buildQuery(PushdownLargeFieldedListsVisitor.pushdown(config, JexlASTHelper.parseJexlQuery(queryData.getQuery()), null, null));
            } catch (ParseException e) {
                return null;
            }
        });

        List<String> queryData = new ArrayList<>();
        while (iter.hasNext())
            queryData.add(iter.next());
        return queryData;
    }

    private ShardQueryLogic getShardQueryLogic() throws IOException {
        ShardQueryLogic clonedLogic = new ShardQueryLogic(this.logic);

        // increase the depth threshold
        clonedLogic.setMaxDepthThreshold(20);

        // set the pushdown threshold really high to avoid collapsing uids into shards (overrides setCollapseUids if #terms is greater than this threshold)
        ((DefaultQueryPlanner) (clonedLogic.getQueryPlanner())).setPushdownThreshold(1000000);

        URL hdfsSiteConfig = this.getClass().getResource("/testhadoop.config");
        clonedLogic.setHdfsSiteConfigURLs(hdfsSiteConfig.toExternalForm());

        setupIvarator(clonedLogic);

        return clonedLogic;
    }

    private void setupIvarator(ShardQueryLogic logic) {
        // Set these to ensure ivarator runs
        logic.setMaxUnfieldedExpansionThreshold(1);
        logic.setMaxValueExpansionThreshold(1);
        logic.setMaxOrExpansionThreshold(maxOrExpansionThreshold);
        logic.setMaxOrExpansionFstThreshold(maxOrFstThreshold);
        logic.setMaxOrRangeThreshold(maxOrRangeThreshold);
        logic.setMaxOrRangeIvarators(maxOrRangeIvarators);
        logic.setMaxRangesPerRangeIvarator(maxRangesPerRangeIvarator);
        logic.setIvaratorFstHdfsBaseURIs(fstUri);
        logic.setCollapseUids(collapseUids);
        logic.setIvaratorCacheScanPersistThreshold(1);
        logic.setIvaratorCacheDirConfigs(ivaratorCacheDirConfigs);
    }

    public static class TestIngestHelper extends ContentBaseIngestHelper {
        @Override
        public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer record) {
            Multimap<String,NormalizedContentInterface> eventFields = HashMultimap.create();
            String rawRecord = new String(record.getRawData());
            for (String value : rawRecord.split(";")) {
                NormalizedContentInterface geo_nci = new NormalizedFieldAndValue(GEO_FIELD, value);
                eventFields.put(GEO_FIELD, geo_nci);
            }
            return normalizeMap(eventFields);
        }
    }
}
