package datawave.query.planner;

import static datawave.microservice.query.QueryParameters.QUERY_AUTHORIZATIONS;
import static datawave.microservice.query.QueryParameters.QUERY_BEGIN;
import static datawave.microservice.query.QueryParameters.QUERY_END;
import static datawave.microservice.query.QueryParameters.QUERY_EXPIRATION;
import static datawave.microservice.query.QueryParameters.QUERY_LOGIC_NAME;
import static datawave.microservice.query.QueryParameters.QUERY_NAME;
import static datawave.microservice.query.QueryParameters.QUERY_PERSISTENCE;
import static datawave.microservice.query.QueryParameters.QUERY_STRING;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.configuration.spring.SpringBean;
import datawave.core.query.configuration.QueryData;
import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.BaseNormalizedContent;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.ContentBaseIngestHelper;
import datawave.ingest.mapreduce.handler.shard.AbstractColumnBasedHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.microservice.query.DefaultQueryParameters;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.microservice.query.QueryParameters;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.GeoWaveQueryInfoVisitor;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.testframework.MockStatusReporter;
import datawave.util.TableName;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;

@RunWith(Arquillian.class)
public class GeoSortedQueryDataTest {

    private static final int NUM_SHARDS = 241;
    private static final String DATA_TYPE_NAME = "wkt";
    private static final String INGEST_HELPER_CLASS = TestIngestHelper.class.getName();
    private static final String FIELD_NAME = "GEO_FIELD";

    private static final String AUTHS = "ALL";

    private static final String formatPattern = "yyyyMMdd HHmmss.SSS";
    private static final SimpleDateFormat formatter = new SimpleDateFormat(formatPattern);

    private static final String BEGIN_DATE = "20000101 000000.000";
    private static final String END_DATE = "20500101 000000.000";

    private static final String USER = "testcorp";
    private static final String USER_DN = "cn=test.testcorp.com, ou=datawave, ou=development, o=testcorp, c=us";

    private static final Configuration conf = new Configuration();

    // @formatter:off
    private static final String[] wktData = {
            "POINT(0 0)",
            "POINT(30 -85)",
            "POINT(-45 17)",

            "POLYGON((10 10, -10 10, -10 -10, 10 -10, 10 10))",
            "POLYGON((25 25, 5 25, 5 5, 25 5, 25 25))",
            "POLYGON((-20 -20, -40 -20, -40 -40, -20 -40, -20 -20))",

            "POLYGON((45 45, -45 45, -45 -45, 45 -45, 45 45))",
            "POLYGON((90 45, 0 45, 0 -45, 90 -45, 90 45))",
            "POLYGON((45 15, -45 15, -45 -60, 45 -60, 45 15))",

            "POLYGON((90 90, -90 90, -90 -90, 90 -90, 90 90))",
            "POLYGON((180 90, 0 90, 0 -90, 180 -90, 180 90))",
            "POLYGON((90 0, -90 0, -90 -180, 90 -180, 90 0))"};

    private static final long[] dates = {
            0,
            TimeUnit.DAYS.toMillis(90),
            TimeUnit.DAYS.toMillis(180),

            0,
            TimeUnit.DAYS.toMillis(90),
            TimeUnit.DAYS.toMillis(180),

            0,
            TimeUnit.DAYS.toMillis(90),
            TimeUnit.DAYS.toMillis(180),

            0,
            TimeUnit.DAYS.toMillis(90),
            TimeUnit.DAYS.toMillis(180)};
    // @formatter:on

    private static final String QUERY_WKT = "POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90))";

    @Inject
    @SpringBean(name = "EventQuery")
    ShardQueryLogic logic;

    private static InMemoryInstance instance;

    @Deployment
    public static JavaArchive createDeployment() throws Exception {
        return ShrinkWrap.create(JavaArchive.class)
                        .addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi", "datawave.query", "datawave.webservice.query.result.event",
                                        "datawave.core.query.result.event")
                        .deleteClass(DefaultEdgeEventQueryLogic.class).deleteClass(RemoteEdgeDictionary.class)
                        .deleteClass(datawave.query.metrics.QueryMetricQueryLogic.class)
                        .addAsManifestResource(new StringAsset(
                                        "<alternatives>" + "<stereotype>datawave.query.tables.edge.MockAlternative</stereotype>" + "</alternatives>"),
                                        "beans.xml");
    }

    @BeforeClass
    public static void setupClass() throws Exception {
        setupEnvVariables();
        conf.addResource(ClassLoader.getSystemResource("datawave/query/tables/geo-test-config.xml"));
        resolveEnvVariables(conf);

        TypeRegistry.reset();
        TypeRegistry registry = TypeRegistry.getInstance(conf);

        TaskAttemptContext ctx = new TaskAttemptContextImpl(conf, new TaskAttemptID());

        AbstractColumnBasedHandler<Text> dataTypeHandler = new AbstractColumnBasedHandler<>();
        dataTypeHandler.setup(ctx);

        TestIngestHelper ingestHelper = (TestIngestHelper) dataTypeHandler.getHelper(registry.get(DATA_TYPE_NAME));

        // create and process events with WKT data
        RawRecordContainer record = new RawRecordContainerImpl();
        Multimap<BulkIngestKey,Value> keyValues = HashMultimap.create();
        int recNum = 1;
        for (int i = 0; i < wktData.length; i++) {
            record.clear();
            record.setDataType(new Type(DATA_TYPE_NAME, TestIngestHelper.class, (Class) null, (String[]) null, 1, (String[]) null));
            record.setRawFileName("geodata_" + recNum + ".dat");
            record.setRawRecordNumber(recNum++);
            record.setDate(formatter.parse(BEGIN_DATE).getTime() + dates[i]);
            record.setRawData(wktData[i].getBytes("UTF8"));
            record.generateId(null);
            record.setVisibility(new ColumnVisibility(AUTHS));

            final Multimap<String,NormalizedContentInterface> fields = ingestHelper.getEventFields(record);

            Multimap kvPairs = dataTypeHandler.processBulk(new Text(), record, fields, new MockStatusReporter());

            keyValues.putAll(kvPairs);

            dataTypeHandler.getMetadata().addEvent(ingestHelper, record, fields);
        }

        keyValues.putAll(dataTypeHandler.getMetadata().getBulkMetadata());

        // write these values to their respective tables
        instance = new InMemoryInstance();
        AccumuloClient client = new InMemoryAccumuloClient("root", instance);
        client.securityOperations().changeUserAuthorizations("root", new Authorizations(AUTHS));

        writeKeyValues(client, keyValues);
    }

    public static void setupEnvVariables() {
        System.setProperty("subject.dn.pattern", "(?:^|,)\\s*OU\\s*=\\s*My Department\\s*(?:,|$)");
        System.setProperty("NUM_SHARDS", Integer.toString(NUM_SHARDS));
        System.setProperty("SHARD_TABLE_NAME", TableName.SHARD);
        System.setProperty("ERROR_SHARD_TABLE_NAME", TableName.ERROR_SHARD);
        System.setProperty("SHARD_INDEX_TABLE_NAME", TableName.SHARD_INDEX);
        System.setProperty("SHARD_REVERSE_INDEX_TABLE_NAME", TableName.SHARD_RINDEX);
        System.setProperty("METADATA_TABLE_NAME", TableName.METADATA);
        System.setProperty("DATA_TYPE_NAME", DATA_TYPE_NAME);
        System.setProperty("INGEST_HELPER_CLASS", INGEST_HELPER_CLASS);
        System.setProperty("FIELD_NAME", FIELD_NAME);
    }

    public static void resolveEnvVariables(Configuration conf) {
        StringBuilder sb = new StringBuilder();
        Pattern p = Pattern.compile("\\$\\{(\\w+)\\}|\\$(\\w+)");
        for (Map.Entry<String,String> entry : conf) {
            boolean reset = false;
            Matcher m = p.matcher(entry.getKey());

            while (m.find()) {
                String envVarName = null == m.group(1) ? m.group(2) : m.group(1);
                String envVarValue = System.getProperty(envVarName);
                m.appendReplacement(sb, null == envVarValue ? "" : Matcher.quoteReplacement(envVarValue));
                reset = true;
            }
            m.appendTail(sb);
            String key = sb.toString();

            m = p.matcher(entry.getValue());
            sb.setLength(0);
            while (m.find()) {
                String envVarName = null == m.group(1) ? m.group(2) : m.group(1);
                String envVarValue = System.getProperty(envVarName);
                m.appendReplacement(sb, null == envVarValue ? "" : Matcher.quoteReplacement(envVarValue));
                reset = true;
            }
            m.appendTail(sb);
            String value = sb.toString();
            sb.setLength(0);

            if (reset) {
                conf.unset(entry.getKey());
                conf.set(key, value);
            }
        }
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
                mutation.put(biKey.getKey().getColumnFamily(), biKey.getKey().getColumnQualifier(), biKey.getKey().getColumnVisibilityParsed(), val);
                writer.addMutation(mutation);
            }
            writer.close();
        }
    }

    @Before
    public void setupTest() {
        // increase the depth threshold
        logic.setMaxDepthThreshold(10);

        logic.setIntermediateMaxTermThreshold(50);
        logic.setIndexedMaxTermThreshold(50);

        // set the pushdown threshold really high to avoid collapsing uids into shards (overrides setCollapseUids if #terms is greater than this threshold)
        ((DefaultQueryPlanner) (logic.getQueryPlanner())).setPushdownThreshold(1000000);
    }

    @Test
    public void testSortedGeoRanges() throws Exception {
        logic.setSortGeoWaveQueryRanges(true);

        Iterator<QueryData> queryIter = initializeGeoQuery();

        // ensure that the queries are sorted geo granularity (high to low)
        GeoWaveQueryInfoVisitor visitor = new GeoWaveQueryInfoVisitor(Arrays.asList(FIELD_NAME));
        GeoWaveQueryInfoVisitor.GeoWaveQueryInfo prevQueryInfo = null;
        while (queryIter.hasNext()) {
            QueryData qd = queryIter.next();
            ASTJexlScript queryTree = JexlASTHelper.parseJexlQuery(qd.getQuery());
            GeoWaveQueryInfoVisitor.GeoWaveQueryInfo queryInfo = visitor.parseGeoWaveQueryInfo(queryTree);
            if (prevQueryInfo != null)
                assertTrue(prevQueryInfo.compareTo(queryInfo) <= 0);
            prevQueryInfo = queryInfo;
        }
    }

    @Test
    public void testIntermediateMaxTermCountEnforcement() {
        logic.setSortGeoWaveQueryRanges(true);
        logic.setIntermediateMaxTermThreshold(10);

        assertThrows(DatawaveFatalQueryException.class, this::initializeGeoQuery);
    }

    @Test
    public void testUnsortedGeoRanges() throws Exception {
        logic.setSortGeoWaveQueryRanges(false);

        Iterator<QueryData> queryIter = initializeGeoQuery();

        // ensure that the queries are sorted by shard (ascending)
        String prevShard = null;
        while (queryIter.hasNext()) {
            QueryData qd = queryIter.next();
            Range range = qd.getRanges().iterator().next();
            if (prevShard != null)
                assertTrue(prevShard.compareTo(range.getStartKey().getRow().toString()) <= 0);
            prevShard = range.getStartKey().getRow().toString();
        }
    }

    private Iterator<QueryData> initializeGeoQuery() throws Exception {
        MultivaluedMap<String,String> params = new MultivaluedMapImpl<>();
        params.putSingle(QUERY_STRING, "geowave:intersects(" + FIELD_NAME + ", '" + QUERY_WKT + "')");
        params.putSingle(QUERY_NAME, "geoQuery");
        params.putSingle(QUERY_LOGIC_NAME, "EventQueryLogic");
        params.putSingle(QUERY_PERSISTENCE, "PERSISTENT");
        params.putSingle(QUERY_AUTHORIZATIONS, AUTHS);
        params.putSingle(QUERY_EXPIRATION, "20200101 000000.000");
        params.putSingle(QUERY_BEGIN, BEGIN_DATE);
        params.putSingle(QUERY_END, END_DATE);

        QueryParameters queryParams = new DefaultQueryParameters();
        queryParams.validate(params);

        Set<Authorizations> auths = new HashSet<>();
        auths.add(new Authorizations(AUTHS));

        Query query = new QueryImpl();
        query.initialize(USER, Arrays.asList(USER_DN), null, queryParams, null);

        ShardQueryConfiguration config = ShardQueryConfiguration.create(logic, query);

        logic.initialize(config, new InMemoryAccumuloClient("root", instance), query, auths);

        logic.setupQuery(config);

        return config.getQueriesIter();
    }

    public static class TestIngestHelper extends ContentBaseIngestHelper {
        @Override
        public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer record) {
            Multimap<String,NormalizedContentInterface> eventFields = HashMultimap.create();
            NormalizedContentInterface nci = new BaseNormalizedContent();
            nci.setFieldName(FIELD_NAME);
            nci.setEventFieldValue(new String(record.getRawData()));
            nci.setIndexedFieldValue(new String(record.getRawData()));
            eventFields.put(FIELD_NAME, nci);
            return normalizeMap(eventFields);
        }
    }
}
