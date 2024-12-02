package datawave.query.discovery;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.MapWritable;
import org.apache.log4j.Logger;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.result.event.DefaultResponseObjectFactory;
import datawave.data.type.LcNoDiacriticsType;
import datawave.ingest.protobuf.Uid;
import datawave.marking.MarkingFunctions;
import datawave.microservice.query.QueryImpl;
import datawave.query.MockAccumuloRecordWriter;
import datawave.query.QueryTestTableHelper;
import datawave.query.util.MetadataHelperFactory;
import datawave.util.TableName;

public class DiscoveryLogicTest {

    private static final Logger log = Logger.getLogger(DiscoveryLogicTest.class);

    private static final Value BLANK_VALUE = new Value(new byte[0]);
    private static final Set<Authorizations> AUTHS = Collections.singleton(new Authorizations("FOO", "BAR"));
    private static final String QUERY_AUTHS = "FOO,BAR";

    private final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyyMMdd");
    private AccumuloClient client = null;
    private DiscoveryLogic logic;

    private String query;
    private String startDate;
    private String endDate;
    private Map<String,String> parameters = new HashMap<>();

    private final List<DiscoveredThing> expected = new ArrayList<>();

    @BeforeClass
    public static void setUp() {
        System.setProperty(MetadataHelperFactory.ALL_AUTHS_PROPERTY, QUERY_AUTHS);
    }

    @Before
    public void setup() throws Throwable {
        initClient();
        writeData();
        initLogic();
    }

    private void initClient() throws AccumuloException, TableNotFoundException, TableExistsException, AccumuloSecurityException {
        QueryTestTableHelper testTableHelper = new QueryTestTableHelper(DiscoveryLogicTest.class.getCanonicalName(), log);
        MockAccumuloRecordWriter recordWriter = new MockAccumuloRecordWriter();
        testTableHelper.configureTables(recordWriter);
        this.client = testTableHelper.client;
    }

    private void writeData() throws Throwable {
        writeEntries("VEHICLE", "motorcycle", "csv", "FOO", "20130101", 10, 20, 2);
        writeEntries("VEHICLE", "motorcycle", "csv", "FOO", "20130102", 10, 20, 2);
        writeEntries("ROCK", "onyx", "csv", "FOO", "20130101", 1, 1, 1);
        writeEntries("ROCK", "onyx", "csv", "FOO", "20130102", 1, 3, 4);
        writeEntries("ROCK", "onyx", "csv", "FOO", "20130103", 1, 3, 3);
        writeEntries("POKEMON", "onyx", "csv", "FOO", "20130101", 20, 5, 5);
        writeEntries("POKEMON", "onyx", "csv", "FOO", "20130102", 10, 1, 1);
        writeEntries("POKEMON", "onyx", "csv", "FOO", "20130103", 1, 1, 22);
        writeEntries("ROOSTER", "onyx", "csv", "BAR", "20130101", 5, 24, 2);
        writeEntries("ROOSTER", "onyx", "csv", "BAR", "20130102", 5, 24, 2);
        writeEntries("ROOSTER", "onyx", "csv", "BAR", "20130103", 5, 24, 20);
        writeEntries("NETWORK", "bbc", "csv", "FOO", "20130101", 10, 24, 20);
        writeEntries("NETWORK", "bbc", "csv", "FOO", "20130102", 10, 24, 20);
        writeEntries("NETWORK", "bbc", "csv", "FOO", "20130103", 10, 24, 20);
        writeEntries("OCCUPATION", "skydiver", "text", "FOO", "20130101", 10, 10, 5);
        writeEntries("OCCUPATION", "skydiver", "text", "FOO", "20130102", 10, 10, 5);
        writeEntries("OCCUPATION", "skydiver", "text", "FOO", "20130103", 10, 10, 5);
        writeEntries("OCCUPATION", "skydiver", "text", "FOO", "20130104", 10, 10, 5);
        writeEntries("OCCUPATION", "xxx.skydiver", "text", "FOO", "20130101", 10, 10, 5);
        writeEntries("OCCUPATION", "xxx.skydiver", "text", "FOO", "20130102", 10, 10, 5);
        writeEntries("OCCUPATION", "xxx.skydiver", "text", "FOO", "20130103", 10, 10, 5);
        writeEntries("OCCUPATION", "xxx.skydiver", "text", "FOO", "20130104", 10, 10, 5);
        writeEntries("OCCUPATION", "yyy.skydiver", "text", "FOO", "20130101", 10, 10, 5);
        writeEntries("OCCUPATION", "yyy.skydiver", "text", "FOO", "20130102", 10, 10, 5);
        writeEntries("OCCUPATION", "yyy.skydiver", "text", "FOO", "20130103", 10, 10, 5);
        writeEntries("OCCUPATION", "yyy.skydiver", "text", "FOO", "20130104", 10, 10, 5);
        writeEntries("JOB", "skydiver", "text", "BAR", "20130101", 10, 10, 5);
        writeEntries("JOB", "skydiver", "text", "BAR", "20130102", 10, 10, 5);
        writeEntries("JOB", "skydiver", "text", "BAR", "20130103", 10, 10, 5);
        writeEntries("JOB", "skydiver", "text", "BAR", "20130104", 10, 10, 5);
        writeEntries("JOB", "police officer", "idem", "FOO", "20130101", 15, 15, 5);
        writeEntries("JOB", "police officer", "idem", "FOO", "20130102", 15, 15, 5);
        writeEntries("JOB", "police officer", "idem", "FOO", "20130103", 15, 15, 5);
        writeEntries("PRIZE", "trophy", "idem", "FOO", "20130101", 1, 5, 5);
        writeEntries("PRIZE", "trophy", "idem", "FOO", "20130102", 1, 5, 5);
        writeEntries("PRIZE", "trophy", "idem", "FOO", "20130103", 1, 5, 5);
        writeEntries("PRIZE", "trophy", "idem", "FOO", "20130104", 1, 5, 5);
        writeEntries("FLOCK", "rooster", "stock", "BAR", "20130101", 2, 15, 5);
        writeEntries("FLOCK", "rooster", "stock", "BAR", "20130102", 2, 15, 5);
        writeEntries("FLOCK", "rooster", "stock", "BAR", "20130103", 2, 15, 5);
        writeEntries("BIRD", "ruddy duck", "stock", "FOO", "20130101", 20, 15, 2);
        writeEntries("BIRD", "ruddy duck", "stock", "FOO", "20130102", 20, 15, 2);
        writeEntries("BIRD", "ruddy duck", "stock", "FOO", "20130103", 20, 15, 2);
        writeEntries("VEHICLE", "ranger", "stock", "FOO", "20130101", 20, 15, 2);
        writeEntries("VEHICLE", "ranger", "stock", "BAR", "20130101", 1, 1, 2);
        writeEntries("VEHICLE", "ranger", "stock", "FOO", "20130102", 20, 15, 2);
        writeEntries("VEHICLE", "ranger", "stock", "BAR", "20130102", 5, 5, 5);
        writeEntries("VEHICLE", "ranger", "stock", "FOO", "20130103", 20, 15, 2);
        writeEntries("VEHICLE", "ranger", "stock", "BAR", "20130103", 6, 1, 2);
        writeEntries("NON_INDEXED_FIELD", "coffee", "csv", "FOO", "20130101", 1, 1, 1);
        writeEntries("NON_INDEXED_FIELD", "espresso", "csv", "FOO", "20130102", 1, 5, 5);

        writeForwardModel("ANIMAL", "ROOSTER");
        writeForwardModel("ANIMAL", "BIRD");
        writeReverseModel("occupation", "job");
    }

    private void writeEntries(String field, String term, String datatype, String visibility, String dateStr, int numShards, int uidListCount, int uidListSize)
                    throws Exception {
        BatchWriterConfig config = new BatchWriterConfig().setMaxMemory(1024L).setMaxLatency(1, TimeUnit.SECONDS).setMaxWriteThreads(1);
        ColumnVisibility columnVisibility = new ColumnVisibility(visibility);
        Date date = dateFormatter.parse(dateStr);

        try (BatchWriter writer = client.createBatchWriter(QueryTestTableHelper.METADATA_TABLE_NAME, config)) {
            Mutation mutation = new Mutation(field);
            mutation.put("t", datatype + "\u0000" + LcNoDiacriticsType.class.getName(), columnVisibility, BLANK_VALUE);
            if (!field.equals("NON_INDEXED_FIELD")) {
                mutation.put("i", datatype + "\u0000" + dateStr, columnVisibility, new Value(SummingCombiner.VAR_LEN_ENCODER.encode(1L)));
            }
            mutation.put("ri", datatype + "\u0000" + dateStr, columnVisibility, new Value(SummingCombiner.VAR_LEN_ENCODER.encode(1L)));
            writer.addMutation(mutation);
        }

        try (BatchWriter writer = client.createBatchWriter(TableName.SHARD_INDEX, config)) {
            Mutation mutation = new Mutation(term);
            for (int i = 0; i < numShards; i++) {
                mutation.put(field, dateStr + "_" + i + "\u0000" + datatype, columnVisibility, date.getTime(), createUidListValue(uidListCount, uidListSize));
            }
            writer.addMutation(mutation);
        }

        try (BatchWriter writer = client.createBatchWriter(TableName.SHARD_RINDEX, config)) {
            Mutation mutation = new Mutation(new StringBuilder(term).reverse().toString());
            for (int i = 0; i < numShards; i++) {
                mutation.put(field, dateStr + "_" + i + "\u0000" + datatype, columnVisibility, date.getTime(), createUidListValue(uidListCount, uidListSize));
            }
            writer.addMutation(mutation);
        }
    }

    private Value createUidListValue(int count, int listSize) {
        Uid.List.Builder builder = Uid.List.newBuilder().setIGNORE(true).setCOUNT(count);
        for (int i = 0; i < listSize; i++) {
            builder.addUID(UUID.randomUUID().toString());
        }
        return new Value(builder.build().toByteArray());
    }

    private void writeForwardModel(String from, String to) throws Throwable {
        BatchWriterConfig config = new BatchWriterConfig().setMaxMemory(1024L).setMaxLatency(1, TimeUnit.SECONDS).setMaxWriteThreads(1);
        ColumnVisibility viz = new ColumnVisibility("FOO");

        try (BatchWriter writer = client.createBatchWriter(QueryTestTableHelper.METADATA_TABLE_NAME, config)) {
            Mutation m = new Mutation(from);
            m.put("DATAWAVE", to + "\u0000forward", viz, BLANK_VALUE);
            writer.addMutation(m);
        }
    }

    private void writeReverseModel(String from, String to) throws Throwable {
        BatchWriterConfig config = new BatchWriterConfig().setMaxMemory(1024L).setMaxLatency(1, TimeUnit.SECONDS).setMaxWriteThreads(1);
        ColumnVisibility viz = new ColumnVisibility("FOO");

        try (BatchWriter writer = client.createBatchWriter(QueryTestTableHelper.METADATA_TABLE_NAME, config)) {
            Mutation m = new Mutation(from);
            m.put("DATAWAVE", to + "\u0000reverse", viz, BLANK_VALUE);
            writer.addMutation(m);
        }
    }

    private void initLogic() {
        logic = new DiscoveryLogic();
        logic.setIndexTableName(TableName.SHARD_INDEX);
        logic.setReverseIndexTableName(TableName.SHARD_RINDEX);
        logic.setModelTableName(QueryTestTableHelper.METADATA_TABLE_NAME);
        logic.setMetadataTableName(QueryTestTableHelper.METADATA_TABLE_NAME);
        logic.setModelName("DATAWAVE");
        logic.setFullTableScanEnabled(false);
        logic.setMaxResults(-1);
        logic.setMaxWork(-1);
        logic.setAllowLeadingWildcard(true);
        logic.setResponseObjectFactory(new DefaultResponseObjectFactory());
        logic.setMarkingFunctions(new MarkingFunctions.Default());
        logic.setMetadataHelperFactory(new MetadataHelperFactory());
    }

    @After
    public void tearDown() throws Exception {
        query = null;
        startDate = null;
        endDate = null;
        parameters.clear();
        expected.clear();
    }

    private void assertQueryResults() throws Exception {
        QueryImpl settings = new QueryImpl();
        settings.setBeginDate(dateFormatter.parse(startDate));
        settings.setEndDate(dateFormatter.parse(endDate));
        settings.setPagesize(Integer.MAX_VALUE);
        settings.setQueryAuthorizations(QUERY_AUTHS);
        settings.setQuery(query);
        settings.setId(UUID.randomUUID());
        settings.addParameters(this.parameters);

        GenericQueryConfiguration config = logic.initialize(client, settings, AUTHS);
        logic.setupQuery(config);
        Iterator<DiscoveredThing> iterator = logic.iterator();
        List<DiscoveredThing> actual = new ArrayList<>();
        while (iterator.hasNext()) {
            actual.add(iterator.next());
        }

        Assertions.assertThat(actual).hasSize(expected.size());
        for (int i = 0; i < expected.size(); i++) {
            DiscoveredThing actualThing = actual.get(i);
            DiscoveredThing expectedThing = expected.get(i);
            Assertions.assertThat(actualThing).isEqualTo(expectedThing);
            Assertions.assertThat(actualThing.getCountsByColumnVisibility()).isEqualTo(expectedThing.getCountsByColumnVisibility());
        }
    }

    private void givenQuery(String query) {
        this.query = query;
    }

    private void givenStartDate(String startDate) {
        this.startDate = startDate;
    }

    private void givenEndDate(String endDate) {
        this.endDate = endDate;
    }

    private void givenParameter(String parameter, String value) {
        this.parameters.put(parameter, value);
    }

    private void expect(DiscoveredThing discoveredThing) {
        this.expected.add(discoveredThing);
    }

    @Test
    public void testLiterals() throws Exception {
        givenQuery("bbc OR onyx");
        givenStartDate("20130101");
        givenEndDate("20130102");

        expect(new DiscoveredThing("bbc", "NETWORK", "csv", "20130101", "FOO", 240L, new MapWritable()));
        expect(new DiscoveredThing("bbc", "NETWORK", "csv", "20130102", "FOO", 240L, new MapWritable()));
        expect(new DiscoveredThing("onyx", "POKEMON", "csv", "20130101", "FOO", 100L, new MapWritable()));
        expect(new DiscoveredThing("onyx", "POKEMON", "csv", "20130102", "FOO", 10L, new MapWritable()));
        expect(new DiscoveredThing("onyx", "ROCK", "csv", "20130101", "FOO", 1L, new MapWritable()));
        expect(new DiscoveredThing("onyx", "ROCK", "csv", "20130102", "FOO", 3L, new MapWritable()));
        expect(new DiscoveredThing("onyx", "ROOSTER", "csv", "20130101", "BAR", 120L, new MapWritable()));
        expect(new DiscoveredThing("onyx", "ROOSTER", "csv", "20130102", "BAR", 120L, new MapWritable()));

        assertQueryResults();
    }

    @Test
    public void testPatterns() throws Exception {
        givenQuery("*yx OR b*");
        givenStartDate("20130101");
        givenEndDate("20130102");

        expect(new DiscoveredThing("bbc", "NETWORK", "csv", "20130101", "FOO", 240L, new MapWritable()));
        expect(new DiscoveredThing("bbc", "NETWORK", "csv", "20130102", "FOO", 240L, new MapWritable()));
        expect(new DiscoveredThing("onyx", "POKEMON", "csv", "20130101", "FOO", 100L, new MapWritable()));
        expect(new DiscoveredThing("onyx", "POKEMON", "csv", "20130102", "FOO", 10L, new MapWritable()));
        expect(new DiscoveredThing("onyx", "ROCK", "csv", "20130101", "FOO", 1L, new MapWritable()));
        expect(new DiscoveredThing("onyx", "ROCK", "csv", "20130102", "FOO", 3L, new MapWritable()));
        expect(new DiscoveredThing("onyx", "ROOSTER", "csv", "20130101", "BAR", 120L, new MapWritable()));
        expect(new DiscoveredThing("onyx", "ROOSTER", "csv", "20130102", "BAR", 120L, new MapWritable()));

        assertQueryResults();
    }

    @Test
    public void testPatternAndLiteral() throws Exception {
        givenQuery("*er OR trophy");
        givenStartDate("20130102");
        givenEndDate("20130104");

        expect(new DiscoveredThing("trophy", "PRIZE", "idem", "20130102", "FOO", 5L, new MapWritable()));
        expect(new DiscoveredThing("trophy", "PRIZE", "idem", "20130103", "FOO", 5L, new MapWritable()));
        expect(new DiscoveredThing("trophy", "PRIZE", "idem", "20130104", "FOO", 5L, new MapWritable()));
        expect(new DiscoveredThing("police officer", "JOB", "idem", "20130102", "FOO", 225L, new MapWritable()));
        expect(new DiscoveredThing("police officer", "JOB", "idem", "20130103", "FOO", 225L, new MapWritable()));
        expect(new DiscoveredThing("ranger", "VEHICLE", "stock", "20130102", "BAR&FOO", 325L, new MapWritable()));
        expect(new DiscoveredThing("ranger", "VEHICLE", "stock", "20130103", "BAR&FOO", 306L, new MapWritable()));
        expect(new DiscoveredThing("rooster", "FLOCK", "stock", "20130102", "BAR", 30L, new MapWritable()));
        expect(new DiscoveredThing("rooster", "FLOCK", "stock", "20130103", "BAR", 30L, new MapWritable()));
        expect(new DiscoveredThing("skydiver", "JOB", "text", "20130102", "BAR", 100L, new MapWritable()));
        expect(new DiscoveredThing("skydiver", "JOB", "text", "20130103", "BAR", 100L, new MapWritable()));
        expect(new DiscoveredThing("skydiver", "JOB", "text", "20130104", "BAR", 100L, new MapWritable()));
        expect(new DiscoveredThing("skydiver", "OCCUPATION", "text", "20130102", "FOO", 100L, new MapWritable()));
        expect(new DiscoveredThing("skydiver", "OCCUPATION", "text", "20130103", "FOO", 100L, new MapWritable()));
        expect(new DiscoveredThing("skydiver", "OCCUPATION", "text", "20130104", "FOO", 100L, new MapWritable()));
        expect(new DiscoveredThing("xxx.skydiver", "OCCUPATION", "text", "20130102", "FOO", 100L, new MapWritable()));
        expect(new DiscoveredThing("xxx.skydiver", "OCCUPATION", "text", "20130103", "FOO", 100L, new MapWritable()));
        expect(new DiscoveredThing("xxx.skydiver", "OCCUPATION", "text", "20130104", "FOO", 100L, new MapWritable()));
        expect(new DiscoveredThing("yyy.skydiver", "OCCUPATION", "text", "20130102", "FOO", 100L, new MapWritable()));
        expect(new DiscoveredThing("yyy.skydiver", "OCCUPATION", "text", "20130103", "FOO", 100L, new MapWritable()));
        expect(new DiscoveredThing("yyy.skydiver", "OCCUPATION", "text", "20130104", "FOO", 100L, new MapWritable()));

        assertQueryResults();
    }

    @Test
    public void testFieldedLiterals() throws Exception {
        givenQuery("rock:onyx OR pokemon:onyx");
        givenStartDate("20130101");
        givenEndDate("20130104");

        expect(new DiscoveredThing("onyx", "POKEMON", "csv", "20130101", "FOO", 100L, new MapWritable()));
        expect(new DiscoveredThing("onyx", "POKEMON", "csv", "20130102", "FOO", 10L, new MapWritable()));
        expect(new DiscoveredThing("onyx", "POKEMON", "csv", "20130103", "FOO", 1L, new MapWritable()));
        expect(new DiscoveredThing("onyx", "ROCK", "csv", "20130101", "FOO", 1L, new MapWritable()));
        expect(new DiscoveredThing("onyx", "ROCK", "csv", "20130102", "FOO", 3L, new MapWritable()));
        expect(new DiscoveredThing("onyx", "ROCK", "csv", "20130103", "FOO", 3L, new MapWritable()));

        assertQueryResults();
    }

    @Test
    public void testFieldedPatterns() throws Exception {
        givenQuery("rock:*n*x OR bird:*r*k");
        givenStartDate("20130101");
        givenEndDate("20130103");

        expect(new DiscoveredThing("ruddy duck", "BIRD", "stock", "20130101", "FOO", 300L, new MapWritable()));
        expect(new DiscoveredThing("ruddy duck", "BIRD", "stock", "20130102", "FOO", 300L, new MapWritable()));
        expect(new DiscoveredThing("ruddy duck", "BIRD", "stock", "20130103", "FOO", 300L, new MapWritable()));
        expect(new DiscoveredThing("onyx", "ROCK", "csv", "20130101", "FOO", 1L, new MapWritable()));
        expect(new DiscoveredThing("onyx", "ROCK", "csv", "20130102", "FOO", 3L, new MapWritable()));
        expect(new DiscoveredThing("onyx", "ROCK", "csv", "20130103", "FOO", 3L, new MapWritable()));

        assertQueryResults();
    }

    @Test
    public void testFieldLiteralAndPattern() throws Exception {
        givenQuery("pokemon:onyx OR bird:*r*k");
        givenStartDate("20130101");
        givenEndDate("20130104");

        expect(new DiscoveredThing("onyx", "POKEMON", "csv", "20130101", "FOO", 100L, new MapWritable()));
        expect(new DiscoveredThing("onyx", "POKEMON", "csv", "20130102", "FOO", 10L, new MapWritable()));
        expect(new DiscoveredThing("onyx", "POKEMON", "csv", "20130103", "FOO", 1L, new MapWritable()));
        expect(new DiscoveredThing("ruddy duck", "BIRD", "stock", "20130101", "FOO", 300L, new MapWritable()));
        expect(new DiscoveredThing("ruddy duck", "BIRD", "stock", "20130102", "FOO", 300L, new MapWritable()));
        expect(new DiscoveredThing("ruddy duck", "BIRD", "stock", "20130103", "FOO", 300L, new MapWritable()));

        assertQueryResults();
    }

    @Test
    public void testIgnoreNonIndexedField() throws Exception {
        givenQuery("coffee OR espresso OR rooster");
        givenStartDate("20130101");
        givenEndDate("20130104");

        expect(new DiscoveredThing("rooster", "FLOCK", "stock", "20130101", "BAR", 30L, new MapWritable()));
        expect(new DiscoveredThing("rooster", "FLOCK", "stock", "20130102", "BAR", 30L, new MapWritable()));
        expect(new DiscoveredThing("rooster", "FLOCK", "stock", "20130103", "BAR", 30L, new MapWritable()));

        assertQueryResults();
    }

    @Test
    public void testReverse() throws Exception {
        givenQuery("*.sky*er");
        givenStartDate("20130101");
        givenEndDate("20130104");

        expect(new DiscoveredThing("xxx.skydiver", "OCCUPATION", "text", "20130101", "FOO", 100L, new MapWritable()));
        expect(new DiscoveredThing("xxx.skydiver", "OCCUPATION", "text", "20130102", "FOO", 100L, new MapWritable()));
        expect(new DiscoveredThing("xxx.skydiver", "OCCUPATION", "text", "20130103", "FOO", 100L, new MapWritable()));
        expect(new DiscoveredThing("xxx.skydiver", "OCCUPATION", "text", "20130104", "FOO", 100L, new MapWritable()));
        expect(new DiscoveredThing("yyy.skydiver", "OCCUPATION", "text", "20130101", "FOO", 100L, new MapWritable()));
        expect(new DiscoveredThing("yyy.skydiver", "OCCUPATION", "text", "20130102", "FOO", 100L, new MapWritable()));
        expect(new DiscoveredThing("yyy.skydiver", "OCCUPATION", "text", "20130103", "FOO", 100L, new MapWritable()));
        expect(new DiscoveredThing("yyy.skydiver", "OCCUPATION", "text", "20130104", "FOO", 100L, new MapWritable()));

        assertQueryResults();
    }

    @Test
    public void testSumCountsForLiterals() throws Exception {
        givenQuery("bbc OR onyx");
        givenStartDate("20130101");
        givenEndDate("20130102");
        givenParameter(DiscoveryLogic.SUM_COUNTS, "true");

        expect(new DiscoveredThing("bbc", "NETWORK", "csv", "", "FOO", 480L, new MapWritable()));
        expect(new DiscoveredThing("onyx", "POKEMON", "csv", "", "FOO", 110L, new MapWritable()));
        expect(new DiscoveredThing("onyx", "ROCK", "csv", "", "FOO", 4L, new MapWritable()));
        expect(new DiscoveredThing("onyx", "ROOSTER", "csv", "", "BAR", 240L, new MapWritable()));

        assertQueryResults();
    }

    @Test
    public void testSumCountsForPatterns() throws Exception {
        givenQuery("*yx OR b*");
        givenStartDate("20130101");
        givenEndDate("20130102");
        givenParameter(DiscoveryLogic.SUM_COUNTS, "true");

        expect(new DiscoveredThing("bbc", "NETWORK", "csv", "", "FOO", 480L, new MapWritable()));
        expect(new DiscoveredThing("onyx", "POKEMON", "csv", "", "FOO", 110L, new MapWritable()));
        expect(new DiscoveredThing("onyx", "ROCK", "csv", "", "FOO", 4L, new MapWritable()));
        expect(new DiscoveredThing("onyx", "ROOSTER", "csv", "", "BAR", 240L, new MapWritable()));

        assertQueryResults();
    }

    @Test
    public void testSumCountsForPatternAndLiteral() throws Exception {
        givenQuery("*er OR trophy");
        givenStartDate("20130102");
        givenEndDate("20130104");
        givenParameter(DiscoveryLogic.SUM_COUNTS, "true");

        expect(new DiscoveredThing("trophy", "PRIZE", "idem", "", "FOO", 15L, new MapWritable()));
        expect(new DiscoveredThing("police officer", "JOB", "idem", "", "FOO", 450L, new MapWritable()));
        expect(new DiscoveredThing("ranger", "VEHICLE", "stock", "", "BAR&FOO", 631L, new MapWritable()));
        expect(new DiscoveredThing("rooster", "FLOCK", "stock", "", "BAR", 60L, new MapWritable()));
        expect(new DiscoveredThing("skydiver", "JOB", "text", "", "BAR", 300L, new MapWritable()));
        expect(new DiscoveredThing("skydiver", "OCCUPATION", "text", "", "FOO", 300L, new MapWritable()));
        expect(new DiscoveredThing("xxx.skydiver", "OCCUPATION", "text", "", "FOO", 300L, new MapWritable()));
        expect(new DiscoveredThing("yyy.skydiver", "OCCUPATION", "text", "", "FOO", 300L, new MapWritable()));

        assertQueryResults();
    }

    @Test
    public void testSumCountsForFieldedLiterals() throws Exception {
        givenQuery("rock:onyx OR pokemon:onyx");
        givenStartDate("20130101");
        givenEndDate("20130104");
        givenParameter(DiscoveryLogic.SUM_COUNTS, "true");

        expect(new DiscoveredThing("onyx", "POKEMON", "csv", "", "FOO", 111L, new MapWritable()));
        expect(new DiscoveredThing("onyx", "ROCK", "csv", "", "FOO", 7L, new MapWritable()));

        assertQueryResults();
    }

    @Test
    public void testSumCountsForFieldedPatterns() throws Exception {
        givenQuery("rock:*n*x OR bird:*r*k");
        givenStartDate("20130101");
        givenEndDate("20130103");
        givenParameter(DiscoveryLogic.SUM_COUNTS, "true");

        expect(new DiscoveredThing("ruddy duck", "BIRD", "stock", "", "FOO", 900L, new MapWritable()));
        expect(new DiscoveredThing("onyx", "ROCK", "csv", "", "FOO", 7L, new MapWritable()));

        assertQueryResults();
    }

    @Test
    public void testSumCountsForFieldLiteralAndPattern() throws Exception {
        givenQuery("pokemon:onyx OR bird:*r*k");
        givenStartDate("20130101");
        givenEndDate("20130104");
        givenParameter(DiscoveryLogic.SUM_COUNTS, "true");

        expect(new DiscoveredThing("onyx", "POKEMON", "csv", "", "FOO", 111L, new MapWritable()));
        expect(new DiscoveredThing("ruddy duck", "BIRD", "stock", "", "FOO", 900L, new MapWritable()));

        assertQueryResults();
    }

    @Test
    public void testSumCountsForReverse() throws Exception {
        givenQuery("*.sky*er");
        givenStartDate("20130101");
        givenEndDate("20130104");
        givenParameter(DiscoveryLogic.SUM_COUNTS, "true");

        expect(new DiscoveredThing("xxx.skydiver", "OCCUPATION", "text", "", "FOO", 400L, new MapWritable()));
        expect(new DiscoveredThing("yyy.skydiver", "OCCUPATION", "text", "", "FOO", 400L, new MapWritable()));

        assertQueryResults();
    }
}
