package datawave.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.configuration.spring.SpringBean;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.microservice.query.QueryImpl;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.attributes.TypeAttribute;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.TreeEqualityVisitor;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.util.ColorsIngest;
import datawave.test.HitTermAssertions;
import datawave.util.TableName;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;

/**
 * A set of tests that exercises multi-shard, multi-day queries
 * <p>
 * Hit Term assertions are supported. Most queries should assert total documents returned and shards seen in the results.
 */
public abstract class ColorsTest {

    private static final Logger log = LoggerFactory.getLogger(ColorsTest.class);
    protected Authorizations auths = new Authorizations("ALL");
    protected Set<Authorizations> authSet = Collections.singleton(auths);

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Inject
    @SpringBean(name = "EventQuery")
    protected ShardQueryLogic logic;

    protected KryoDocumentDeserializer deserializer = new KryoDocumentDeserializer();
    private final DateFormat format = new SimpleDateFormat("yyyyMMdd");
    private AccumuloClient clientForTest;

    public void setClientForTest(AccumuloClient client) {
        this.clientForTest = client;
    }

    // used for declarative style tests
    private String query;
    private String startDate;
    private String endDate;

    private Map<String,String> parameters = new HashMap<>();
    private Set<String> expected = new HashSet<>();
    private Set<Document> results = new HashSet<>();

    private int expectedCount = 0;
    private final Set<String> expectedDays = new HashSet<>();
    private final Set<String> expectedShards = new HashSet<>();

    private final HitTermAssertions assertHitTerms = new HitTermAssertions();

    @RunWith(Arquillian.class)
    public static class ShardRange extends ColorsTest {
        protected static AccumuloClient client = null;

        @BeforeClass
        public static void setUp() throws Exception {
            InMemoryInstance i = new InMemoryInstance(ShardRange.class.getName());
            client = new InMemoryAccumuloClient("", i);

            ColorsIngest.writeData(client, ColorsIngest.RangeType.SHARD);

            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(client, auths, TableName.SHARD);
            PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        }

        @Before
        public void beforeEach() {
            setClientForTest(client);
        }
    }

    @RunWith(Arquillian.class)
    public static class DocumentRange extends ColorsTest {
        protected static AccumuloClient client = null;

        @BeforeClass
        public static void setUp() throws Exception {
            InMemoryInstance i = new InMemoryInstance(DocumentRange.class.getName());
            client = new InMemoryAccumuloClient("", i);

            ColorsIngest.writeData(client, ColorsIngest.RangeType.DOCUMENT);

            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(client, auths, TableName.SHARD);
            PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        }

        @Before
        public void beforeEach() {
            setClientForTest(client);
        }
    }

    @Deployment
    public static JavaArchive createDeployment() throws Exception {
        //  @formatter:off
        return ShrinkWrap.create(JavaArchive.class)
                .addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi", "datawave.query", "org.jboss.logging",
                        "datawave.webservice.query.result.event")
                .deleteClass(DefaultEdgeEventQueryLogic.class)
                .deleteClass(RemoteEdgeDictionary.class)
                .deleteClass(datawave.query.metrics.QueryMetricQueryLogic.class)
                .addAsManifestResource(new StringAsset(
                                "<alternatives>" + "<stereotype>datawave.query.tables.edge.MockAlternative</stereotype>" + "</alternatives>"),
                        "beans.xml");
        //  @formatter:on
    }

    @Before
    public void setup() throws IOException {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        resetState();

        URL hadoopConfig = this.getClass().getResource("/testhadoop.config");
        Preconditions.checkNotNull(hadoopConfig);
        logic.setHdfsSiteConfigURLs(hadoopConfig.toExternalForm());

        IvaratorCacheDirConfig config = new IvaratorCacheDirConfig(temporaryFolder.newFolder().toURI().toString());
        logic.setIvaratorCacheDirConfigs(Collections.singletonList(config));

        logic.setMaxFieldIndexRangeSplit(1); // keep things simple

        // disable by default to make clear what tests actually require these settings
        logic.setSortQueryPostIndexWithTermCounts(false);
        logic.setCardinalityThreshold(0);

        // every test also exercises hit terms
        withParameter(QueryParameters.HIT_LIST, "true");
        logic.setHitList(true);
    }

    @After
    public void after() {
        resetState();
    }

    private void resetState() {
        query = null;
        if (logic != null) {
            logic.setReduceIngestTypes(false);
            logic.setRebuildDatatypeFilter(false);
            logic.setPruneQueryByIngestTypes(false);
        }
        parameters.clear();
        expected.clear();
        results.clear();

        expectedCount = 0;

        assertHitTerms.resetState();

        // default to full date range
        startDate = ColorsIngest.getStartDay();
        endDate = ColorsIngest.getEndDay();

        expectedDays.clear();
        expectedShards.clear();
    }

    @AfterClass
    public static void teardown() {
        TypeRegistry.reset();
    }

    protected void runTestQuery(String query, Map<String,String> extraParameters, Set<String> expected) throws Exception {
        withQuery(query);
        withParameters(extraParameters);
        withExpected(expected);
        planQuery();
        executeQuery();
        // assertUuids();

    }

    public ColorsTest withQuery(String query) {
        this.query = query;
        return this;
    }

    public ColorsTest withDateRange(String start, String end) {
        this.startDate = start;
        this.endDate = end;
        return this;
    }

    public ColorsTest withParameter(String key, String value) {
        parameters.put(key, value);
        return this;
    }

    public ColorsTest withParameters(Map<String,String> parameters) {
        this.parameters = parameters;
        return this;
    }

    public ColorsTest withExpected(Set<String> expected) {
        this.expected = expected;
        return this;
    }

    public ColorsTest withExpectedCount(int expectedCount) {
        this.expectedCount = expectedCount;
        return this;
    }

    public ColorsTest withFullExpectedCount() {
        this.expectedCount = getTotalEventCount();
        return this;
    }

    public int getTotalEventCount() {
        // there are 26 days of data for 'num shards' and 5 days of data using the 'new shards' count
        int count = (26 * ColorsIngest.getNumShards());
        count += (5 * ColorsIngest.getNewShards());
        return count;
    }

    public ColorsTest withExpectedDays(String... days) {
        this.expectedDays.addAll(List.of(days));
        return this;
    }

    public ColorsTest withExpectedShards(String day, int numShards) {
        for (int i = 0; i < numShards; i++) {
            this.expectedShards.add(day + "_" + i);
        }
        return this;
    }

    /**
     * Required hit terms must exist in every result, for example an anchor term
     *
     * @param hitTerms
     *            one or more hit terms
     * @return the test instance
     */
    public ColorsTest withRequiredAllOf(String... hitTerms) {
        assertHitTerms.withRequiredAllOf(hitTerms);
        return this;
    }

    /**
     * Required hit terms must exist in every result, for example an anchor term
     *
     * @param hitTerms
     *            one or more hit terms
     * @return the test instance
     */
    public ColorsTest withRequiredAnyOf(String... hitTerms) {
        assertHitTerms.withRequiredAnyOf(hitTerms);
        return this;
    }

    /**
     * At least one optional hit term must exist in every result, for example terms in a union
     *
     * @param hitTerms
     *            one or more hit terms
     * @return the test instance
     */
    public ColorsTest withOptionalAllOf(String... hitTerms) {
        assertHitTerms.withOptionalAllOf(hitTerms);
        return this;
    }

    public ColorsTest withOptionalAnyOf(String... hitTerms) {
        assertHitTerms.withOptionalAnyOf(hitTerms);
        return this;
    }

    public ColorsTest planAndExecuteQuery() throws Exception {
        planQuery();
        executeQuery();
        // assertUuids();
        assertExpectedCount();
        assertExpectedShardsAndDays();
        assertHitTerms();
        return this;
    }

    public void planQuery() throws Exception {
        try {
            QueryImpl settings = new QueryImpl();
            settings.setBeginDate(getStartDate());
            settings.setEndDate(getEndDate());
            settings.setPagesize(Integer.MAX_VALUE);
            settings.setQueryAuthorizations(auths.serialize());
            settings.setQuery(query);
            settings.setParameters(parameters);
            settings.setId(UUID.randomUUID());

            logic.setMaxEvaluationPipelines(1);

            GenericQueryConfiguration config = logic.initialize(clientForTest, settings, authSet);
            logic.setupQuery(config);
        } catch (Exception e) {
            log.info("exception while planning query", e);
            throw e;
        }
    }

    protected Date getStartDate() throws Exception {
        Assert.assertNotNull(startDate);
        return format.parse(startDate);
    }

    protected Date getEndDate() throws Exception {
        Assert.assertNotNull(endDate);
        return format.parse(endDate);
    }

    public ColorsTest executeQuery() {
        results = new HashSet<>();
        for (Map.Entry<Key,Value> entry : logic) {
            Document d = deserializer.apply(entry).getValue();
            results.add(d);
        }
        logic.close();
        return this;
    }

    public ColorsTest assertUuids() {
        assertNotNull(expected);
        assertNotNull(results);

        Set<String> found = new HashSet<>();
        for (Document result : results) {
            Attribute<?> attr = result.get("UUID");
            assertNotNull("result did not contain a UUID", attr);
            String uuid = getUUID(attr);
            found.add(uuid);
        }

        Set<String> missing = Sets.difference(expected, found);
        if (!missing.isEmpty()) {
            log.info("missing uuids: {}", missing);
        }

        Set<String> extra = Sets.difference(found, expected);
        if (!extra.isEmpty()) {
            log.info("extra uuids: {}", extra);
        }

        assertEquals(expected, found);
        return this;
    }

    public String getUUID(Attribute<?> attribute) {
        boolean typed = attribute instanceof TypeAttribute;
        assertTrue("Attribute was not a TypeAttribute, was: " + attribute.getClass(), typed);
        TypeAttribute<?> uuid = (TypeAttribute<?>) attribute;
        return uuid.getType().getDelegateAsString();
    }

    public void assertExpectedCount() {
        assertEquals(expectedCount, results.size());
    }

    /**
     * Extract the row from each event, recording both the day and shard.
     *
     * @return the test suite
     */
    public ColorsTest assertExpectedShardsAndDays() {
        if (expectedShards.isEmpty() && expectedDays.isEmpty()) {
            return this;
        }

        if (results.isEmpty()) {
            fail("expected days or shards but had no results");
        }

        Set<String> days = new HashSet<>();
        Set<String> shards = new HashSet<>();

        for (Document result : results) {
            Attribute<?> attr = result.get("RECORD_ID");
            Key meta = attr.getMetadata();
            String row = meta.getRow().toString();
            String[] parts = row.split("_");
            days.add(parts[0]);
            shards.add(row);
        }

        if (!expectedDays.isEmpty()) {
            assertEquals(expectedDays, days);
        }

        if (!expectedShards.isEmpty()) {
            assertEquals(expectedShards, shards);

        }
        return this;
    }

    public ColorsTest assertHitTerms() {
        // first, assert that if hit terms were expected that we got results. It is an error condition to expect hits and not get any results
        assertEquals(assertHitTerms.hitTermExpected(), !results.isEmpty());
        if (!results.isEmpty()) {
            boolean validated = assertHitTerms.assertHitTerms(results);
            assertEquals(assertHitTerms.hitTermExpected(), validated);
        }
        return this;
    }

    public void assertPlannedQuery(String query) {
        try {
            ASTJexlScript expected = JexlASTHelper.parseAndFlattenJexlQuery(query);
            ASTJexlScript plannedScript = logic.getConfig().getQueryTree();
            if (!TreeEqualityVisitor.isEqual(expected, plannedScript)) {
                log.info("expected: {}", query);
                log.info("planned : {}", logic.getConfig().getQueryString());
                fail("Planned query did not match expectation");
            }
        } catch (ParseException e) {
            fail("Failed to parse query: " + query);
        }
    }

    @SafeVarargs
    public final Set<String> createSet(Set<String>... sets) {
        Set<String> s = new HashSet<>();
        for (Set<String> set : sets) {
            s.addAll(set);
        }
        return s;
    }

    @Test
    public void testColorRed() throws Exception {
        withQuery("COLOR == 'red'");
        withRequiredAllOf("COLOR:red");
        withFullExpectedCount();
        planAndExecuteQuery();
    }

    @Test
    public void testColorYellow() throws Exception {
        withQuery("COLOR == 'yellow'");
        withRequiredAllOf("COLOR:yellow");
        withFullExpectedCount();
        planAndExecuteQuery();
    }

    @Test
    public void testColorBlue() throws Exception {
        withQuery("COLOR == 'blue'");
        withRequiredAllOf("COLOR:blue");
        withFullExpectedCount();
        planAndExecuteQuery();
    }

    @Test
    public void testAllColors() throws Exception {
        withQuery("COLOR == 'red' || COLOR == 'yellow' || COLOR == 'blue'");
        withOptionalAnyOf("COLOR:red", "COLOR:yellow", "COLOR:blue");
        withExpectedCount(3 * getTotalEventCount());
        planAndExecuteQuery();
    }

    @Test
    public void testSearchAllShardsDefeatedAtFieldIndex() throws Exception {
        withQuery("COLOR == 'red' && !COLOR == 'red'");
        planAndExecuteQuery();
    }

    @Test
    public void testSearchAllShardsDefeatedAtEvaluation() throws Exception {
        withQuery("COLOR == 'red' && filter:includeRegex(COLOR, 'yellow')");
        planAndExecuteQuery();
    }

    @Test
    public void testReturnedShardsForEarlierDate() throws Exception {
        withQuery("COLOR == 'red'");
        withRequiredAllOf("COLOR:red");
        withDateRange("20250301", "20250301");
        withExpectedCount(1);
        withExpectedDays("20250301");
        withExpectedShards("20250301", ColorsIngest.getNumShards());
        planAndExecuteQuery();
    }

    @Test
    public void testReturnedShardsForLaterDate() throws Exception {
        withQuery("COLOR == 'red'");
        withRequiredAllOf("COLOR:red");
        withDateRange("20250331", "20250331");
        withExpectedCount(2);
        withExpectedDays("20250331");
        withExpectedShards("20250331", ColorsIngest.getNewShards());
        planAndExecuteQuery();
    }

    @Test
    public void testReturnedShardsForQueryThatCrossesBoundary() throws Exception {
        withQuery("COLOR == 'red'");
        withRequiredAllOf("COLOR:red");
        withDateRange("20250326", "20250327");
        withExpectedCount(3);
        withExpectedDays("20250326", "20250327");
        withExpectedShards("20250326", ColorsIngest.getNumShards());
        withExpectedShards("20250327", ColorsIngest.getNewShards());
        planAndExecuteQuery();
    }

    // TODO: unique

    // TODO: grouping
}
