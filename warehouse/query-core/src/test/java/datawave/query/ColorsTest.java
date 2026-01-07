package datawave.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

import javax.inject.Inject;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.configuration.spring.SpringBean;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.index.day.IndexIngestUtil;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.TreeEqualityVisitor;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.util.AbstractQueryTest;
import datawave.query.util.ColorsIngest;
import datawave.query.util.TestIndexTableNames;
import datawave.util.TableName;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;

/**
 * A set of tests that exercises multi-shard, multi-day queries
 * <p>
 * Hit Term assertions are supported. Most queries should assert total documents returned and shards seen in the results.
 */
@RunWith(Arquillian.class)
public class ColorsTest extends AbstractQueryTest {

    private static final Logger log = LoggerFactory.getLogger(ColorsTest.class);

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Inject
    @SpringBean(name = "EventQuery")
    protected ShardQueryLogic logic;

    // this two client setup is a little odd, but it allows us to create tables once
    protected static AccumuloClient client = null;

    private final Set<String> expectedDays = new HashSet<>();
    private final Set<String> expectedShards = new HashSet<>();

    private static final IndexIngestUtil ingestUtil = new IndexIngestUtil();

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

    @BeforeClass
    public static void setUp() throws Exception {
        InMemoryInstance i = new InMemoryInstance(ColorsTest.class.getName());
        client = new InMemoryAccumuloClient("", i);

        ColorsIngest.writeData(client);

        Authorizations auths = new Authorizations("ALL");

        ingestUtil.write(client, auths);

        PrintUtility.printTable(client, auths, TableName.SHARD);
        PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
    }

    @Before
    public void setup() throws IOException {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        resetState();
        setClientForTest(client);

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

        // default to full date range
        withDate(ColorsIngest.getStartDay(), ColorsIngest.getEndDay());
    }

    @Override
    public ShardQueryLogic getLogic() {
        return logic;
    }

    @After
    public void after() {
        super.after();
        resetState();
    }

    private void resetState() {
        if (logic != null) {
            logic.setReduceIngestTypes(false);
            logic.setRebuildDatatypeFilter(false);
            logic.setPruneQueryByIngestTypes(false);
        }
        expectedDays.clear();
        expectedShards.clear();
    }

    @AfterClass
    public static void teardown() {
        TypeRegistry.reset();
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
     * Supports running the same query against multiple types of index tables
     *
     * @return this class
     * @throws Exception
     *             if something goes wrong
     */
    public ColorsTest planAndExecuteQueryAgainstMultipleIndices() throws Exception {
        for (String indexTableName : TestIndexTableNames.names()) {
            logic.setIndexTableName(indexTableName);
            switch (indexTableName) {
                case TestIndexTableNames.SHARD_INDEX:
                case TestIndexTableNames.NO_UID_INDEX:
                    break;
                case TestIndexTableNames.TRUNCATED_INDEX:
                    logic.setUseTruncatedIndex(true);
                    break;
                default:
                    throw new IllegalStateException("Unknown index table name " + indexTableName);
            }

            log.debug("=== using index: {} ===", indexTableName);
            planAndExecuteQuery();

            switch (indexTableName) {
                case TestIndexTableNames.SHARD_INDEX:
                case TestIndexTableNames.NO_UID_INDEX:
                    break;
                case TestIndexTableNames.TRUNCATED_INDEX:
                    logic.setUseTruncatedIndex(false);
                    break;
                default:
                    throw new IllegalStateException("Unknown index table name " + indexTableName);
            }
        }
        return this;
    }

    /**
     * Delegate to super method to plan and execute the query and assert the hit terms
     *
     * @throws Exception
     *             if something goes wrong
     */
    @Override
    public void planAndExecuteQuery() throws Exception {
        super.planAndExecuteQuery();
        assertResultCount();
        assertExpectedShardsAndDays();
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

    @Test
    public void testColorRed() throws Exception {
        withQuery("COLOR == 'red'");
        withRequiredAllOf("COLOR:red");
        withResultCount(getTotalEventCount());
        planAndExecuteQueryAgainstMultipleIndices();
    }

    @Test
    public void testColorYellow() throws Exception {
        withQuery("COLOR == 'yellow'");
        withRequiredAllOf("COLOR:yellow");
        withResultCount(getTotalEventCount());
        planAndExecuteQueryAgainstMultipleIndices();
    }

    @Test
    public void testColorBlue() throws Exception {
        withQuery("COLOR == 'blue'");
        withRequiredAllOf("COLOR:blue");
        withResultCount(getTotalEventCount());
        planAndExecuteQueryAgainstMultipleIndices();
    }

    @Test
    public void testAllColors() throws Exception {
        withQuery("COLOR == 'red' || COLOR == 'yellow' || COLOR == 'blue'");
        withOptionalAnyOf("COLOR:red", "COLOR:yellow", "COLOR:blue");
        withResultCount(3 * getTotalEventCount());
        planAndExecuteQueryAgainstMultipleIndices();
    }

    @Test
    public void testSearchAllShardsDefeatedAtFieldIndex() throws Exception {
        withQuery("COLOR == 'red' && !(COLOR == 'red')");
        withResultCount(0);
        planAndExecuteQueryAgainstMultipleIndices();
    }

    @Test
    public void testSearchAllShardsDefeatedAtEvaluation() throws Exception {
        withQuery("COLOR == 'red' && filter:includeRegex(COLOR, 'yellow')");
        withResultCount(0);
        planAndExecuteQueryAgainstMultipleIndices();
    }

    @Test
    public void testReturnedShardsForEarlierDate() throws Exception {
        withQuery("COLOR == 'red'");
        withRequiredAllOf("COLOR:red");
        withDate("20250301", "20250301");
        withResultCount(ColorsIngest.getNumShards());
        withExpectedDays("20250301");
        withExpectedShards("20250301", ColorsIngest.getNumShards());
        planAndExecuteQueryAgainstMultipleIndices();
    }

    @Test
    public void testReturnedShardsForLaterDate() throws Exception {
        withQuery("COLOR == 'red'");
        withRequiredAllOf("COLOR:red");
        withDate("20250331", "20250331");
        withResultCount(ColorsIngest.getNewShards());
        withExpectedDays("20250331");
        withExpectedShards("20250331", ColorsIngest.getNewShards());
        planAndExecuteQueryAgainstMultipleIndices();
    }

    @Test
    public void testReturnedShardsForQueryThatCrossesBoundary() throws Exception {
        withQuery("COLOR == 'red'");
        withRequiredAllOf("COLOR:red");
        withDate("20250326", "20250327");
        withResultCount(ColorsIngest.getNumShards() + ColorsIngest.getNewShards());
        withExpectedDays("20250326", "20250327");
        withExpectedShards("20250326", ColorsIngest.getNumShards());
        withExpectedShards("20250327", ColorsIngest.getNewShards());
        planAndExecuteQueryAgainstMultipleIndices();
    }

    // TODO: unique

    // TODO: grouping
}
