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
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.util.AbstractQueryTest;
import datawave.query.util.ColorsIngest;
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
        givenParameter(QueryParameters.HIT_LIST, "true");
        logic.setHitList(true);

        // default to full date range
        givenDate(ColorsIngest.getStartDay(), ColorsIngest.getEndDay());
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

    public ColorsTest expectDays(String... days) {
        this.expectedDays.addAll(List.of(days));
        return this;
    }

    public ColorsTest expectShards(String day, int numShards) {
        for (int i = 0; i < numShards; i++) {
            this.expectedShards.add(day + "_" + i);
        }
        return this;
    }

    /**
     * Extract the row from each event, recording both the day and shard.
     */
    public void assertExpectedShardsAndDays() {
        if (expectedShards.isEmpty() && expectedDays.isEmpty()) {
            return;
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
    }

    @Override
    protected void extraConfigurations() {
        // no-op
    }

    @Override
    protected void extraAssertions() {
        assertExpectedShardsAndDays();
    }

    @Test
    public void testColorRed() throws Exception {
        givenQuery("COLOR == 'red'");
        expectPlan("COLOR == 'red'");
        expectResultCount(getTotalEventCount());
        expectHitTermsRequiredAllOf("COLOR:red");
        planAndExecuteQuery();
    }

    @Test
    public void testColorYellow() throws Exception {
        givenQuery("COLOR == 'yellow'");
        expectPlan("COLOR == 'yellow'");
        expectResultCount(getTotalEventCount());
        expectHitTermsRequiredAllOf("COLOR:yellow");
        planAndExecuteQuery();
    }

    @Test
    public void testColorBlue() throws Exception {
        givenQuery("COLOR == 'blue'");
        expectPlan("COLOR == 'blue'");
        expectResultCount(getTotalEventCount());
        expectHitTermsRequiredAllOf("COLOR:blue");
        planAndExecuteQuery();
    }

    @Test
    public void testAllColors() throws Exception {
        givenQuery("COLOR == 'red' || COLOR == 'yellow' || COLOR == 'blue'");
        expectPlan("COLOR == 'red' || COLOR == 'yellow' || COLOR == 'blue'");
        expectResultCount(3 * getTotalEventCount());
        expectHitTermsOptionalAnyOf("COLOR:red", "COLOR:yellow", "COLOR:blue");
        planAndExecuteQuery();
    }

    @Test
    public void testSearchAllShardsDefeatedAtFieldIndex() throws Exception {
        givenQuery("COLOR == 'red' && !(COLOR == 'red')");
        expectPlan("COLOR == 'red' && !(COLOR == 'red')");
        expectResultCount(0);
        planAndExecuteQuery();
    }

    @Test
    public void testSearchAllShardsDefeatedAtEvaluation() throws Exception {
        givenQuery("COLOR == 'red' && filter:includeRegex(COLOR, 'yellow')");
        expectPlan("COLOR == 'red' && filter:includeRegex(COLOR, 'yellow')");
        expectResultCount(0);
        planAndExecuteQuery();
    }

    @Test
    public void testReturnedShardsForEarlierDate() throws Exception {
        givenDate("20250301", "20250301");
        givenQuery("COLOR == 'red'");
        expectPlan("COLOR == 'red'");
        expectResultCount(ColorsIngest.getNumShards());
        expectHitTermsRequiredAllOf("COLOR:red");
        expectDays("20250301");
        expectShards("20250301", ColorsIngest.getNumShards());
        planAndExecuteQuery();
    }

    @Test
    public void testReturnedShardsForLaterDate() throws Exception {
        givenDate("20250331", "20250331");
        givenQuery("COLOR == 'red'");
        expectPlan("COLOR == 'red'");
        expectResultCount(ColorsIngest.getNewShards());
        expectHitTermsRequiredAllOf("COLOR:red");
        expectDays("20250331");
        expectShards("20250331", ColorsIngest.getNewShards());
        planAndExecuteQuery();
    }

    @Test
    public void testReturnedShardsForQueryThatCrossesBoundary() throws Exception {
        givenDate("20250326", "20250327");
        givenQuery("COLOR == 'red'");
        expectPlan("COLOR == 'red'");
        expectResultCount(ColorsIngest.getNumShards() + ColorsIngest.getNewShards());
        expectHitTermsRequiredAllOf("COLOR:red");
        expectDays("20250326", "20250327");
        expectShards("20250326", ColorsIngest.getNumShards());
        expectShards("20250327", ColorsIngest.getNewShards());
        planAndExecuteQuery();
    }

    // TODO: unique

    // TODO: grouping
}
