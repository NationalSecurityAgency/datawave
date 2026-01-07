package datawave.query;

import javax.inject.Inject;

import org.apache.accumulo.core.client.AccumuloClient;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.configuration.spring.SpringBean;
import datawave.ingest.data.TypeRegistry;
import datawave.query.index.day.IndexIngestUtil;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.util.AbstractQueryTest;
import datawave.query.util.SizesIngest;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;

/**
 * This suite of tests exercises many random events over a small number of shards
 */
@RunWith(Arquillian.class)
public class SizesTest extends AbstractQueryTest {

    private static final Logger log = LoggerFactory.getLogger(SizesTest.class);

    // static utilities for test
    private static final InMemoryInstance instance = new InMemoryInstance(SizesTest.class.getName());
    private static AccumuloClient clientForSetup;
    private static SizesIngest ingest;

    // utility for writing different index table structures
    private static final IndexIngestUtil ingestUtil = new IndexIngestUtil();

    @Inject
    @SpringBean(name = "EventQuery")
    protected ShardQueryLogic logic;

    @Override
    public ShardQueryLogic getLogic() {
        return logic;
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

    @BeforeClass
    public static void beforeClass() throws Exception {
        clientForSetup = new InMemoryAccumuloClient("", instance);

        ingest = new SizesIngest(clientForSetup);
        ingest.write();

        ingestUtil.write(clientForSetup, auths);
    }

    @Before
    public void setup() throws Exception {
        givenDate("20250606", "20250606");
        setClientForTest(clientForSetup);
    }

    @AfterClass
    public static void teardown() {
        TypeRegistry.reset();
    }

    @Override
    protected void extraConfigurations() {
        // no-op
    }

    @Override
    protected void extraAssertions() {
        // no-op
    }

    @Test
    public void testSizeSmall() throws Exception {
        givenQuery("SIZE == 'small'");
        expectPlan("SIZE == 'small'");
        planAndExecuteQuery();
    }

    @Test
    public void testSizeSmallAndUniqueColor() throws Exception {
        givenQuery("SIZE == 'small' && f:unique(COLOR)");
        expectPlan("SIZE == 'small'");
        planAndExecuteQuery();
    }

    @Test
    public void testSizeMediumAndUniqueColor() throws Exception {
        givenQuery("SIZE == 'medium' && f:unique(COLOR)");
        expectPlan("SIZE == 'medium'");
        planAndExecuteQuery();
    }

    @Test
    public void testSizeLargeAndUniqueColor() throws Exception {
        givenQuery("SIZE == 'large' && f:unique(COLOR)");
        expectPlan("SIZE == 'large'");
        planAndExecuteQuery();
    }

    @Test
    public void testSizeSmallAndGroupByColorShape() throws Exception {
        givenQuery("SIZE == 'small' && f:groupby(COLOR,SHAPE)");
        expectPlan("SIZE == 'small'");
        planAndExecuteQuery();
    }

    @Test
    public void testAllSizesAndGroupByColorShapeSize() throws Exception {
        givenQuery("(SIZE == 'small' || SIZE == 'medium' || SIZE == 'large') && f:groupby(COLOR,SHAPE,SIZE)");
        expectPlan("(SIZE == 'small' || SIZE == 'medium' || SIZE == 'large')");
        expectResultCount(ingest.getNumShards() * ingest.getNumEventsPerShard());
        planAndExecuteQuery();
    }

    @Test
    public void testSizeMedium() throws Exception {
        givenQuery("SIZE == 'medium'");
        expectPlan("SIZE == 'medium'");
        planAndExecuteQuery();
    }

    @Test
    public void testSizeLarge() throws Exception {
        givenQuery("SIZE == 'large'");
        expectPlan("SIZE == 'large'");
        planAndExecuteQuery();
    }

    @Test
    public void testAllSizes() throws Exception {
        givenQuery("SIZE == 'small' || SIZE == 'medium' ||  SIZE == 'large'");
        expectPlan("SIZE == 'small' || SIZE == 'medium' ||  SIZE == 'large'");
        expectResultCount(ingest.getNumShards() * ingest.getNumEventsPerShard());
        planAndExecuteQuery();
    }

    @Ignore
    @Test
    public void testRandomQuery() throws Exception {
        // there exist edge cases where no document satisfies this query due to random event generation.
        // when event metadata is generated a random query can be constructed from the metadata
        givenQuery("SIZE == 'small' && COLOR == 'green' && SHAPE == 'triangle'");
        expectPlan("SIZE == 'small' && COLOR == 'green' && SHAPE == 'triangle'");
        expectResultCount(0);
        planAndExecuteQuery();
    }
}
