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
import datawave.query.util.TestIndexTableNames;
import datawave.util.TableName;
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
        withDate("20250606", "20250606");
        clientForTest = clientForSetup;
        setClientForTest(clientForTest);
    }

    @AfterClass
    public static void teardown() {
        TypeRegistry.reset();
    }

    /**
     * Plans and executes a query against multiple versions of a shard index
     *
     * @throws Exception
     *             if something goes wrong
     */
    @Override
    public void planAndExecuteQuery() throws Exception {
        for (String indexTableName : TestIndexTableNames.names()) {
            logic.setIndexTableName(indexTableName);
            switch (indexTableName) {
                case TestIndexTableNames.SHARD_INDEX:
                    logic.setIndexTableName(TableName.SHARD_INDEX);
                    break;
                case TestIndexTableNames.NO_UID_INDEX:
                    logic.setIndexTableName(TestIndexTableNames.NO_UID_INDEX);
                    break;
                case TestIndexTableNames.TRUNCATED_INDEX:
                    logic.setUseTruncatedIndex(true);
                    break;
                default:
                    throw new IllegalStateException("Unknown index table name " + indexTableName);
            }

            log.debug("=== using index: {} ===", indexTableName);
            planQuery();
            executeQuery();
            assertPlannedQuery();
            // TODO: while generating random events also generate metadata so the framework can assert result counts

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
    }

    @Test
    public void testSizeSmall() throws Exception {
        withQuery("SIZE == 'small'");
        withQueryPlan("SIZE == 'small'");
        planAndExecuteQuery();
    }

    @Test
    public void testSizeSmallAndUniqueColor() throws Exception {
        withQuery("SIZE == 'small' && f:unique(COLOR)");
        withQueryPlan("SIZE == 'small'");
        planAndExecuteQuery();
    }

    @Test
    public void testSizeMediumAndUniqueColor() throws Exception {
        withQuery("SIZE == 'medium' && f:unique(COLOR)");
        withQueryPlan("SIZE == 'medium'");
        planAndExecuteQuery();
    }

    @Test
    public void testSizeLargeAndUniqueColor() throws Exception {
        withQuery("SIZE == 'large' && f:unique(COLOR)");
        withQueryPlan("SIZE == 'large'");
        planAndExecuteQuery();
    }

    @Test
    public void testSizeSmallAndGroupByColorShape() throws Exception {
        withQuery("SIZE == 'small' && f:groupby(COLOR,SHAPE)");
        withQueryPlan("SIZE == 'small'");
        planAndExecuteQuery();
    }

    @Test
    public void testAllSizesAndGroupByColorShapeSize() throws Exception {
        withQuery("(SIZE == 'small' || SIZE == 'medium' || SIZE == 'large') && f:groupby(COLOR,SHAPE,SIZE)");
        withQueryPlan("(SIZE == 'small' || SIZE == 'medium' || SIZE == 'large')");
        withResultCount(ingest.getNumShards() * ingest.getNumEventsPerShard());
        planAndExecuteQuery();
    }

    @Test
    public void testSizeMedium() throws Exception {
        withQuery("SIZE == 'medium'");
        withQueryPlan("SIZE == 'medium'");
        planAndExecuteQuery();
    }

    @Test
    public void testSizeLarge() throws Exception {
        withQuery("SIZE == 'large'");
        withQueryPlan("SIZE == 'large'");
        planAndExecuteQuery();
    }

    @Test
    public void testAllSizes() throws Exception {
        withQuery("SIZE == 'small' || SIZE == 'medium' ||  SIZE == 'large'");
        withQueryPlan("SIZE == 'small' || SIZE == 'medium' ||  SIZE == 'large'");
        withResultCount(ingest.getNumShards() * ingest.getNumEventsPerShard());
        planAndExecuteQuery();
    }

    @Test
    public void testRandomQuery() throws Exception {
        withQuery("SIZE == 'small' && COLOR == 'green' && SHAPE == 'triangle'");
        withQueryPlan("SIZE == 'small' && COLOR == 'green' && SHAPE == 'triangle'");
        withResultCount(0);
        planAndExecuteQuery();
    }
}
