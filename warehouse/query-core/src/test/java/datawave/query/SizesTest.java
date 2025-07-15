package datawave.query;

import static datawave.query.util.AbstractQueryTest.RangeType.DOCUMENT;
import static datawave.query.util.AbstractQueryTest.RangeType.SHARD;

import javax.inject.Inject;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
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
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.util.AbstractQueryTest;
import datawave.query.util.SizesIngest;
import datawave.util.TableName;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;

/**
 * This suite of tests exercises many random events over a small number of shards
 */
public abstract class SizesTest extends AbstractQueryTest {

    private static final Logger log = LoggerFactory.getLogger(SizesTest.class);

    @Inject
    @SpringBean(name = "EventQuery")
    protected ShardQueryLogic logic;

    @Override
    public ShardQueryLogic getLogic() {
        return logic;
    }

    @RunWith(Arquillian.class)
    public static class ShardRangeTest extends SizesTest {

        protected static AccumuloClient client = null;

        @BeforeClass
        public static void setUp() throws Exception {
            InMemoryInstance i = new InMemoryInstance(ShardRangeTest.class.getName());
            client = new InMemoryAccumuloClient("", i);

            SizesIngest ingest = new SizesIngest(client);
            ingest.write(SHARD);

            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(client, auths, TableName.SHARD);
            PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(client, auths, TableName.METADATA);
        }

        @Before
        public void beforeEach() {
            setClientForTest(client);
        }
    }

    @RunWith(Arquillian.class)
    public static class DocumentRangeTest extends SizesTest {

        protected static AccumuloClient client = null;

        @BeforeClass
        public static void setUp() throws Exception {
            InMemoryInstance i = new InMemoryInstance(DocumentRangeTest.class.getName());
            client = new InMemoryAccumuloClient("", i);

            SizesIngest ingest = new SizesIngest(client);
            ingest.write(DOCUMENT);

            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(client, auths, TableName.SHARD);
            PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(client, auths, TableName.METADATA);
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

    public void planAndExecuteQuery() throws Exception {
        planQuery();
        executeQuery();
        // TODO: assert based on test metadata
    }

    @Before
    public void setup() throws Exception {
        withDate("20250606", "20250606");
    }

    @AfterClass
    public static void teardown() {
        TypeRegistry.reset();
    }

    @Test
    public void testSizeSmall() throws Exception {
        withQuery("SIZE == 'small'");
        planAndExecuteQuery();
    }

    @Test
    public void testSizeSmallAndUniqueColor() throws Exception {
        withQuery("SIZE == 'small' && f:unique(COLOR)");
        planAndExecuteQuery();
    }

    @Test
    public void testSizeMediumAndUniqueColor() throws Exception {
        withQuery("SIZE == 'medium' && f:unique(COLOR)");
        planAndExecuteQuery();
    }

    @Test
    public void testSizeLargeAndUniqueColor() throws Exception {
        withQuery("SIZE == 'large' && f:unique(COLOR)");
        planAndExecuteQuery();
    }

    @Test
    public void testSizeSmallAndGroupByColorShape() throws Exception {
        withQuery("SIZE == 'small' && f:groupby(COLOR,SHAPE)");
        planAndExecuteQuery();
    }

    @Test
    public void testSizeMedium() throws Exception {
        withQuery("SIZE == 'medium'");
        planAndExecuteQuery();
    }

    @Test
    public void testSizeLarge() throws Exception {
        withQuery("SIZE == 'large'");
        planAndExecuteQuery();
    }

    @Test
    public void testAllSizes() throws Exception {
        withQuery("SIZE == 'small' || SIZE == 'medium' ||  SIZE == 'large'");
        planAndExecuteQuery();
    }

    @Test
    public void testRandomQuery() throws Exception {
        withQuery("SIZE == 'small' && COLOR == 'green' && SHAPE == 'triangle'");
        planAndExecuteQuery();
    }
}
