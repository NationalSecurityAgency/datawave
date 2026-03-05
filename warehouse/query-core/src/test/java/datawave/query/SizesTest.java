package datawave.query;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.ingest.data.TypeRegistry;
import datawave.query.index.day.IndexIngestUtil;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.AbstractQueryTest;
import datawave.query.util.SizesIngest;

/**
 * This suite of tests exercises many random events over a small number of shards
 */
@ExtendWith(SpringExtension.class)
@ComponentScan(basePackages = "datawave.query")
// @formatter:off
@ContextConfiguration(locations = {
        "classpath:datawave/query/QueryLogicFactory.xml",
        "classpath:MarkingFunctionsContext.xml",
        "classpath:MetadataHelperContext.xml",
        "classpath:CacheContext.xml"})
// @formatter:on
public class SizesTest extends AbstractQueryTest {

    private static final Logger log = LoggerFactory.getLogger(SizesTest.class);

    // static utilities for test
    private static final InMemoryInstance instance = new InMemoryInstance(SizesTest.class.getName());
    private static AccumuloClient clientForSetup;
    private static SizesIngest ingest;

    private static final Authorizations auths = new Authorizations("ALL");

    // utility for writing different index table structures
    private static final IndexIngestUtil ingestUtil = new IndexIngestUtil();

    @Autowired
    @Qualifier("EventQuery")
    protected ShardQueryLogic logic;

    @Override
    public ShardQueryLogic getLogic() {
        return logic;
    }

    @Override
    public Authorizations getAuths() {
        return auths;
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        clientForSetup = new InMemoryAccumuloClient("", instance);

        ingest = new SizesIngest(clientForSetup);
        ingest.write();

        ingestUtil.write(clientForSetup, auths);
    }

    @BeforeEach
    public void beforeEach() throws Exception {
        givenDate("20250606", "20250606");
        setClientForTest(clientForSetup);
    }

    @AfterAll
    public static void afterAll() {
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

    @Disabled
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
