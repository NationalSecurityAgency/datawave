package datawave.query.util;

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
import datawave.query.MultiNormalizerIngest;
import datawave.query.QueryParameters;
import datawave.query.exceptions.DatawaveQueryException;
import datawave.query.index.day.IndexIngestUtil;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;

/**
 * Test that simulates normalizer changes over time where some events have one normalizer applied but later a different normalizer is configured
 */
@RunWith(Arquillian.class)
public class MultiNormalizerTest extends AbstractQueryTest {

    private static final Logger log = LoggerFactory.getLogger(MultiNormalizerTest.class);

    @Inject
    @SpringBean(name = "EventQuery")
    protected ShardQueryLogic logic;

    @Override
    public ShardQueryLogic getLogic() {
        return logic;
    }

    private static final InMemoryInstance instance = new InMemoryInstance(MultiNormalizerTest.class.getName());
    private static AccumuloClient clientForSetup;
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
    public static void beforeClass() throws Exception {
        clientForSetup = new InMemoryAccumuloClient("", instance);

        MultiNormalizerIngest ingest = new MultiNormalizerIngest(clientForSetup);
        ingest.write();

        ingestUtil.write(clientForSetup, auths);
    }

    @Before
    public void beforeEach() {
        setClientForTest(clientForSetup);
        withParameter(QueryParameters.HIT_LIST, "true");
    }

    @Before
    public void setup() throws Exception {
        // default to full date range
        withDate("20250707", "20250708");
    }

    @AfterClass
    public static void teardown() {
        TypeRegistry.reset();
    }

    /**
     * Supports running the same query against multiple types of index tables
     *
     * @throws Exception
     *             if something goes wrong
     */
    public void planAndExecuteQueryAgainstMultipleIndices() throws Exception {
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
            assertPlannedQuery();
            assertResultCount();

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
    public void testColorRed() throws Exception {
        withQuery("COLOR == 'red'");
        withQueryPlan("COLOR == 'red'");
        withRequiredAllOf("COLOR:red");
        withResultCount(20);
        planAndExecuteQueryAgainstMultipleIndices();
    }

    @Test
    public void testSizeOne() throws Exception {
        withQuery("SIZE == '1'");
        withQueryPlan("SIZE == '+aE1' || SIZE == '1'");
        withDate("20250707");
        withRequiredAllOf("SIZE:1");
        withResultCount(1);
        planAndExecuteQueryAgainstMultipleIndices();

        withQueryPlan("SIZE == '+aE1' || SIZE == '1'");
        withDate("20250708");
        withRequiredAllOf("SIZE:1");
        withResultCount(1);
        planAndExecuteQueryAgainstMultipleIndices();

        withQueryPlan("SIZE == '+aE1' || SIZE == '1'");
        withDate("20250707", "20250708");
        withRequiredAllOf("SIZE:1");
        withResultCount(2);
        planAndExecuteQueryAgainstMultipleIndices();
    }

    @Test
    public void testColorRedSizeSix() throws Exception {
        withQuery("COLOR == 'red' && SIZE == '6'");
        withQueryPlan("COLOR == 'red' && (SIZE == '+aE6' || SIZE == '6')");
        withDate("20250707");
        withRequiredAllOf("COLOR:red", "SIZE:6");
        withResultCount(1);
        planAndExecuteQueryAgainstMultipleIndices();

        withQueryPlan("COLOR == 'red' && (SIZE == '+aE6' || SIZE == '6')");
        withDate("20250708");
        withRequiredAllOf("COLOR:red", "SIZE:6");
        withResultCount(1);
        planAndExecuteQueryAgainstMultipleIndices();

        withQueryPlan("COLOR == 'red' && (SIZE == '+aE6' || SIZE == '6')");
        withDate("20250707", "20250708");
        withRequiredAllOf("COLOR:red", "SIZE:6");
        withResultCount(2);
        planAndExecuteQueryAgainstMultipleIndices();
    }

    @Test
    public void testRangeSizeOneToTwo_firstDay() throws Exception {
        // range with text normalizer expands into value "10", while technically correct this is wrong for numeric data
        withQuery("((_Bounded_ = true) && (SIZE >= '1' && SIZE <= '2'))");
        withQueryPlan("(SIZE == '1' || SIZE == '10' || SIZE == '2')");
        withDate("20250707");
        withRequiredAnyOf("SIZE:1", "SIZE:10", "SIZE:2");
        withResultCount(3);
        planAndExecuteQueryAgainstMultipleIndices();
    }

    @Test
    public void testRangeSizeOneToTwo_lastDay() throws Exception {
        // range with numeric normalizer does not expand into "10". This is more correct.
        withQuery("((_Bounded_ = true) && (SIZE >= '1' && SIZE <= '2'))");
        withQueryPlan("(SIZE == '+aE1' || SIZE == '+aE2')");
        withDate("20250708");
        withRequiredAnyOf("SIZE:1", "SIZE:2");
        withResultCount(2);
        planAndExecuteQueryAgainstMultipleIndices();
    }

    @Test
    public void testRangeSizeOneToTwo_bothDays() throws Exception {
        // range with both normalizers applied will expand into all values above, including the incorrect value "10"
        withQuery("((_Bounded_ = true) && (SIZE >= '1' && SIZE <= '2'))");
        withQueryPlan("(SIZE == '+aE1' || SIZE == '+aE2' || SIZE == '1' || SIZE == '10' || SIZE == '2')");
        withDate("20250707", "20250708");
        withRequiredAnyOf("SIZE:1", "SIZE:10", "SIZE:2");
        withResultCount(5);
        planAndExecuteQueryAgainstMultipleIndices();
    }

    @Test
    public void testRangeSizeOneToTwo_rangeExpansionDisabled() throws Exception {
        try {
            // simulate a bounded range expansion failure
            ((DefaultQueryPlanner) logic.getQueryPlanner()).setDisableBoundedLookup(true);

            // range with text normalizer will incorrectly return result [SIZE:10]
            withQuery("((_Bounded_ = true) && (SIZE >= '1' && SIZE <= '2'))");
            withQueryPlan("(((_Bounded_ = true) && (SIZE >= '+aE1' && SIZE <= '+aE2')) || ((_Bounded_ = true) && (SIZE >= '1' && SIZE <= '2')))");
            withDate("20250707");
            withRequiredAnyOf("SIZE:1", "SIZE:10", "SIZE:2");
            withResultCount(3);
            planAndExecuteQueryAgainstMultipleIndices();

            // because both ranges are pushed down to shard 20250708 and all normalized forms are applied at evaluation,
            // the text range will return the result with SIZE:10. While technically correct this was not the intent.
            withQueryPlan("(((_Bounded_ = true) && (SIZE >= '+aE1' && SIZE <= '+aE2')) || ((_Bounded_ = true) && (SIZE >= '1' && SIZE <= '2')))");
            withDate("20250708");
            withRequiredAnyOf("SIZE:1", "SIZE:10", "SIZE:2");
            withResultCount(3);
            planAndExecuteQueryAgainstMultipleIndices();

            // for reasons stated above this query returns result SIZE:10 from both shards
            withQueryPlan("(((_Bounded_ = true) && (SIZE >= '+aE1' && SIZE <= '+aE2')) || ((_Bounded_ = true) && (SIZE >= '1' && SIZE <= '2')))");
            withDate("20250707", "20250708");
            withRequiredAnyOf("SIZE:1", "SIZE:10", "SIZE:2");
            withResultCount(6);
            planAndExecuteQueryAgainstMultipleIndices();
        } finally {
            ((DefaultQueryPlanner) logic.getQueryPlanner()).setDisableBoundedLookup(false);
        }
    }

    @Test
    public void testRangeSizeFourToTen() throws Exception {
        // range with text normalizer is not valid and thus finds zero hits
        withQuery("((_Bounded_ = true) && (SIZE >= '4' && SIZE <= '10'))");
        withQueryPlan("((_Bounded_ = true) && (SIZE >= '4' && SIZE <= '10'))");
        withDate("20250707");
        withResultCount(0);
        planAndExecuteQueryAgainstMultipleIndices();

        // range with numeric normalizer will find hits in the shard index and expand into discrete values
        withQueryPlan("(SIZE == '+aE4' || SIZE == '+aE5' || SIZE == '+aE6' || SIZE == '+aE7' || SIZE == '+aE8' || SIZE == '+aE9' || SIZE == '+bE1' || ((_Bounded_ = true) && (SIZE >= '4' && SIZE <= '10')))");
        withDate("20250708");
        withRequiredAnyOf("SIZE:4", "SIZE:5", "SIZE:6", "SIZE:7", "SIZE:8", "SIZE:9", "SIZE:10");
        withResultCount(7);
        planAndExecuteQueryAgainstMultipleIndices();

        withQueryPlan("(SIZE == '+aE4' || SIZE == '+aE5' || SIZE == '+aE6' || SIZE == '+aE7' || SIZE == '+aE8' || SIZE == '+aE9' || SIZE == '+bE1' || ((_Bounded_ = true) && (SIZE >= '4' && SIZE <= '10')))");
        withDate("20250707", "20250708");
        withRequiredAnyOf("SIZE:4", "SIZE:5", "SIZE:6", "SIZE:7", "SIZE:8", "SIZE:9", "SIZE:10");
        withResultCount(7);
        planAndExecuteQueryAgainstMultipleIndices();
    }

    @Test(expected = DatawaveQueryException.class)
    public void testRangeSizeFourToTen_rangeExpansionDisabled() throws Exception {
        try {
            // simulate a bounded range expansion failure
            ((DefaultQueryPlanner) logic.getQueryPlanner()).setDisableBoundedLookup(true);

            // numeric range still matches against numeric data that has a text normalizer
            // withQuery("((_Bounded_ = true) && (SIZE >= '4' && SIZE <= '10'))"); // expected when validating bounded ranges
            withQuery("(((_Bounded_ = true) && (SIZE >= '+aE4' && SIZE <= '+bE1')) || ((_Bounded_ = true) && (SIZE >= '4' && SIZE <= '10')))");
            withQueryPlan("((_Bounded_ = true) && (SIZE >= '+aE4' && SIZE <= '+bE1'))");
            withDate("20250707");
            withRequiredAnyOf("SIZE:4", "SIZE:5", "SIZE:6", "SIZE:7", "SIZE:8", "SIZE:9", "SIZE:10");
            withResultCount(7);
            planAndExecuteQueryAgainstMultipleIndices();

            // numeric range matches against numeric data with number normalizer
            withQueryPlan("((_Bounded_ = true) && (SIZE >= '+aE4' && SIZE <= '+bE1'))");
            withDate("20250708");
            withRequiredAnyOf("SIZE:4", "SIZE:5", "SIZE:6", "SIZE:7", "SIZE:8", "SIZE:9", "SIZE:10");
            withResultCount(7);
            planAndExecuteQueryAgainstMultipleIndices();

            // numeric range matches against numeric data with either a text or number normalizer
            withQueryPlan("((_Bounded_ = true) && (SIZE >= '+aE4' && SIZE <= '+bE1'))");
            withDate("20250707", "20250708");
            withRequiredAnyOf("SIZE:4", "SIZE:5", "SIZE:6", "SIZE:7", "SIZE:8", "SIZE:9", "SIZE:10");
            withResultCount(14);
            planAndExecuteQueryAgainstMultipleIndices();
        } finally {
            ((DefaultQueryPlanner) logic.getQueryPlanner()).setDisableBoundedLookup(false);
        }
    }

    @Test
    public void testRangeSizeFourToTenWithAnchor() throws Exception {
        // range with text normalizer will not find any hits
        withQuery("COLOR == 'red' && ((_Bounded_ = true) && (SIZE >= '4' && SIZE <= '10'))");
        withQueryPlan("COLOR == 'red' && ((_Bounded_ = true) && (SIZE >= '4' && SIZE <= '10'))");
        withDate("20250707");
        withResultCount(0);
        planAndExecuteQueryAgainstMultipleIndices();
        assertResultCount(0);

        // range with numeric normalizer finds expected hits
        withQueryPlan("COLOR == 'red' && (SIZE == '+aE4' || SIZE == '+aE5' || SIZE == '+aE6' || SIZE == '+aE7' || SIZE == '+aE8' || SIZE == '+aE9' || SIZE == '+bE1' || ((_Bounded_ = true) && (SIZE >= '4' && SIZE <= '10')))");
        withDate("20250708");
        withRequiredAnyOf("COLOR:red", "SIZE:4", "SIZE:5", "SIZE:6", "SIZE:7", "SIZE:8", "SIZE:9", "SIZE:10");
        withResultCount(7);
        planAndExecuteQueryAgainstMultipleIndices();

        withQueryPlan("COLOR == 'red' && (SIZE == '+aE4' || SIZE == '+aE5' || SIZE == '+aE6' || SIZE == '+aE7' || SIZE == '+aE8' || SIZE == '+aE9' || SIZE == '+bE1' || ((_Bounded_ = true) && (SIZE >= '4' && SIZE <= '10')))");
        withDate("20250707", "20250708");
        withRequiredAnyOf("COLOR:red", "SIZE:4", "SIZE:5", "SIZE:6", "SIZE:7", "SIZE:8", "SIZE:9", "SIZE:10");
        withResultCount(7);
        planAndExecuteQueryAgainstMultipleIndices();
    }

    @Test(expected = DatawaveQueryException.class)
    public void testRangeSizeFourToTenWithAnchor_rangeExpansionDisabled() throws Exception {
        try {
            // simulate a bounded range expansion failure
            ((DefaultQueryPlanner) logic.getQueryPlanner()).setDisableBoundedLookup(true);

            // range with text normalizer would ordinarily not find any hits but anchor term nominates candidates. At
            // evaluation multiple normalizers are applied, thus text number can match the numeric range
            // withQuery("COLOR == 'red' && ((_Bounded_ = true) && (SIZE >= '4' && SIZE <= '10'))"); // expected when validating bounded ranges
            withQuery("COLOR == 'red' && (((_Bounded_ = true) && (SIZE >= '+aE4' && SIZE <= '+bE1')) || ((_Bounded_ = true) && (SIZE >= '4' && SIZE <= '10')))");
            withQueryPlan("COLOR == 'red' && ((_Bounded_ = true) && (SIZE >= '+aE4' && SIZE <= '+bE1'))");
            withDate("20250707");
            withRequiredAnyOf("COLOR:red", "SIZE:4", "SIZE:5", "SIZE:6", "SIZE:7", "SIZE:8", "SIZE:9", "SIZE:10");
            withResultCount(7);
            planAndExecuteQueryAgainstMultipleIndices();

            // range with numeric normalizer finds expected hits
            withQueryPlan("COLOR == 'red' && ((_Bounded_ = true) && (SIZE >= '+aE4' && SIZE <= '+bE1'))");
            withDate("20250708");
            withRequiredAnyOf("COLOR:red", "SIZE:4", "SIZE:5", "SIZE:6", "SIZE:7", "SIZE:8", "SIZE:9", "SIZE:10");
            withResultCount(7);
            planAndExecuteQueryAgainstMultipleIndices();

            // anchor term plus multi-normalization at evaluation time allows all valid hits to be found
            withQueryPlan("COLOR == 'red' && ((_Bounded_ = true) && (SIZE >= '+aE4' && SIZE <= '+bE1'))");
            withDate("20250707", "20250708");
            withRequiredAnyOf("COLOR:red", "SIZE:4", "SIZE:5", "SIZE:6", "SIZE:7", "SIZE:8", "SIZE:9", "SIZE:10");
            withResultCount(14);
            planAndExecuteQueryAgainstMultipleIndices();
        } finally {
            ((DefaultQueryPlanner) logic.getQueryPlanner()).setDisableBoundedLookup(false);
        }
    }
}
