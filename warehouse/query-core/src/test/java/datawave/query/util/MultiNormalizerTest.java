package datawave.query.util;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.accumulo.core.client.AccumuloClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
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
import datawave.query.MultiNormalizerIngest;
import datawave.query.QueryParameters;
import datawave.query.exceptions.DatawaveQueryException;
import datawave.query.index.day.IndexIngestUtil;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.tables.ShardQueryLogic;

/**
 * Test that simulates normalizer changes over time where some events have one normalizer applied but later a different normalizer is configured
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
public class MultiNormalizerTest extends AbstractQueryTest {

    private static final Logger log = LoggerFactory.getLogger(MultiNormalizerTest.class);

    @Autowired
    @Qualifier("EventQuery")
    protected ShardQueryLogic logic;

    @Override
    public ShardQueryLogic getLogic() {
        return logic;
    }

    private static final InMemoryInstance instance = new InMemoryInstance(MultiNormalizerTest.class.getName());
    private static AccumuloClient clientForSetup;
    private static final IndexIngestUtil ingestUtil = new IndexIngestUtil();

    @BeforeAll
    public static void beforeAll() throws Exception {
        clientForSetup = new InMemoryAccumuloClient("", instance);

        MultiNormalizerIngest ingest = new MultiNormalizerIngest(clientForSetup);
        ingest.write();

        ingestUtil.write(clientForSetup, auths);
    }

    @BeforeEach
    public void beforeEach() {
        setClientForTest(clientForSetup);
        givenParameter(QueryParameters.HIT_LIST, "true");
        // default to full date range
        givenDate("20250707", "20250708");
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
    public void testColorRed() throws Exception {
        givenQuery("COLOR == 'red'");
        expectPlan("COLOR == 'red'");
        expectHitTermsRequiredAllOf("COLOR:red");
        expectResultCount(20);
        planAndExecuteQuery();
    }

    @Test
    public void testSizeOne() throws Exception {
        givenDate("20250707");
        givenQuery("SIZE == '1'");
        expectPlan("SIZE == '+aE1' || SIZE == '1'");
        expectResultCount(1);
        expectHitTermsRequiredAllOf("SIZE:1");
        planAndExecuteQuery();

        givenDate("20250708");
        expectPlan("SIZE == '+aE1' || SIZE == '1'");
        expectResultCount(1);
        expectHitTermsRequiredAllOf("SIZE:1");
        planAndExecuteQuery();

        givenDate("20250707", "20250708");
        expectPlan("SIZE == '+aE1' || SIZE == '1'");
        expectResultCount(2);
        expectHitTermsRequiredAllOf("SIZE:1");
        planAndExecuteQuery();
    }

    @Test
    public void testColorRedSizeSix() throws Exception {
        givenDate("20250707");
        givenQuery("COLOR == 'red' && SIZE == '6'");
        expectPlan("COLOR == 'red' && (SIZE == '+aE6' || SIZE == '6')");
        expectResultCount(1);
        expectHitTermsRequiredAllOf("COLOR:red", "SIZE:6");
        planAndExecuteQuery();

        givenDate("20250708");
        expectPlan("COLOR == 'red' && (SIZE == '+aE6' || SIZE == '6')");
        expectResultCount(1);
        expectHitTermsRequiredAllOf("COLOR:red", "SIZE:6");
        planAndExecuteQuery();

        givenDate("20250707", "20250708");
        expectPlan("COLOR == 'red' && (SIZE == '+aE6' || SIZE == '6')");
        expectResultCount(2);
        expectHitTermsRequiredAllOf("COLOR:red", "SIZE:6");
        planAndExecuteQuery();
    }

    @Test
    public void testRangeSizeOneToTwo_firstDay() throws Exception {
        // range with text normalizer expands into value "10", while technically correct this is wrong for numeric data
        givenDate("20250707");
        givenQuery("((_Bounded_ = true) && (SIZE >= '1' && SIZE <= '2'))");
        expectPlan("(SIZE == '1' || SIZE == '10' || SIZE == '2')");
        expectResultCount(3);
        expectHitTermsRequiredAnyOf("SIZE:1", "SIZE:10", "SIZE:2");
        planAndExecuteQuery();
    }

    @Test
    public void testRangeSizeOneToTwo_lastDay() throws Exception {
        // range with numeric normalizer does not expand into "10". This is more correct.
        givenDate("20250708");
        givenQuery("((_Bounded_ = true) && (SIZE >= '1' && SIZE <= '2'))");
        expectPlan("(SIZE == '+aE1' || SIZE == '+aE2')");
        expectResultCount(2);
        expectHitTermsRequiredAnyOf("SIZE:1", "SIZE:2");
        planAndExecuteQuery();
    }

    @Test
    public void testRangeSizeOneToTwo_bothDays() throws Exception {
        // range with both normalizers applied will expand into all values above, including the incorrect value "10"
        givenDate("20250707", "20250708");
        givenQuery("((_Bounded_ = true) && (SIZE >= '1' && SIZE <= '2'))");
        expectPlan("(SIZE == '+aE1' || SIZE == '+aE2' || SIZE == '1' || SIZE == '10' || SIZE == '2')");
        expectResultCount(5);
        expectHitTermsRequiredAnyOf("SIZE:1", "SIZE:10", "SIZE:2");
        planAndExecuteQuery();
    }

    @Test
    public void testRangeSizeOneToTwo_rangeExpansionDisabled() throws Exception {
        try {
            // simulate a bounded range expansion failure
            ((DefaultQueryPlanner) logic.getQueryPlanner()).setDisableBoundedLookup(true);

            // range with text normalizer will incorrectly return result [SIZE:10]
            givenDate("20250707");
            givenQuery("((_Bounded_ = true) && (SIZE >= '1' && SIZE <= '2'))");
            expectPlan("(((_Bounded_ = true) && (SIZE >= '+aE1' && SIZE <= '+aE2')) || ((_Bounded_ = true) && (SIZE >= '1' && SIZE <= '2')))");
            expectResultCount(3);
            expectHitTermsRequiredAnyOf("SIZE:1", "SIZE:10", "SIZE:2");
            planAndExecuteQuery();

            // because both ranges are pushed down to shard 20250708 and all normalized forms are applied at evaluation,
            // the text range will return the result with SIZE:10. While technically correct this was not the intent.
            givenDate("20250708");
            expectPlan("(((_Bounded_ = true) && (SIZE >= '+aE1' && SIZE <= '+aE2')) || ((_Bounded_ = true) && (SIZE >= '1' && SIZE <= '2')))");
            expectResultCount(3);
            expectHitTermsRequiredAnyOf("SIZE:1", "SIZE:10", "SIZE:2");
            planAndExecuteQuery();

            // for reasons stated above this query returns result SIZE:10 from both shards
            givenDate("20250707", "20250708");
            expectPlan("(((_Bounded_ = true) && (SIZE >= '+aE1' && SIZE <= '+aE2')) || ((_Bounded_ = true) && (SIZE >= '1' && SIZE <= '2')))");
            expectResultCount(6);
            expectHitTermsRequiredAnyOf("SIZE:1", "SIZE:10", "SIZE:2");
            planAndExecuteQuery();
        } finally {
            ((DefaultQueryPlanner) logic.getQueryPlanner()).setDisableBoundedLookup(false);
        }
    }

    @Test
    public void testRangeSizeFourToTen() throws Exception {
        // range with text normalizer is not valid and thus finds zero hits
        givenQuery("((_Bounded_ = true) && (SIZE >= '4' && SIZE <= '10'))");
        givenDate("20250707");
        expectPlan("((_Bounded_ = true) && (SIZE >= '4' && SIZE <= '10'))");
        expectResultCount(0);
        planAndExecuteQuery();

        // range with numeric normalizer will find hits in the shard index and expand into discrete values
        givenDate("20250708");
        expectPlan("(SIZE == '+aE4' || SIZE == '+aE5' || SIZE == '+aE6' || SIZE == '+aE7' || SIZE == '+aE8' || SIZE == '+aE9' || SIZE == '+bE1' || ((_Bounded_ = true) && (SIZE >= '4' && SIZE <= '10')))");
        expectResultCount(7);
        expectHitTermsRequiredAnyOf("SIZE:4", "SIZE:5", "SIZE:6", "SIZE:7", "SIZE:8", "SIZE:9", "SIZE:10");
        planAndExecuteQuery();

        givenDate("20250707", "20250708");
        expectPlan("(SIZE == '+aE4' || SIZE == '+aE5' || SIZE == '+aE6' || SIZE == '+aE7' || SIZE == '+aE8' || SIZE == '+aE9' || SIZE == '+bE1' || ((_Bounded_ = true) && (SIZE >= '4' && SIZE <= '10')))");
        expectResultCount(7);
        expectHitTermsRequiredAnyOf("SIZE:4", "SIZE:5", "SIZE:6", "SIZE:7", "SIZE:8", "SIZE:9", "SIZE:10");
        planAndExecuteQuery();
    }

    @Test
    public void testRangeSizeFourToTen_rangeExpansionDisabled() throws Exception {
        try {
            // simulate a bounded range expansion failure
            ((DefaultQueryPlanner) logic.getQueryPlanner()).setDisableBoundedLookup(true);

            // numeric range still matches against numeric data that has a text normalizer
            // withQuery("((_Bounded_ = true) && (SIZE >= '4' && SIZE <= '10'))"); // expected when validating bounded ranges
            givenDate("20250707");
            givenQuery("(((_Bounded_ = true) && (SIZE >= '+aE4' && SIZE <= '+bE1')) || ((_Bounded_ = true) && (SIZE >= '4' && SIZE <= '10')))");
            expectPlan("((_Bounded_ = true) && (SIZE >= '+aE4' && SIZE <= '+bE1'))");
            expectResultCount(7);
            expectHitTermsRequiredAnyOf("SIZE:4", "SIZE:5", "SIZE:6", "SIZE:7", "SIZE:8", "SIZE:9", "SIZE:10");
            assertThrows(DatawaveQueryException.class, this::planAndExecuteQuery);

            // numeric range matches against numeric data with number normalizer
            givenDate("20250708");
            expectPlan("((_Bounded_ = true) && (SIZE >= '+aE4' && SIZE <= '+bE1'))");
            expectResultCount(7);
            expectHitTermsRequiredAnyOf("SIZE:4", "SIZE:5", "SIZE:6", "SIZE:7", "SIZE:8", "SIZE:9", "SIZE:10");
            assertThrows(DatawaveQueryException.class, this::planAndExecuteQuery);

            // numeric range matches against numeric data with either a text or number normalizer
            givenDate("20250707", "20250708");
            expectPlan("((_Bounded_ = true) && (SIZE >= '+aE4' && SIZE <= '+bE1'))");
            expectResultCount(14);
            expectHitTermsRequiredAnyOf("SIZE:4", "SIZE:5", "SIZE:6", "SIZE:7", "SIZE:8", "SIZE:9", "SIZE:10");
            assertThrows(DatawaveQueryException.class, this::planAndExecuteQuery);
        } finally {
            ((DefaultQueryPlanner) logic.getQueryPlanner()).setDisableBoundedLookup(false);
        }
    }

    @Test
    public void testRangeSizeFourToTenWithAnchor() throws Exception {
        // range with text normalizer will not find any hits
        givenDate("20250707");
        givenQuery("COLOR == 'red' && ((_Bounded_ = true) && (SIZE >= '4' && SIZE <= '10'))");
        expectPlan("COLOR == 'red' && ((_Bounded_ = true) && (SIZE >= '4' && SIZE <= '10'))");
        expectResultCount(0);
        planAndExecuteQuery();

        // range with numeric normalizer finds expected hits
        givenDate("20250708");
        expectPlan("COLOR == 'red' && (SIZE == '+aE4' || SIZE == '+aE5' || SIZE == '+aE6' || SIZE == '+aE7' || SIZE == '+aE8' || SIZE == '+aE9' || SIZE == '+bE1' || ((_Bounded_ = true) && (SIZE >= '4' && SIZE <= '10')))");
        expectResultCount(7);
        expectHitTermsRequiredAnyOf("COLOR:red", "SIZE:4", "SIZE:5", "SIZE:6", "SIZE:7", "SIZE:8", "SIZE:9", "SIZE:10");
        planAndExecuteQuery();

        givenDate("20250707", "20250708");
        expectPlan("COLOR == 'red' && (SIZE == '+aE4' || SIZE == '+aE5' || SIZE == '+aE6' || SIZE == '+aE7' || SIZE == '+aE8' || SIZE == '+aE9' || SIZE == '+bE1' || ((_Bounded_ = true) && (SIZE >= '4' && SIZE <= '10')))");
        expectResultCount(7);
        expectHitTermsRequiredAnyOf("COLOR:red", "SIZE:4", "SIZE:5", "SIZE:6", "SIZE:7", "SIZE:8", "SIZE:9", "SIZE:10");
        planAndExecuteQuery();
    }

    @Test
    public void testRangeSizeFourToTenWithAnchor_rangeExpansionDisabled() throws Exception {
        try {
            // simulate a bounded range expansion failure
            ((DefaultQueryPlanner) logic.getQueryPlanner()).setDisableBoundedLookup(true);

            // range with text normalizer would ordinarily not find any hits but anchor term nominates candidates. At
            // evaluation multiple normalizers are applied, thus text number can match the numeric range
            // withQuery("COLOR == 'red' && ((_Bounded_ = true) && (SIZE >= '4' && SIZE <= '10'))"); // expected when validating bounded ranges
            givenDate("20250707");
            givenQuery("COLOR == 'red' && (((_Bounded_ = true) && (SIZE >= '+aE4' && SIZE <= '+bE1')) || ((_Bounded_ = true) && (SIZE >= '4' && SIZE <= '10')))");
            expectPlan("COLOR == 'red' && ((_Bounded_ = true) && (SIZE >= '+aE4' && SIZE <= '+bE1'))");
            expectResultCount(7);
            expectHitTermsRequiredAnyOf("COLOR:red", "SIZE:4", "SIZE:5", "SIZE:6", "SIZE:7", "SIZE:8", "SIZE:9", "SIZE:10");
            assertThrows(DatawaveQueryException.class, this::planAndExecuteQuery);

            // range with numeric normalizer finds expected hits
            givenDate("20250708");
            expectPlan("COLOR == 'red' && ((_Bounded_ = true) && (SIZE >= '+aE4' && SIZE <= '+bE1'))");
            expectResultCount(7);
            expectHitTermsRequiredAnyOf("COLOR:red", "SIZE:4", "SIZE:5", "SIZE:6", "SIZE:7", "SIZE:8", "SIZE:9", "SIZE:10");
            assertThrows(DatawaveQueryException.class, this::planAndExecuteQuery);

            // anchor term plus multi-normalization at evaluation time allows all valid hits to be found
            givenDate("20250707", "20250708");
            expectPlan("COLOR == 'red' && ((_Bounded_ = true) && (SIZE >= '+aE4' && SIZE <= '+bE1'))");
            expectResultCount(14);
            expectHitTermsRequiredAnyOf("COLOR:red", "SIZE:4", "SIZE:5", "SIZE:6", "SIZE:7", "SIZE:8", "SIZE:9", "SIZE:10");
            assertThrows(DatawaveQueryException.class, this::planAndExecuteQuery);
        } finally {
            ((DefaultQueryPlanner) logic.getQueryPlanner()).setDisableBoundedLookup(false);
        }
    }
}
