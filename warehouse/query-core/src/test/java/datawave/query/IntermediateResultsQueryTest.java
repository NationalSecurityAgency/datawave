package datawave.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.cache.ResultsPage;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.query.index.day.IndexIngestUtil;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.tables.CountingShardQueryLogic;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.AbstractQueryTest;
import datawave.query.util.ShapesIngest;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.test.MacTestUtil;
import datawave.util.TableName;
import datawave.webservice.query.cache.RunningQueryTimingImpl;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.runner.RunningQuery;

@ExtendWith(SpringExtension.class)
@ComponentScan(basePackages = "datawave.query")
// @formatter:off
@ContextConfiguration(locations = {
        "classpath:datawave/query/QueryLogicFactory.xml",
        "classpath:MarkingFunctionsContext.xml",
        "classpath:MetadataHelperContext.xml",
        "classpath:CacheContext.xml"})
// @formatter:on
public class IntermediateResultsQueryTest extends AbstractQueryTest {

    private static final Logger log = LoggerFactory.getLogger(IntermediateResultsQueryTest.class);

    private static final Authorizations auths = new Authorizations("ALL");
    protected static AccumuloClient client = null;
    protected static TableOperations tops = null;

    protected static final String PASSWORD = "password";
    protected static MiniAccumuloCluster mac;

    @TempDir
    public static Path folder;

    @Autowired
    @Qualifier("EventQuery")
    protected ShardQueryLogic logic;

    @Autowired
    @Qualifier("CountQuery")
    protected CountingShardQueryLogic countingLogic;

    @Override
    public ShardQueryLogic getLogic() {
        return logic;
    }

    @Override
    public Authorizations getAuths() {
        return auths;
    }

    private int completePageCount = 0;
    private int expectedCompletePageCount = 0;

    private int completePageSize = 0;
    private int expectedCompletePageSize = 0;

    private int intermediatePageCount = 0;
    private int expectedMinimumIntermediatePages = 0;

    // slow event aggregation to trigger short circuit timeouts
    private final int iterationDelayMS = 100;

    // default timing values
    private final long maxCallMsDefault = 60 * 60 * 1000;
    private final long pageSizeShortCircuitCheckTimeMsDefault = 30 * 60 * 1000;
    private final long pageShortCircuitTimeoutMsDefault = 58 * 60 * 1000;
    private final int maxLongRunningTimeoutRetriesDefault = 3;

    // override timing values
    private long maxCallMsOverride = 0;
    private long pageSizeShortCircuitCheckTimeMsOverride = 0;
    private long pageShortCircuitTimeoutMsOverride = 0;
    private int maxLongRunningTimeoutRetriesOverride = 0;

    // higher order variables required by the RunningQuery
    private final SubjectIssuerDNPair userDN = SubjectIssuerDNPair.of("userDn", "issuerDn");
    private final DatawaveUser user = new DatawaveUser(userDN, DatawaveUser.UserType.USER, Sets.newHashSet(auths.toString().split(",")), null, null, -1L);
    private final DatawavePrincipal datawavePrincipal = new DatawavePrincipal((Collections.singleton(user)));

    @Override
    protected void extraConfigurations() {

    }

    @Override
    protected void extraAssertions() {
        log.info("expected {} intermediate pages and got {}", expectedMinimumIntermediatePages, intermediatePageCount);
        assertTrue(intermediatePageCount >= expectedMinimumIntermediatePages);

        assertEquals(expectedCompletePageCount, completePageCount, "expected page count: " + expectedCompletePageCount + " but got " + completePageCount);
        assertEquals(expectedCompletePageSize, completePageSize, "expected page size: " + expectedCompletePageSize + " but got " + completePageSize);
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        boolean useMAC = false;
        if (useMAC) {
            MiniAccumuloConfig cfg = new MiniAccumuloConfig(folder.toFile(), PASSWORD);
            cfg.setNumTservers(1);
            mac = new MiniAccumuloCluster(cfg);
            mac.start();
            client = mac.createAccumuloClient("root", new PasswordToken(PASSWORD));
        } else {
            InMemoryInstance instance = new InMemoryInstance(IntermediateResultsQueryTest.class.getName());
            client = new InMemoryAccumuloClient("root", instance);
        }

        ShapesIngest.writeData(client, ShapesIngest.RangeType.SHARD);
        IndexIngestUtil ingestUtil = new IndexIngestUtil();
        Authorizations auths = new Authorizations("ALL");
        ingestUtil.write(client, auths);

        tops = client.tableOperations();
    }

    @BeforeEach
    public void beforeEach() {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        setClientForTest(client);
        givenDate("20240201", "20240209");
        configureLogic(logic);
        configureLogic(countingLogic);
    }

    private void configureLogic(ShardQueryLogic logic) {
        URL hadoopConfig = this.getClass().getResource("/testhadoop.config");
        Preconditions.checkNotNull(hadoopConfig);
        logic.setHdfsSiteConfigURLs(hadoopConfig.toExternalForm());

        IvaratorCacheDirConfig config = new IvaratorCacheDirConfig(folder.toUri().toString());
        logic.setIvaratorCacheDirConfigs(Collections.singletonList(config));
        logic.setMaxFieldIndexRangeSplit(1); // keep things simple

        // every test also exercises hit terms
        givenParameter(QueryParameters.HIT_LIST, "true");
        logic.setHitList(true);

        logic.setCollapseUids(false); // index table will either have uids or not

        logic.setQueryExecutionForPageTimeout(1);
        logic.setMaxEvaluationPipelines(1);
        logic.setMaxPipelineCachedResults(0);
        logic.setIvaratorCacheBufferSize(0);
    }

    @AfterEach
    public void afterEach() {
        super.afterEach();
        resetState();
    }

    @AfterAll
    public static void afterAll() throws Exception {
        if (mac != null) {
            mac.stop();
        }
    }

    private void resetState() {
        // reset override state
        maxCallMsOverride = 0;
        pageSizeShortCircuitCheckTimeMsOverride = 0;
        pageShortCircuitTimeoutMsOverride = 0;
        maxLongRunningTimeoutRetriesOverride = 0;

        // reset state for page count, page size, and intermediate page count

        completePageCount = 0;
        expectedCompletePageCount = 0;

        completePageSize = 0;
        expectedCompletePageSize = 0;

        intermediatePageCount = 0;
        expectedMinimumIntermediatePages = 0;
    }

    /**
     * This test does not require iteration across multiple index tables
     *
     * @return a single index table name
     */
    protected List<String> getIndexTableNames() {
        return List.of(TableName.SHARD_INDEX);
    }

    @Override
    protected void executeQuery(ShardQueryLogic logic) throws Exception {
        try {
            results.clear();

            logic.setQueryExecutionForPageTimeout(1);

            RunningQueryTimingImpl timing = getQueryTiming();
            RunningQuery runningQuery = new RunningQuery(null, client, AccumuloConnectionFactory.Priority.NORMAL, logic, getSettings(), "", datawavePrincipal,
                            timing, null, new QueryMetricFactoryImpl());

            while (true) {
                ResultsPage<?> page = runningQuery.next();
                log.info("status: {} size: {}", page.getStatus(), page.getResults().size());
                switch (page.getStatus()) {
                    case PARTIAL:
                        intermediatePageCount++;
                        break;
                    case COMPLETE:
                        completePageCount++;
                        completePageSize += page.getResults().size();
                        break;
                    case NONE:
                    default:
                        break;
                }
                if (page.getStatus().equals(ResultsPage.Status.NONE)) {
                    break;
                }
            }
        } finally {
            logic.close();
            log.info("query retrieved {} results", results.size());
        }
    }

    /**
     * Create a {@link RunningQueryTimingImpl} using default values, or their overrides if configured
     *
     * @return query timing
     */
    private RunningQueryTimingImpl getQueryTiming() {
        return new RunningQueryTimingImpl(getMaxCallMs(), getPageSizeShortCircuitCheckMs(), getPageShortCircuitTimeoutMs(), getMaxLongRunningTimeoutRetries());
    }

    private long getMaxCallMs() {
        if (maxCallMsOverride > 0) {
            return maxCallMsOverride;
        }
        return maxCallMsDefault;
    }

    private long getPageSizeShortCircuitCheckMs() {
        if (pageSizeShortCircuitCheckTimeMsOverride > 0) {
            return pageSizeShortCircuitCheckTimeMsOverride;
        }
        return pageSizeShortCircuitCheckTimeMsDefault;
    }

    private long getPageShortCircuitTimeoutMs() {
        if (pageShortCircuitTimeoutMsOverride > 0) {
            return pageShortCircuitTimeoutMsOverride;
        }
        return pageShortCircuitTimeoutMsDefault;
    }

    private int getMaxLongRunningTimeoutRetries() {
        if (maxLongRunningTimeoutRetriesOverride > 0) {
            return maxLongRunningTimeoutRetriesOverride;
        }
        return maxLongRunningTimeoutRetriesDefault;
    }

    private void givenMaxCallMs(int maxCallMs) {
        this.maxCallMsOverride = maxCallMs;
    }

    private void givenPageSizeShortCircuitCheckMs(int pageSizeShortCircuitCheckMs) {
        this.pageSizeShortCircuitCheckTimeMsOverride = pageSizeShortCircuitCheckMs;
    }

    private void givenPageShortCircuitTimeoutMs(int pageShortCircuitTimeoutMs) {
        this.pageShortCircuitTimeoutMsOverride = pageShortCircuitTimeoutMs;
    }

    private void givenMaxLongRunningTimeoutRetries(int maxLongRunningTimeoutRetries) {
        this.maxLongRunningTimeoutRetriesOverride = maxLongRunningTimeoutRetries;
    }

    private void expectCompletePageSize(int completePageSize) {
        this.expectedCompletePageSize = completePageSize;
    }

    private void expectMinimumIntermediatePages(int numMinimumIntermediatePages) {
        this.expectedMinimumIntermediatePages = numMinimumIntermediatePages;
    }

    private void expectCompletePageCount(int completePageCount) {
        this.expectedCompletePageCount = completePageCount;
    }

    /**
     * Add a {@link datawave.test.iter.DelayIterator} to the shard table with a default timeout of {@link #iterationDelayMS}
     */
    protected void addDelayIterator() {
        addDelayIterator(iterationDelayMS);
    }

    /**
     * Add a {@link datawave.test.iter.DelayIterator} to the shard table with the provided timeout
     *
     * @param delay
     *            the delay in milliseconds
     */
    protected void addDelayIterator(int delay) {
        Map<String,String> properties = new HashMap<>();
        properties.put("table.iterator.scan.delay", "1,datawave.test.iter.DelayIterator");
        properties.put("table.iterator.scan.delay.opt.delay", String.valueOf(delay));
        MacTestUtil.addPropertiesAndWait(tops, TableName.SHARD, properties);
    }

    /**
     * Remove the {@link datawave.test.iter.DelayIterator} from the shard table
     */
    protected void removeDelayIterator() {
        Set<String> properties = new HashSet<>();
        properties.add("table.iterator.scan.delay");
        properties.add("table.iterator.scan.delay.opt.delay");
        MacTestUtil.removePropertiesAndWait(tops, TableName.SHARD, properties);
    }

    @Test
    public void testLongRunningEventQuery() throws Exception {
        try {
            addDelayIterator();
            givenParameter(QueryParameters.GROUP_FIELDS, "SHAPE,TYPE");
            givenParameter(QueryParameters.GROUP_FIELDS_BATCH_SIZE, "10");
            givenQuery("SHAPE == 'triangle' || SHAPE == 'square' || SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
            expectPlan("SHAPE == 'triangle' || SHAPE == 'square' || SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
            expectCompletePageCount(1);
            expectCompletePageSize(6);
            planAndExecuteQuery();
        } finally {
            removeDelayIterator();
        }
    }

    /**
     * A single timeout retry and a max call time of 1 ms will trigger a query timeout exception
     */
    @Test
    public void testLongRunningEventQuery_maxCallMs() {
        try {
            addDelayIterator();
            givenParameter(QueryParameters.GROUP_FIELDS, "SHAPE,TYPE");
            givenParameter(QueryParameters.GROUP_FIELDS_BATCH_SIZE, "10");
            givenQuery("SHAPE == 'triangle' || SHAPE == 'square' || SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
            givenMaxCallMs(1);
            givenMaxLongRunningTimeoutRetries(1);
            assertThrows(QueryException.class, this::planAndExecuteQuery);
        } finally {
            removeDelayIterator();
        }
    }

    @Test
    public void testLongRunningEventQuery_shortCircuitOnPageSize() throws Exception {
        int originalMaxPageSize = logic.getMaxPageSize();
        try {
            addDelayIterator();
            givenParameter(QueryParameters.GROUP_FIELDS, "SHAPE,TYPE");
            givenParameter(QueryParameters.GROUP_FIELDS_BATCH_SIZE, "10");
            givenQuery("SHAPE == 'triangle' || SHAPE == 'square' || SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
            givenPageSizeShortCircuitCheckMs(1);
            logic.setMaxPageSize(2); // six results, two per page, should be three pages
            expectPlan("SHAPE == 'triangle' || SHAPE == 'square' || SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
            expectCompletePageCount(3);
            expectCompletePageSize(6);
            planAndExecuteQuery();
        } finally {
            logic.setMaxPageSize(originalMaxPageSize);
            removeDelayIterator();
        }
    }

    @Test
    public void testLongRunningEventQuery_shortCircuitOnNextTimeout() throws Exception {
        try {
            addDelayIterator();
            givenParameter(QueryParameters.GROUP_FIELDS, "SHAPE,TYPE");
            givenParameter(QueryParameters.GROUP_FIELDS_BATCH_SIZE, "10");
            givenQuery("SHAPE == 'triangle' || SHAPE == 'square' || SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
            givenPageShortCircuitTimeoutMs(1_000);
            givenMaxLongRunningTimeoutRetries(10); // need more than default of three
            expectPlan("SHAPE == 'triangle' || SHAPE == 'square' || SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
            expectCompletePageCount(1);
            expectCompletePageSize(6);
            planAndExecuteQuery();
        } finally {
            removeDelayIterator();
        }
    }

    @Test
    public void testLongRunningCountQuery() throws Exception {
        givenQuery("SHAPE == 'triangle' || SHAPE == 'square' || SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
        countingLogic.setPageWaitTimeMillis(10_000);
        expectPlan("SHAPE == 'triangle' || SHAPE == 'square' || SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
        expectCompletePageCount(1);
        expectCompletePageSize(1);
        planAndExecuteQuery(countingLogic);
    }

    /**
     * A single timeout retry and a max call time of 1 ms will trigger a query timeout exception
     */
    @Test
    public void testLongRunningCountQuery_maxCallMs() {
        try {
            addDelayIterator();
            givenQuery("SHAPE == 'triangle' || SHAPE == 'square' || SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
            expectPlan("SHAPE == 'triangle' || SHAPE == 'square' || SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
            givenMaxCallMs(1); // absurdly small next timeout
            givenMaxLongRunningTimeoutRetries(1); // only retry once
            countingLogic.setPageWaitTimeMillis(750); // configure wait time
            assertThrows(QueryException.class, () -> planAndExecuteQuery(countingLogic));
            // note: this code path will cause the batch scanner to throw exceptions as the query logic
            // is forcibly torn down
        } finally {
            removeDelayIterator();
        }
    }

    @Test
    public void testLongRunningCountQuery_shortCircuitOnPageSize() throws Exception {
        int originalMaxPageSize = countingLogic.getMaxPageSize();
        try {
            addDelayIterator();
            givenQuery("SHAPE == 'triangle' || SHAPE == 'square' || SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
            givenPageSizeShortCircuitCheckMs(1);
            givenMaxLongRunningTimeoutRetries(10); // need more than default of three
            countingLogic.setMaxPageSize(2); // six results, two per page, should be three pages
            countingLogic.setPageWaitTimeMillis(750); // configure wait time
            expectPlan("SHAPE == 'triangle' || SHAPE == 'square' || SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
            expectCompletePageCount(1);
            expectCompletePageSize(1);
            expectMinimumIntermediatePages(5);
            planAndExecuteQuery(countingLogic);
        } finally {
            countingLogic.setMaxPageSize(originalMaxPageSize);
            removeDelayIterator();
        }
    }

    @Test
    public void testLongRunningCountQuery_shortCircuitOnNextTimeout() throws Exception {
        try {
            addDelayIterator();
            givenQuery("SHAPE == 'triangle' || SHAPE == 'square' || SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
            givenPageShortCircuitTimeoutMs(1_000);
            givenMaxLongRunningTimeoutRetries(10); // need more than default of three
            countingLogic.setPageWaitTimeMillis(750); // configure await time
            expectPlan("SHAPE == 'triangle' || SHAPE == 'square' || SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
            expectCompletePageCount(1);
            expectCompletePageSize(1);
            expectMinimumIntermediatePages(5);
            planAndExecuteQuery(countingLogic);
        } finally {
            removeDelayIterator();
        }
    }
}
