package datawave.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.google.common.collect.Sets;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.cache.ResultsPage;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.result.event.DefaultResponseObjectFactory;
import datawave.helpers.PrintUtility;
import datawave.marking.MarkingFunctions;
import datawave.microservice.query.QueryImpl;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.transformer.DocumentTransform;
import datawave.query.transformer.DocumentTransformer;
import datawave.query.transformer.GroupingTransform;
import datawave.query.transformer.UniqueTransform;
import datawave.query.util.DateIndexHelperFactory;
import datawave.query.util.MetadataHelperFactory;
import datawave.query.util.VisibilityWiseGuysIngest;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.table.constants.TableName;
import datawave.webservice.query.runner.RunningQuery;

/** Tests completion and pagination through actual transform-generated intermediate results. */
@Timeout(30)
public class LongRunningQueryTest {

    private static final long PAGE_TIMEOUT_MS = 1000L;
    private SubjectIssuerDNPair userDN = SubjectIssuerDNPair.of("userDn", "issuerDn");
    private static Authorizations auths = new Authorizations("ALL", "E", "I");
    private final DateFormat format = new SimpleDateFormat("yyyyMMdd");
    private static MockAccumuloRecordWriter recordWriter;
    private DatawavePrincipal datawavePrincipal;
    private static final Logger log = Logger.getLogger(LongRunningQueryTest.class);
    private static AccumuloClient client = null;
    private ShardQueryLogic logic;
    private ExecutorService executor;
    private AdvancingClock pageClock;

    @BeforeEach
    public void setup() throws Exception {
        DatawaveUser user = new DatawaveUser(userDN, DatawaveUser.UserType.USER, Sets.newHashSet(auths.toString().split(",")), null, null, -1L);
        datawavePrincipal = new DatawavePrincipal((Collections.singleton(user)));

        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        QueryTestTableHelper testTableHelper = new QueryTestTableHelper(LongRunningQueryTest.class.toString(), log);
        recordWriter = new MockAccumuloRecordWriter();
        testTableHelper.configureTables(recordWriter);
        client = testTableHelper.client;

        VisibilityWiseGuysIngest.writeItAll(client);
        PrintUtility.printTable(client, auths, TableName.SHARD);
        PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);

        logic = new ShardQueryLogic() {
            @Override
            public void setPageProcessingStartTime(long pageProcessingStartTime) {
                if (pageClock != null) {
                    pageClock.startPage(pageProcessingStartTime);
                }
                super.setPageProcessingStartTime(pageProcessingStartTime);
            }
        };
        logic.setIncludeGroupingContext(true);
        logic.setIncludeDataTypeAsField(true);
        logic.setMarkingFunctions(new MarkingFunctions.Default());
        logic.setMetadataHelperFactory(new MetadataHelperFactory());
        logic.setDateIndexHelperFactory(new DateIndexHelperFactory());
        logic.setResponseObjectFactory(new DefaultResponseObjectFactory());
        logic.setMaxEvaluationPipelines(1);
        logic.setFullTableScanEnabled(true);
        logic.setCacheModel(false);
        logic.setMaxPipelineCachedResults(0);
        logic.setIvaratorCacheBufferSize(0);
        logic.setQueryExecutionForPageTimeout(PAGE_TIMEOUT_MS);

        executor = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    public void after() {
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }

    /** Grouping emits an empty intermediate page, then all eight groups, then completes. */
    @Test
    public void testLongRunningGroupByQuery() throws Exception {
        RunningQuery runningQuery = createRunningQuery("UUID =~ '^[CS].*'", Integer.MAX_VALUE, groupParameters());
        installClockThatAdvancesPastTimeout(runningQuery, GroupingTransform.class);

        List<ResultsPage> pages = collectPages(runningQuery);
        assertPages(pages, List.of(ResultsPage.Status.PARTIAL, ResultsPage.Status.COMPLETE, ResultsPage.Status.NONE), List.of(0, 8, 0));
    }

    /** A duplicate triggers an intermediate result after the first unique event; the query then completes. */
    @Test
    public void testLongRunningUniqueQuery() throws Exception {
        // Keep duplicates in the iterator output so the web-server uniqueness transform can exercise its intermediate-result path.
        logic.setDisableIteratorUniqueFields(true);
        RunningQuery runningQuery = createRunningQuery("UUID =~ '^[CS].*'", Integer.MAX_VALUE, Map.of("unique.fields", "GROUP"));
        installClockThatAdvancesPastTimeout(runningQuery, UniqueTransform.class);

        List<ResultsPage> pages = collectPages(runningQuery);
        assertPages(pages, List.of(ResultsPage.Status.PARTIAL, ResultsPage.Status.NONE), List.of(1, 0));
    }

    /** An empty query completes immediately without an intermediate page. */
    @Test
    public void testLongRunningQueryWithNoResults() throws Exception {
        RunningQuery runningQuery = createRunningQuery("UUID =~ '^[NAN].*' AND UUID =~ '^[BAN].*' AND UUID =~ '^[MAN].*' AND UUID =~ '^[PAN].*'",
                        Integer.MAX_VALUE, groupParameters());
        installClockThatAdvancesPastTimeout(runningQuery, GroupingTransform.class);

        List<ResultsPage> pages = collectPages(runningQuery);
        assertPages(pages, List.of(ResultsPage.Status.NONE), List.of(0));
    }

    /** An intermediate page preserves all eight groups, which are returned in two four-result pages. */
    @Test
    public void testLongRunningQueryWithSmallPageSize() throws Exception {
        RunningQuery runningQuery = createRunningQuery("UUID =~ '^[CS].*'", 4, groupParameters());
        installClockThatAdvancesPastTimeout(runningQuery, GroupingTransform.class);

        List<ResultsPage> pages = collectPages(runningQuery);
        assertPages(pages, List.of(ResultsPage.Status.PARTIAL, ResultsPage.Status.COMPLETE, ResultsPage.Status.COMPLETE, ResultsPage.Status.NONE),
                        List.of(0, 4, 4, 0));
    }

    private static Map<String,String> groupParameters() {
        return Map.of("group.fields", "AGE,$GENDER", "group.fields.batch.size", "6");
    }

    private RunningQuery createRunningQuery(String queryString, int pageSize, Map<String,String> parameters) throws Exception {
        QueryImpl query = new QueryImpl();
        query.setQuery(queryString);
        query.setBeginDate(format.parse("20091231"));
        query.setEndDate(format.parse("20150101"));
        query.setQueryAuthorizations(auths.serialize());
        query.setColumnVisibility("A&E&I");
        query.setPagesize(pageSize);
        query.setParameters(parameters);
        query.setId(UUID.randomUUID());

        GenericQueryConfiguration config = logic.initialize(client, query, Collections.singleton(auths));
        logic.setupQuery(config);

        // No request-thread short-circuit timeout: PARTIAL pages must come from the transform's intermediate results.
        RunningQuery runningQuery = new RunningQuery(null, client, AccumuloConnectionFactory.Priority.NORMAL, logic, query, "", datawavePrincipal, null, null,
                        new QueryMetricFactoryImpl());
        runningQuery.setExecutor(executor);
        return runningQuery;
    }

    private void installClockThatAdvancesPastTimeout(RunningQuery runningQuery, Class<? extends DocumentTransform> transformType) {
        DocumentTransformer transformer = (DocumentTransformer) runningQuery.getTransformIterator().getTransformer();
        DocumentTransform transform = transformer.containsTransform(transformType);
        assertNotNull(transform);
        pageClock = new AdvancingClock(ZoneOffset.UTC);
        ((DocumentTransform.DefaultDocumentTransform) transform).setClock(pageClock);
    }

    /**
     * Seeds the first reading at the actual page start, then advances past that page's deadline. Later pages start with an expired clock and cannot record
     * activity, so only the first page can emit an intermediate result.
     */
    private static class AdvancingClock extends Clock {
        private final ZoneId zone;
        private final AtomicLong currentTimeMillis;
        private final AtomicBoolean advanced;

        private AdvancingClock(ZoneId zone) {
            this(zone, new AtomicLong(), new AtomicBoolean());
        }

        private AdvancingClock(ZoneId zone, AtomicLong currentTimeMillis, AtomicBoolean advanced) {
            this.zone = zone;
            this.currentTimeMillis = currentTimeMillis;
            this.advanced = advanced;
        }

        private void startPage(long pageStartTime) {
            currentTimeMillis.set(pageStartTime + (advanced.get() ? PAGE_TIMEOUT_MS + 1 : 0));
        }

        @Override
        public long millis() {
            long now = currentTimeMillis.get();
            if (advanced.compareAndSet(false, true)) {
                currentTimeMillis.addAndGet(PAGE_TIMEOUT_MS + 1);
            }
            return now;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(currentTimeMillis.get());
        }

        @Override
        public ZoneId getZone() {
            return zone;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return new AdvancingClock(zone, currentTimeMillis, advanced);
        }
    }

    private static List<ResultsPage> collectPages(RunningQuery runningQuery) throws Exception {
        List<ResultsPage> pages = new ArrayList<>();
        try {
            for (int i = 0; i < 20; i++) {
                ResultsPage page = runningQuery.next();
                pages.add(page);
                if (page.getStatus() == ResultsPage.Status.NONE) {
                    return pages;
                }
            }
            return fail("Query did not complete within 20 pages");
        } finally {
            runningQuery.cancel();
        }
    }

    private static void assertPages(List<ResultsPage> pages, List<ResultsPage.Status> statuses, List<Integer> sizes) {
        assertEquals(statuses, pages.stream().map(ResultsPage::getStatus).collect(Collectors.toList()));
        assertEquals(sizes, pages.stream().map(page -> page.getResults().size()).collect(Collectors.toList()));
    }
}
