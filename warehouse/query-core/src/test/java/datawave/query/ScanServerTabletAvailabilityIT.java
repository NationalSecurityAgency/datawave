package datawave.query;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.InvalidTabletHostingRequestException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.RowRange;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.ClusterServerConfiguration;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
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

import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.LcType;
import datawave.data.type.NumberType;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.AbstractIngest;
import datawave.query.util.AbstractQueryTest;
import datawave.table.constants.TableName;

/**
 * Verifies that a query can read shard data on {@code ONDEMAND} and {@code UNHOSTED} data tables.
 * <p>
 * Apache Accumulo <a href="https://accumulo.apache.org/blog/2024/10/07/accumulo4-preview.html">4.0</a> introduces the tablet availability feature. Tablets are
 * by default assigned the ONDEMAND availability where tablets are assigned and hosted on-demand, and unhosted after a configured amount of time has passed.
 * Operations requiring immediate consistency require tablets to be hosted, while operations that require eventual consistency can be performed on unhosted
 * tablets.
 * <p>
 * The data tables ({@code shard}, {@code shardIndex}, {@code shardReverseIndex}) are made {@code UNHOSTED} for some tests, and {@code ONDEMAND} for others
 * while {@code DatawaveMetadata} is left {@code ONDEMAND}, since query planning reads metadata through the ordinary tablet server path.
 * <p>
 * Two query logics are exercised against exactly the same data:
 * <ul>
 * <li>{@code EventQuery} - the stock logic, which pins every table to {@code IMMEDIATE} via {@code DefaultConsistencyLevels}.</li>
 * <li>{@code ScanServerEventQuery} - declared in {@code ScanServerQueryLogicFactory.xml}, identical apart from flipping the data tables to {@code EVENTUAL}.
 * </li>
 * </ul>
 * <p>
 * The events are generated pseudo-randomly from a fixed seed and spread over several shards across several days, so the queries below span multiple tablets of
 * rather than a single one.
 */
@ExtendWith(SpringExtension.class)
@ComponentScan(basePackages = "datawave.query")
// @formatter:off
@ContextConfiguration(locations = {
                "classpath:datawave/query/ScanServerQueryLogicFactory.xml",
                "classpath:beanRefContext.xml",
                "classpath:MarkingFunctionsContext.xml",
                "classpath:MetadataHelperContext.xml",
                "classpath:CacheContext.xml"})
// @formatter:on
public class ScanServerTabletAvailabilityIT extends AbstractQueryTest {

    private static final Logger log = LoggerFactory.getLogger(ScanServerTabletAvailabilityIT.class);

    private static final String PASSWORD = "password";
    private static final Authorizations auths = new Authorizations("ALL");

    /** Fixed so the generated event set, and therefore every expected result below, is reproducible. */
    private static final long SEED = 20260806L;
    private static final int EVENT_COUNT = 60;

    private static final List<String> DATES = List.of("20260701", "20260702", "20260703");
    private static final int SHARDS_PER_DAY = 3;
    private static final List<String> COLORS = List.of("red", "blue", "green", "yellow");
    private static final int MIN_SIZE = 1;
    private static final int MAX_SIZE = 9;

    /**
     * CODE values share a prefix ({@code alpha0..alpha4}, {@code beta0..beta4}) so that a regex expands to several index values rather than one. The prefixes
     * are deliberately longer than two characters, because {@code RegexPushdownTransformRule} forces any regex of two-or-fewer leading characters to
     * evaluation-only, which would make the term non-executable instead of driving an index lookup.
     */
    private static final List<String> CODE_GROUPS = List.of("alpha", "beta");
    private static final int CODES_PER_GROUP = 5;

    /** The data tables that will be made unhosted. The metadata table is deliberately absent. */
    private static final List<String> DATA_TABLES = List.of(TableName.SHARD, TableName.SHARD_INDEX, TableName.SHARD_RINDEX);

    @TempDir
    public static Path folder;

    private static MiniAccumuloClusterImpl mac;
    private static AccumuloClient client;

    private static final List<TestEvent> events = new ArrayList<>();

    @Autowired
    @Qualifier("ScanServerEventQuery")
    protected ShardQueryLogic eventualLogic;

    @Autowired
    @Qualifier("EventQuery")
    protected ShardQueryLogic immediateLogic;

    @Override
    public ShardQueryLogic getLogic() {
        return eventualLogic;
    }

    @Override
    public Authorizations getAuths() {
        return auths;
    }

    @Override
    protected void extraConfigurations() {
        // no-op
    }

    @Override
    protected void extraAssertions() {
        // no-op
    }

    /**
     * This test is about scan routing, not about index table variants, so only the standard shard index is exercised.
     *
     * @return the single index table name
     */
    @Override
    protected List<String> getIndexTableNames() {
        return List.of(TableName.SHARD_INDEX);
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        MiniAccumuloConfigImpl cfg = new MiniAccumuloConfigImpl(folder.toFile(), PASSWORD);

        ClusterServerConfiguration serverCfg = cfg.getClusterServerConfiguration();
        serverCfg.setNumDefaultTabletServers(1);
        serverCfg.setNumDefaultScanServers(1);

        mac = new MiniAccumuloClusterImpl(cfg);
        mac.start();

        // MiniAccumuloCluster#start does not launch scan servers even when numScanServers is set, so start them here
        mac.getClusterControl().start(ServerType.SCAN_SERVER, "localhost");

        client = mac.createAccumuloClient("root", new PasswordToken(PASSWORD));

        awaitScanServers();
        writeData();
    }

    @AfterAll
    public static void afterAll() throws Exception {
        if (mac != null) {
            mac.stop();
        }
    }

    @BeforeEach
    public void beforeEach() {
        setClientForTest(client);
        configure(eventualLogic);
        configure(immediateLogic);
    }

    /**
     * Apply the settings both logics need, so the only difference between them remains the consistency level.
     * <p>
     * The cardinality threshold is disabled because it converts a regex into a filter ivarator instead of expanding it against the index, which would defeat
     * {@link UnhostedOnlyAvailabilityTests#testRegexIndexExpansion()}. The ivarator cache directory and hadoop config are supplied so that any test which does
     * fall back to an ivarator has somewhere to spill to.
     *
     * @param queryLogic
     *            the logic to configure
     */
    private void configure(ShardQueryLogic queryLogic) {
        URL hadoopConfig = getClass().getResource("/testhadoop.config");
        Preconditions.checkNotNull(hadoopConfig);
        queryLogic.setHdfsSiteConfigURLs(hadoopConfig.toExternalForm());
        queryLogic.setIvaratorCacheDirConfigs(List.of(new IvaratorCacheDirConfig(folder.toUri().toString())));

        queryLogic.setCardinalityThreshold(0);
    }

    /**
     * A scan server registers itself in ZooKeeper asynchronously after the process starts. Wait for at least one to appear.
     */
    private static void awaitScanServers() throws Exception {
        long deadline = System.currentTimeMillis() + 60_000L;
        int registered = 0;
        while (System.currentTimeMillis() < deadline) {
            registered = client.instanceOperations().getServers(ServerId.Type.SCAN_SERVER).size();
            if (registered > 0) {
                break;
            }
            Thread.sleep(250L);
        }
        assertTrue(registered > 0, "no scan server registered");
        log.info("scan servers registered: {}", registered);
    }

    /**
     * Generate the pseudo-random event set and write it, then split the data tables so that the scans below have to cross tablet boundaries.
     */
    private static void writeData() throws Exception {
        AbstractIngest ingest = new AbstractIngest(client, auths);

        ingest.registerField("UUID", new LcNoDiacriticsType());
        ingest.registerColumns("UUID", List.of("i", "e"));

        ingest.registerField("COLOR", new LcType());
        ingest.registerColumns("COLOR", List.of("i", "e"));

        ingest.registerField("SIZE", new NumberType());
        ingest.registerColumns("SIZE", List.of("i", "e"));

        ingest.registerField("CODE", new LcType());
        ingest.registerColumns("CODE", List.of("i", "e"));

        Random random = new Random(SEED);
        for (int id = 0; id < EVENT_COUNT; id++) {
            String date = DATES.get(random.nextInt(DATES.size()));
            String row = date + "_" + random.nextInt(SHARDS_PER_DAY);
            String uuid = String.format("uuid-%03d", id);
            String color = COLORS.get(random.nextInt(COLORS.size()));
            int size = MIN_SIZE + random.nextInt(MAX_SIZE - MIN_SIZE + 1);
            String code = CODE_GROUPS.get(random.nextInt(CODE_GROUPS.size())) + random.nextInt(CODES_PER_GROUP);

            events.add(new TestEvent(row, uuid, color, size, code));

            ingest.writeFV(row, ingest.getDatatype(), id, "UUID", uuid);
            ingest.writeFV(row, ingest.getDatatype(), id, "COLOR", color);
            ingest.writeFV(row, ingest.getDatatype(), id, "SIZE", String.valueOf(size));
            ingest.writeFV(row, ingest.getDatatype(), id, "CODE", code);
        }

        TableOperations tops = client.tableOperations();

        SortedSet<Text> shardSplits = new TreeSet<>();
        for (String date : DATES) {
            shardSplits.add(new Text(date + "_1"));
        }
        tops.addSplits(TableName.SHARD, shardSplits);

        SortedSet<Text> indexSplits = new TreeSet<>();
        indexSplits.add(new Text("g"));
        indexSplits.add(new Text("r"));
        tops.addSplits(TableName.SHARD_INDEX, indexSplits);

        log.info("wrote {} events across {} shards", events.size(), events.stream().map(e -> e.row).distinct().count());
    }

    // ------------------------------------------------------------------
    // expected-result helpers, derived from the generated event set
    // ------------------------------------------------------------------

    private Set<String> uuidsMatching(Predicate<TestEvent> predicate) {
        // @formatter:off
        return events.stream()
                        .filter(predicate)
                        .map(event -> event.uuid)
                        .collect(Collectors.toCollection(LinkedHashSet::new));
        // @formatter:on
    }

    private void expect(Set<String> uuids) {
        assertFalse(uuids.isEmpty(), "test data did not produce any matching events, the query would be vacuous");
        expectResultCount(uuids.size());
        expectUUIDs(uuids);
    }

    private void givenFullDateRange() {
        givenDate(DATES.get(0), DATES.get(DATES.size() - 1));
    }

    /**
     * Sanity check on the generated data, so a change to the seed or the generator that quietly collapses the event set is caught here rather than surfacing as
     * a confusing failure in one of the query tests.
     */
    @DisplayName("Generated valid test data")
    @Test
    public void testGeneratedDataSpansMultipleShards() {
        assertEquals(EVENT_COUNT, events.size());

        Set<String> rows = events.stream().map(event -> event.row).collect(Collectors.toCollection(TreeSet::new));
        assertTrue(rows.size() > 1, "events must span more than one shard, found: " + rows);

        Set<String> dates = rows.stream().map(row -> row.substring(0, row.indexOf('_'))).collect(Collectors.toCollection(TreeSet::new));
        assertEquals(new TreeSet<>(DATES), dates, "events must span every date");

        Set<String> colors = events.stream().map(event -> event.color).collect(Collectors.toCollection(TreeSet::new));
        assertEquals(new TreeSet<>(COLORS), colors, "events must cover every color");
    }

    @DisplayName("Given all UNHOSTED data tablets")
    @Nested
    class UnhostedOnlyAvailabilityTests {

        @BeforeAll
        static void beforeAll() throws Exception {
            TableOperations tops = client.tableOperations();
            for (String table : DATA_TABLES) {
                tops.flush(table, null, null, true);
                tops.setTabletAvailability(table, RowRange.all(), TabletAvailability.UNHOSTED);
                log.info("Made all tablets for table {} unhosted", table);
            }
        }

        /**
         * The narrowest case, a single event fetched by uuid from a single unhosted shard.
         */
        @DisplayName("A Scan with eventual consistency finds single event fetched by UUID from a single shard")
        @Test
        void testSingleEventByUUID() throws Exception {
            String uuid = events.get(0).uuid;

            givenFullDateRange();
            givenQuery("UUID == '" + uuid + "'");
            expectPlan("UUID == '" + uuid + "'");
            expect(Set.of(uuid));
            planAndExecuteQuery(eventualLogic);
        }

        /**
         * A single term that matches events spread across every unhosted shard.
         */
        @DisplayName("A Scan with eventual consistency finds single term that matches events spread across every shard")
        @Test
        public void testEqualityAcrossShards() throws Exception {
            Set<String> expected = uuidsMatching(event -> event.color.equals("red"));

            givenFullDateRange();
            givenQuery("COLOR == 'red'");
            expectPlan("COLOR == 'red'");
            expect(expected);
            planAndExecuteQuery(eventualLogic);
        }

        /**
         * An intersection, which reads two field index ranges out of the shard table.
         */
        @DisplayName("A Scan with eventual consistency finds intersection")
        @Test
        public void testIntersection() throws Exception {
            Set<String> expected = uuidsMatching(event -> event.color.equals("red") && event.size == 5);

            givenFullDateRange();
            givenQuery("COLOR == 'red' && SIZE == '5'");
            expectPlan("COLOR == 'red' && SIZE == '+aE5'");
            expect(expected);
            planAndExecuteQuery(eventualLogic);
        }

        /**
         * A union of two terms.
         */
        @DisplayName("A Scan with eventual consistency finds union")
        @Test
        public void testUnion() throws Exception {
            Set<String> expected = uuidsMatching(event -> event.color.equals("red") || event.color.equals("blue"));

            givenFullDateRange();
            givenQuery("COLOR == 'red' || COLOR == 'blue'");
            expectPlan("COLOR == 'red' || COLOR == 'blue'");
            expect(expected);
            planAndExecuteQuery(eventualLogic);
        }

        /**
         * A negation, which requires the event to be evaluated after the index hit.
         */
        @DisplayName("A Scan with eventual consistency finds negation")
        @Test
        public void testIntersectionWithNegation() throws Exception {
            Set<String> expected = uuidsMatching(event -> event.color.equals("red") && event.size != 5);

            givenFullDateRange();
            givenQuery("COLOR == 'red' && !(SIZE == '5')");
            expectPlan("COLOR == 'red' && !(SIZE == '+aE5')");
            expect(expected);
            planAndExecuteQuery(eventualLogic);
        }

        /**
         * A regex, which drives an index expansion scan against the unhosted shard index before the shard table is read. The expansion resolves to the several
         * CODE values that share the queried prefix.
         */
        @DisplayName("A Scan with eventual consistency finds regex")
        @Test
        void testRegexIndexExpansion() throws Exception {
            Set<String> expected = uuidsMatching(event -> event.code.startsWith("alpha"));

            givenFullDateRange();
            givenQuery("CODE =~ 'alpha.*'");
            // the planner rewrites this into the union of the matching index values
            disableQueryPlanAssertion();
            expect(expected);
            planAndExecuteQuery(eventualLogic);
        }

        /**
         * A bounded numeric range, which is expanded from the unhosted shard index. The {@code _Bounded_} marker is required, otherwise the planner rejects the
         * pair of inequalities as an incorrectly marked bounded range.
         */
        @DisplayName("A Scan with eventual consistency finds bounded range")
        @Test
        public void testBoundedRange() throws Exception {
            Set<String> expected = uuidsMatching(event -> event.size >= 3 && event.size <= 5);

            givenFullDateRange();
            givenQuery("((_Bounded_ = true) && (SIZE >= '3' && SIZE <= '5'))");
            // the bounded range is rewritten into the expanded set of index values
            disableQueryPlanAssertion();
            expect(expected);
            planAndExecuteQuery(eventualLogic);
        }

        /**
         * Restricting the query to one day proves the shard ranges are still honored when the table is unhosted.
         */
        @DisplayName("A Scan with eventual consistency finds single day range")
        @Test
        public void testSingleDayRange() throws Exception {
            String date = DATES.get(1);
            Set<String> expected = uuidsMatching(event -> event.row.startsWith(date) && event.color.equals("red"));

            givenDate(date);
            givenQuery("COLOR == 'red'");
            expectPlan("COLOR == 'red'");
            expect(expected);
            planAndExecuteQuery(eventualLogic);
        }

        /**
         * The reason {@code ScanServerQueryLogicFactory.xml} exists.
         * <p>
         * The stock {@code EventQuery} logic leaves every table at {@code IMMEDIATE}, so its scans are routed to a tablet server that has unloaded the unhosted
         * tablets. Without a consistency level to flip, there is no configuration on the stock logic that would make this query succeed.
         */
        @DisplayName("A scan with immediate consistency throws an exception")
        @Test
        public void testImmediateConsistencyFailsOnFullyUnhostedTabletIndex() {
            givenFullDateRange();
            givenQuery("COLOR == 'red' && SIZE == '5'");
            disableQueryPlanAssertion();

            Exception thrown = assertThrows(Exception.class, () -> planAndExecuteQuery(immediateLogic),
                            "the stock IMMEDIATE logic must not be able to read the UNHOSTED tablets in data tables");

            assertTrue(hasTabletUnhostedAvailabilityCause(thrown), "expected an tablet unhosted failure but got: " + describe(thrown));
        }

        /**
         * Walk the cause chain looking for evidence that the failure was caused by the tablets being unhosted.
         *
         * @param throwable
         *            the thrown exception
         * @return true if the failure is attributable to an unhosted tablet
         */
        private boolean hasTabletUnhostedAvailabilityCause(Throwable throwable) {
            for (Throwable t = throwable; t != null; t = t.getCause()) {
                if (t instanceof InvalidTabletHostingRequestException) {
                    return true;
                }
                String message = t.getMessage();
                if (message != null && message.toLowerCase().contains("has a tablet availability unhosted")) {
                    return true;
                }
                if (t.getCause() == t) {
                    break;
                }
            }
            return false;
        }

        private String describe(Throwable throwable) {
            StringBuilder sb = new StringBuilder();
            for (Throwable t = throwable; t != null; t = t.getCause()) {
                sb.append(t.getClass().getName()).append(": ").append(t.getMessage()).append(" | ");
                if (t.getCause() == t) {
                    break;
                }
            }
            return sb.toString();
        }

        /**
         * The companion to {@link #testImmediateConsistencyFailsOnFullyUnhostedTabletIndex()}, and the more dangerous half of it.
         * <p>
         * A single-term query does not surface the unhosted tablet as an error at all. The index lookup for the term comes back empty, so the query simply
         * yields no ranges and reports success with zero results, which is indistinguishable from a query that legitimately matched nothing.
         */
        @DisplayName("A scan with immediate consistency reports success with zero results for a single-term query")
        @Test
        public void testImmediateConsistencyReturnsNoResultsForSingleTerm() throws Exception {
            givenFullDateRange();
            givenQuery("COLOR == 'red'");
            expectPlan("COLOR == 'red'");
            expectResultCount(0);

            planAndExecuteQuery(immediateLogic);

            assertTrue(results.isEmpty(), "the stock IMMEDIATE logic must not return results from a table of all UNHOSTED tablets");
        }
    }

    @DisplayName("Given eventual consistency logic with ONDEMAND data tablets")
    @Nested
    class OnDemandEventualLogicTests extends OnDemandTests {

        @Override
        ShardQueryLogic getLogic() {
            return eventualLogic;
        }
    }

    @DisplayName("Given immediate consistency logic with ONDEMAND data tablets")
    @Nested
    class OnDemandImmediateLogicTests extends OnDemandTests {

        @Override
        ShardQueryLogic getLogic() {
            return immediateLogic;
        }
    }

    abstract class OnDemandTests {

        abstract ShardQueryLogic getLogic();

        @BeforeAll
        static void beforeAll() throws Exception {
            TableOperations tops = client.tableOperations();
            for (String table : DATA_TABLES) {
                tops.flush(table, null, null, true);
            }
        }

        @AfterEach
        void tearDown() throws AccumuloException, TableNotFoundException, AccumuloSecurityException {
            TableOperations tops = client.tableOperations();
            for (String table : DATA_TABLES) {
                // Set the availability of all tablets in the table to UNHOSTED to force the tablets to be unloaded.
                tops.setTabletAvailability(table, RowRange.all(), TabletAvailability.UNHOSTED);
                TableId tableId = TableId.of(tops.tableIdMap().get(table));

                // Wait for all tablets to be unloaded.
                await().atMost(30, TimeUnit.SECONDS).pollDelay(250, TimeUnit.MILLISECONDS).until(() -> countTabletsWithLocation(client, tableId) == 0);

                // Reset the availability fo all tablets back to ONDEMAND.
                tops.setTabletAvailability(table, RowRange.all(), TabletAvailability.ONDEMAND);
            }
        }

        private long countTabletsWithLocation(AccumuloClient client, TableId tableId) {
            try (TabletsMetadata tabletsMetadata = ((ClientContext) client).getAmple().readTablets().forTable(tableId).fetch(TabletMetadata.ColumnType.LOCATION)
                            .build()) {
                return tabletsMetadata.stream().filter(tabletMetadata -> tabletMetadata.getLocation() != null).count();
            }
        }

        /**
         * The narrowest case, a single event fetched by uuid from a single unhosted shard.
         */
        @DisplayName("Scan finds single event fetched by UUID from a single shard")
        @Test
        void testSingleEventByUUID() throws Exception {
            String uuid = events.get(0).uuid;

            givenFullDateRange();
            givenQuery("UUID == '" + uuid + "'");
            expectPlan("UUID == '" + uuid + "'");
            expect(Set.of(uuid));
            planAndExecuteQuery(getLogic());
        }

        /**
         * A single term that matches events spread across every unhosted shard.
         */
        @DisplayName("Scan finds single term that matches events spread across every shard")
        @Test
        public void testEqualityAcrossShards() throws Exception {
            Set<String> expected = uuidsMatching(event -> event.color.equals("red"));

            givenFullDateRange();
            givenQuery("COLOR == 'red'");
            expectPlan("COLOR == 'red'");
            expect(expected);
            planAndExecuteQuery(getLogic());
        }

        /**
         * An intersection, which reads two field index ranges out of the shard table.
         */
        @DisplayName("Scan finds intersection")
        @Test
        public void testIntersection() throws Exception {
            Set<String> expected = uuidsMatching(event -> event.color.equals("red") && event.size == 5);

            givenFullDateRange();
            givenQuery("COLOR == 'red' && SIZE == '5'");
            expectPlan("COLOR == 'red' && SIZE == '+aE5'");
            expect(expected);
            planAndExecuteQuery(getLogic());
        }

        /**
         * A union of two terms.
         */
        @DisplayName("Scan finds union")
        @Test
        public void testUnion() throws Exception {
            Set<String> expected = uuidsMatching(event -> event.color.equals("red") || event.color.equals("blue"));

            givenFullDateRange();
            givenQuery("COLOR == 'red' || COLOR == 'blue'");
            expectPlan("COLOR == 'red' || COLOR == 'blue'");
            expect(expected);
            planAndExecuteQuery(getLogic());
        }

        /**
         * A negation, which requires the event to be evaluated after the index hit.
         */
        @DisplayName("Scan finds negation")
        @Test
        public void testIntersectionWithNegation() throws Exception {
            Set<String> expected = uuidsMatching(event -> event.color.equals("red") && event.size != 5);

            givenFullDateRange();
            givenQuery("COLOR == 'red' && !(SIZE == '5')");
            expectPlan("COLOR == 'red' && !(SIZE == '+aE5')");
            expect(expected);
            planAndExecuteQuery(getLogic());
        }

        /**
         * A regex, which drives an index expansion scan against the unhosted shard index before the shard table is read. The expansion resolves to the several
         * CODE values that share the queried prefix.
         */
        @DisplayName("Scan finds regex")
        @Test
        void testRegexIndexExpansion() throws Exception {
            Set<String> expected = uuidsMatching(event -> event.code.startsWith("alpha"));

            givenFullDateRange();
            givenQuery("CODE =~ 'alpha.*'");
            // the planner rewrites this into the union of the matching index values
            disableQueryPlanAssertion();
            expect(expected);
            planAndExecuteQuery(getLogic());
        }

        /**
         * A bounded numeric range, which is expanded from the unhosted shard index. The {@code _Bounded_} marker is required, otherwise the planner rejects the
         * pair of inequalities as an incorrectly marked bounded range.
         */
        @DisplayName("Scan finds bounded range")
        @Test
        public void testBoundedRange() throws Exception {
            Set<String> expected = uuidsMatching(event -> event.size >= 3 && event.size <= 5);

            givenFullDateRange();
            givenQuery("((_Bounded_ = true) && (SIZE >= '3' && SIZE <= '5'))");
            // the bounded range is rewritten into the expanded set of index values
            disableQueryPlanAssertion();
            expect(expected);
            planAndExecuteQuery(getLogic());
        }

        /**
         * Restricting the query to one day proves the shard ranges are still honored when the table is unhosted.
         */
        @DisplayName("Scan finds single day range")
        @Test
        public void testSingleDayRange() throws Exception {
            String date = DATES.get(1);
            Set<String> expected = uuidsMatching(event -> event.row.startsWith(date) && event.color.equals("red"));

            givenDate(date);
            givenQuery("COLOR == 'red'");
            expectPlan("COLOR == 'red'");
            expect(expected);
            planAndExecuteQuery(getLogic());
        }
    }

    /**
     * A single generated event.
     */
    private static final class TestEvent {
        private final String row;
        private final String uuid;
        private final String color;
        private final int size;
        private final String code;

        private TestEvent(String row, String uuid, String color, int size, String code) {
            this.row = row;
            this.uuid = uuid;
            this.color = color;
            this.size = size;
            this.code = code;
        }
    }
}
