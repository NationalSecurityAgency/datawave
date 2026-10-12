package datawave.query;

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
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
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

import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.LcType;
import datawave.data.type.NumberType;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.AbstractIngest;
import datawave.query.util.AbstractQueryTest;
import datawave.table.constants.TableName;

/**
 * Verifies that a query can read shard data after the data tables have been taken offline, by routing scans through a scan server.
 * <p>
 * Apache Accumulo 2.1.5 (<a href="https://github.com/apache/accumulo/pull/6156">PR 6156</a>) allows a client to scan an offline table, but only when the scan
 * carries the {@code EVENTUAL} consistency level so that it is served by a scan server. A scan issued with {@code IMMEDIATE} is routed to a tablet server,
 * which has unloaded the offline table, and fails with a {@link TableOfflineException}.
 * <p>
 * The data tables ({@code shard}, {@code shardIndex}, {@code shardReverseIndex}) are taken offline while {@code DatawaveMetadata} is left online, since query
 * planning reads metadata through the ordinary tablet server path.
 * <p>
 * Two query logics are exercised against exactly the same offline data:
 * <ul>
 * <li>{@code EventQuery} - the stock logic, which pins every table to {@code IMMEDIATE} via {@code DefaultConsistencyLevels}. It cannot read the offline
 * tables, which is asserted by {@link #testImmediateConsistencyCannotReadOfflineTables()}.</li>
 * <li>{@code ScanServerEventQuery} - declared in {@code ScanServerQueryLogicFactory.xml}, identical apart from flipping the data tables to {@code EVENTUAL}.
 * Every other test uses this logic and expects complete results.</li>
 * </ul>
 * The events are generated pseudo-randomly from a fixed seed and spread over several shards across several days, so the queries below span multiple tablets of
 * an offline table rather than a single one.
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
public class ScanServerOfflineTableIT extends AbstractQueryTest {

    private static final Logger log = LoggerFactory.getLogger(ScanServerOfflineTableIT.class);

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

    /** The data tables that get taken offline. The metadata table is deliberately absent. */
    private static final List<String> OFFLINE_TABLES = List.of(TableName.SHARD, TableName.SHARD_INDEX, TableName.SHARD_RINDEX);

    @TempDir
    public static Path folder;

    private static MiniAccumuloClusterImpl mac;
    private static AccumuloClient client;
    private static AbstractIngest ingest;

    private static final List<TestEvent> events = new ArrayList<>();

    @Autowired
    @Qualifier("ScanServerEventQuery")
    protected ShardQueryLogic logic;

    @Autowired
    @Qualifier("EventQuery")
    protected ShardQueryLogic immediateLogic;

    @Override
    public ShardQueryLogic getLogic() {
        return logic;
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
        cfg.setNumTservers(1);
        cfg.setNumScanServers(1);

        mac = new MiniAccumuloClusterImpl(cfg);
        mac.start();

        // MiniAccumuloCluster#start does not launch scan servers even when numScanServers is set, so start them here
        mac.getClusterControl().start(ServerType.SCAN_SERVER, "localhost");

        client = mac.createAccumuloClient("root", new PasswordToken(PASSWORD));

        awaitScanServers();
        writeData();
        takeDataTablesOffline();
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
        configure(logic);
        configure(immediateLogic);
    }

    /**
     * Apply the settings both logics need, so the only difference between them remains the consistency level.
     * <p>
     * The cardinality threshold is disabled because it converts a regex into a filter ivarator instead of expanding it against the index, which would defeat
     * {@link #testRegexIndexExpansion()}. The ivarator cache directory and hadoop config are supplied so that any test which does fall back to an ivarator has
     * somewhere to spill to.
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
     * A scan server registers itself in ZooKeeper asynchronously after the process starts. Scanning an offline table before that happens fails, so wait for at
     * least one to appear.
     */
    private static void awaitScanServers() throws Exception {
        long deadline = System.currentTimeMillis() + 60_000L;
        int registered = 0;
        while (System.currentTimeMillis() < deadline) {
            registered = client.instanceOperations().getScanServers().size();
            if (registered > 0) {
                break;
            }
            Thread.sleep(250L);
        }
        assertTrue(registered > 0, "no scan server registered, an offline table cannot be scanned without one");
        log.info("scan servers registered: {}", registered);
    }

    /**
     * Generate the pseudo-random event set and write it, then split the data tables so that the offline scans below have to cross tablet boundaries.
     */
    private static void writeData() throws Exception {
        ingest = new AbstractIngest(client, auths);

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

    /**
     * Flush and take the data tables offline, leaving the metadata table online.
     */
    private static void takeDataTablesOffline() throws Exception {
        TableOperations tops = client.tableOperations();
        for (String table : OFFLINE_TABLES) {
            tops.flush(table, null, null, true);
            tops.offline(table, true);
            log.info("took table offline: {}", table);
        }

        assertTrue(tops.isOnline(TableName.METADATA), "the metadata table must remain online");
        for (String table : OFFLINE_TABLES) {
            assertFalse(tops.isOnline(table), "expected table to be offline: " + table);
        }
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

    // ------------------------------------------------------------------
    // tests
    // ------------------------------------------------------------------

    /**
     * The narrowest case, a single event fetched by uuid from a single offline shard.
     */
    @Test
    public void testSingleEventByUuid() throws Exception {
        String uuid = events.get(0).uuid;

        givenFullDateRange();
        givenQuery("UUID == '" + uuid + "'");
        expectPlan("UUID == '" + uuid + "'");
        expect(Set.of(uuid));
        planAndExecuteQuery();
    }

    /**
     * A single term that matches events spread across every offline shard.
     */
    @Test
    public void testEqualityAcrossShards() throws Exception {
        Set<String> expected = uuidsMatching(event -> event.color.equals("red"));

        givenFullDateRange();
        givenQuery("COLOR == 'red'");
        expectPlan("COLOR == 'red'");
        expect(expected);
        planAndExecuteQuery();
    }

    /**
     * An intersection, which reads two field index ranges out of the offline shard table.
     */
    @Test
    public void testIntersection() throws Exception {
        Set<String> expected = uuidsMatching(event -> event.color.equals("red") && event.size == 5);

        givenFullDateRange();
        givenQuery("COLOR == 'red' && SIZE == '5'");
        expectPlan("COLOR == 'red' && SIZE == '+aE5'");
        expect(expected);
        planAndExecuteQuery();
    }

    /**
     * A union of two terms.
     */
    @Test
    public void testUnion() throws Exception {
        Set<String> expected = uuidsMatching(event -> event.color.equals("red") || event.color.equals("blue"));

        givenFullDateRange();
        givenQuery("COLOR == 'red' || COLOR == 'blue'");
        expectPlan("COLOR == 'red' || COLOR == 'blue'");
        expect(expected);
        planAndExecuteQuery();
    }

    /**
     * A negation, which requires the event to be evaluated after the index hit.
     */
    @Test
    public void testIntersectionWithNegation() throws Exception {
        Set<String> expected = uuidsMatching(event -> event.color.equals("red") && event.size != 5);

        givenFullDateRange();
        givenQuery("COLOR == 'red' && !(SIZE == '5')");
        expectPlan("COLOR == 'red' && !(SIZE == '+aE5')");
        expect(expected);
        planAndExecuteQuery();
    }

    /**
     * A regex, which drives an index expansion scan against the offline shard index before the shard table is read. The expansion resolves to the several CODE
     * values that share the queried prefix.
     */
    @Test
    public void testRegexIndexExpansion() throws Exception {
        Set<String> expected = uuidsMatching(event -> event.code.startsWith("alpha"));

        givenFullDateRange();
        givenQuery("CODE =~ 'alpha.*'");
        // the planner rewrites this into the union of the matching index values
        disableQueryPlanAssertion();
        expect(expected);
        planAndExecuteQuery();
    }

    /**
     * A bounded numeric range, which is expanded from the offline shard index. The {@code _Bounded_} marker is required, otherwise the planner rejects the pair
     * of inequalities as an incorrectly marked bounded range.
     */
    @Test
    public void testBoundedRange() throws Exception {
        Set<String> expected = uuidsMatching(event -> event.size >= 3 && event.size <= 5);

        givenFullDateRange();
        givenQuery("((_Bounded_ = true) && (SIZE >= '3' && SIZE <= '5'))");
        // the bounded range is rewritten into the expanded set of index values
        disableQueryPlanAssertion();
        expect(expected);
        planAndExecuteQuery();
    }

    /**
     * Restricting the query to one day proves the shard ranges are still honoured when the table is offline.
     */
    @Test
    public void testSingleDayRange() throws Exception {
        String date = DATES.get(1);
        Set<String> expected = uuidsMatching(event -> event.row.startsWith(date) && event.color.equals("red"));

        givenDate(date);
        givenQuery("COLOR == 'red'");
        expectPlan("COLOR == 'red'");
        expect(expected);
        planAndExecuteQuery();
    }

    /**
     * The reason {@code ScanServerQueryLogicFactory.xml} exists.
     * <p>
     * The stock {@code EventQuery} logic leaves every table at {@code IMMEDIATE}, so its scans are routed to a tablet server that has unloaded the offline
     * tables. Without a consistency level to flip, there is no configuration on the stock logic that would make this query succeed.
     */
    @Test
    public void testImmediateConsistencyFailsOnOfflineIndex() {
        givenFullDateRange();
        givenQuery("COLOR == 'red' && SIZE == '5'");
        disableQueryPlanAssertion();

        Exception thrown = assertThrows(Exception.class, () -> planAndExecuteQuery(immediateLogic),
                        "the stock IMMEDIATE logic must not be able to read the offline data tables");

        assertTrue(hasOfflineCause(thrown), "expected an offline table failure but got: " + describe(thrown));
    }

    /**
     * The companion to {@link #testImmediateConsistencyFailsOnOfflineIndex()}, and the more dangerous half of it.
     * <p>
     * A single-term query does not surface the offline table as an error at all. The index lookup for the term comes back empty, so the query simply yields no
     * ranges and reports success with zero results, which is indistinguishable from a query that legitimately matched nothing.
     */
    @Test
    public void testImmediateConsistencyReturnsNoResultsForSingleTerm() throws Exception {
        givenFullDateRange();
        givenQuery("COLOR == 'red'");
        expectPlan("COLOR == 'red'");
        expectResultCount(0);

        planAndExecuteQuery(immediateLogic);

        assertTrue(results.isEmpty(), "the stock IMMEDIATE logic must not return results from an offline table");
    }

    /**
     * Walk the cause chain looking for evidence that the failure was caused by the tables being offline.
     *
     * @param throwable
     *            the thrown exception
     * @return true if the failure is attributable to an offline table
     */
    private boolean hasOfflineCause(Throwable throwable) {
        for (Throwable t = throwable; t != null; t = t.getCause()) {
            if (t instanceof TableOfflineException) {
                return true;
            }
            String message = t.getMessage();
            if (message != null && message.toLowerCase().contains("offline")) {
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
     * Sanity check on the generated data, so a change to the seed or the generator that quietly collapses the event set is caught here rather than surfacing as
     * a confusing failure in one of the query tests.
     */
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
