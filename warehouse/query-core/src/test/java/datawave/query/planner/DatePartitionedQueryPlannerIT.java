package datawave.query.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TimeZone;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.data.type.LcNoDiacriticsType;
import datawave.query.QueryParameters;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.exceptions.DatawaveQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.TreeEqualityVisitor;
import datawave.query.jexl.visitors.TreeFlatteningRebuildingVisitor;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.AbstractIngest;
import datawave.query.util.AbstractQueryTest;
import datawave.table.constants.TableName;
import datawave.util.time.DateHelper;

/**
 * End-to-end tests of {@link DatePartitionedQueryPlanner}: ingest, plan, and execute via {@link ShardQueryLogic}. Each test ingests its own data with
 * {@link AbstractIngest}, using the minimum number of days needed to exercise its scenario.
 * <p>
 * {@link SingleTermTests}, {@link UnionTests}, and {@link IntersectionTests} run the same hole-topology scenarios against a single-term, an OR, and an AND
 * query respectively, since pushdown of unindexed fields produces different plan text - and, for OR queries, different resolvability - depending on query form.
 * The remaining top-level tests cover cross-datatype merging, datatype filtering, threshold edge cases, and sub-plan failure tolerance once, since those are
 * independent of query form.
 * <p>
 * Full table scan is always disabled. A sub-range that cannot resolve without it drops its events, or fails the whole query if every sub-range fails. Tests
 * note this where it drops a valid hit - including where OR and AND diverge: an OR needs every field indexed to avoid full table scan, but an AND only needs
 * one.
 * <p>
 * {@link AbstractQueryTest}'s plan assertion checks {@code config.getQueryTree()}, which reflects only the initial pre-partition plan here, so it stays
 * disabled. Instead, {@link #assertPlans(DayPlan...)} asserts the final plan per calendar day, cross-checked against the real plan text in
 * {@code logic.getConfig().getQueryString()}. {@link #assertPartition(RangeExpectation...)} separately asserts sub-range boundaries and unindexed-field sets.
 */
@ExtendWith(SpringExtension.class)
@ComponentScan(basePackages = "datawave.query")
// @formatter:off
@ContextConfiguration(locations = {
        "classpath:datawave/query/QueryLogicFactory.xml",
        "classpath:beanRefContext.xml",
        "classpath:MarkingFunctionsContext.xml",
        "classpath:MetadataHelperContext.xml",
        "classpath:CacheContext.xml"})
// @formatter:on
class DatePartitionedQueryPlannerIT extends AbstractQueryTest {

    private static final Authorizations auths = new Authorizations("ALL");
    private static final String FIELD_A = "FIELD_A";
    private static final String VALUE_A = "value-a";
    private static final String FIELD_B = "FIELD_B";
    private static final String VALUE_B = "value-b";
    private static final String DATATYPE_A = "datatype-a";
    private static final String DATATYPE_B = "datatype-b";

    private static final String SINGLE_TERM_QUERY = FIELD_A + " == '" + VALUE_A + "'";
    private static final String UNION_QUERY = FIELD_A + " == '" + VALUE_A + "' || " + FIELD_B + " == '" + VALUE_B + "'";
    private static final String INTERSECTION_QUERY = FIELD_A + " == '" + VALUE_A + "' && " + FIELD_B + " == '" + VALUE_B + "'";

    private static InMemoryAccumuloClient client;

    private AbstractIngest ingest;

    @Autowired
    @Qualifier("EventQuery")
    protected ShardQueryLogic logic;

    @Autowired
    @Qualifier("metadataHelperCacheManager")
    protected CacheManager metadataHelperCacheManager;

    @Override
    public ShardQueryLogic getLogic() {
        return logic;
    }

    @Override
    public Authorizations getAuths() {
        return auths;
    }

    @Override
    protected List<String> getIndexTableNames() {
        return List.of(TableName.SHARD_INDEX);
    }

    @Override
    protected void extraConfigurations() {
        logic.setQueryPlanner(new DatePartitionedQueryPlanner());
        // full table scan is never enabled in this suite - see the class javadoc
        logic.setFullTableScanEnabled(false);
        disableQueryPlanAssertion();
    }

    @Override
    protected void extraAssertions() {
        // no-op
    }

    @BeforeAll
    static void beforeAll() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        client = new InMemoryAccumuloClient("", new InMemoryInstance(DatePartitionedQueryPlannerIT.class.getName()));
    }

    @BeforeEach
    void beforeEach() throws Exception {
        // AbstractIngest's constructor runs MacTestUtil.createOrRecreate() -> tables are deleted and recreated fresh for every test
        ingest = new AbstractIngest(client, auths);
        ingest.registerField(FIELD_A, DATATYPE_A, new LcNoDiacriticsType());
        ingest.registerField(FIELD_A, DATATYPE_B, new LcNoDiacriticsType());
        ingest.registerField(FIELD_B, DATATYPE_A, new LcNoDiacriticsType());
        ingest.registerField(FIELD_B, DATATYPE_B, new LcNoDiacriticsType());
        // "i" is write-gating only (no METADATA declaration row) since writeMetadataCounts drives hole detection instead - see
        // AbstractIngest#registerWriteGatingColumns. "e" is registered normally since its declaration row isn't read by hole detection.
        ingest.registerColumns(FIELD_A, DATATYPE_A, List.of("e"));
        ingest.registerColumns(FIELD_A, DATATYPE_B, List.of("e"));
        ingest.registerColumns(FIELD_B, DATATYPE_A, List.of("e"));
        ingest.registerColumns(FIELD_B, DATATYPE_B, List.of("e"));
        ingest.registerWriteGatingColumns(FIELD_A, List.of("i"));
        ingest.registerWriteGatingColumns(FIELD_B, List.of("i"));

        // CRITICAL: metadata lookups (including getFieldIndexHoles) are @Cacheable keyed by {auths, metadataTableName} - NOT by Accumulo instance.
        // Without this, a test run after a hole-ingesting test would read stale index holes out of the Caffeine cache.
        metadataHelperCacheManager.getCacheNames().forEach(name -> metadataHelperCacheManager.getCache(name).clear());

        setClientForTest(client);
    }

    /** Write one event on the given date/datatype carrying both {@value #FIELD_A} and {@value #FIELD_B}, so it is a hit for every query form tested here. */
    private void writeEvent(String date, String datatype, int eventId) {
        ingest.writeFV(date + "_0", datatype, eventId, FIELD_A, VALUE_A);
        ingest.writeFV(date + "_0", datatype, eventId, FIELD_B, VALUE_B);
    }

    private void writeFullIndex(String date, String datatype, String field) {
        ingest.writeMetadataCounts(field, datatype, date, 10L, 10L);
    }

    private void writeHole(String date, String datatype, String field) {
        ingest.writeMetadataCounts(field, datatype, date, 10L, 0L);
    }

    private SortedMap<Pair<Date,Date>,Set<String>> partition() throws Exception {
        DatePartitionedQueryPlanner planner = (DatePartitionedQueryPlanner) logic.getQueryPlanner();
        return planner.getSubQueryDateRanges(logic.getConfig());
    }

    private static Date date(String yyyyMMdd) {
        return DateHelper.parse(yyyyMMdd);
    }

    private static Date endOfDay(String yyyyMMdd) {
        return new Date(date(yyyyMMdd).getTime() + 24L * 60 * 60 * 1000 - 1);
    }

    private static final class RangeExpectation {
        private final Date start;
        private final Date end;
        private final Set<String> fields;

        private RangeExpectation(Date start, Date end, String... fields) {
            this.start = start;
            this.end = end;
            this.fields = Set.of(fields);
        }
    }

    private static RangeExpectation range(Date start, Date end, String... unindexedFields) {
        return new RangeExpectation(start, end, unindexedFields);
    }

    private void assertPartition(RangeExpectation... expected) throws Exception {
        SortedMap<Pair<Date,Date>,Set<String>> actual = partition();
        assertEquals(expected.length, actual.size(), () -> "expected " + expected.length + " ranges but got: " + actual);
        List<Map.Entry<Pair<Date,Date>,Set<String>>> entries = new ArrayList<>(actual.entrySet());
        for (int i = 0; i < expected.length; i++) {
            Map.Entry<Pair<Date,Date>,Set<String>> actualEntry = entries.get(i);
            RangeExpectation exp = expected[i];
            assertEquals(exp.start, actualEntry.getKey().getLeft(), "start of range " + i);
            assertEquals(exp.end, actualEntry.getKey().getRight(), "end of range " + i);
            assertEquals(exp.fields, actualEntry.getValue(), "unindexed fields of range " + i);
        }
    }

    private static final class DayPlan {
        private final Date day;
        private final String plan;

        private DayPlan(Date day, String plan) {
            this.day = day;
            this.plan = plan;
        }
    }

    /** Pairs a calendar day with the final plan text expected for whichever sub-range contains it. */
    private static DayPlan plan(String yyyyMMdd, String expectedPlan) {
        return new DayPlan(date(yyyyMMdd), expectedPlan);
    }

    /**
     * Assert the final plan text for every calendar day in the query range, one assertion per day (see the class javadoc). Each day must fall inside exactly
     * one sub-range - verified here, not assumed - and the distinct set of plan texts supplied is cross-checked against the real output read back from
     * {@code logic.getConfig().getQueryString()} via {@link PartitionedPlanVisitor}.
     * <p>
     * "Distinct" here is judged by exact String equality, matching how the real output's own {@code Plans} set is deduplicated in production (a plain
     * {@code Set<String>} in {@link PartitionedPlanVisitor}/{@code DatePartitionedQueryIterable}) - <b>not</b> the structural equality {@link #plansEqual} uses
     * for the final cross-check. Deliberately so: a {@code QueryPropertyMarker} node like {@code (_Eval_ = true) && (source)} loses which specific term it
     * wraps once {@link TreeFlatteningRebuildingVisitor#flatten} merges it into a surrounding AND/OR of 3 or more terms, so two structurally-flattened-equal
     * but semantically different plans (e.g. FIELD_A delayed vs. FIELD_B delayed, both ANDed with an unrelated indexed term) would be wrongly treated as one
     * plan if this method deduplicated structurally too. Every {@code plan(...)} call in this file therefore reuses the exact same literal for what is truly
     * the same plan, and intentionally writes out distinct-but-similar-looking plans in full rather than relying on any form of equivalence-based collapsing.
     */
    private void assertPlans(DayPlan... expected) throws Exception {
        SortedMap<Pair<Date,Date>,Set<String>> partition = partition();
        Set<String> expectedFinalPlans = new HashSet<>();
        for (DayPlan dayPlan : expected) {
            Pair<Date,Date> containingRange = null;
            for (Pair<Date,Date> range : partition.keySet()) {
                if (!dayPlan.day.before(range.getLeft()) && !dayPlan.day.after(range.getRight())) {
                    Pair<Date,Date> alreadyFound = containingRange;
                    assertNull(alreadyFound, () -> "day " + dayPlan.day + " is contained by more than one sub-range: " + alreadyFound + " and " + range);
                    containingRange = range;
                }
            }
            assertNotNull(containingRange, () -> "no sub-range contains day " + dayPlan.day);
            expectedFinalPlans.add(dayPlan.plan);
        }

        Set<String> actualFinalPlans = PartitionedPlanVisitor.getPlans(logic.getConfig().getQueryString()).getPlans();
        assertPlanEquals(expectedFinalPlans, actualFinalPlans);
    }

    private static void assertPlanEquals(Set<String> expectedPlans, Set<String> actualPlans) throws ParseException {
        assertEquals(expectedPlans.size(), actualPlans.size(),
                        () -> "expected " + expectedPlans.size() + " distinct final plan(s) but got " + actualPlans.size() + ": " + actualPlans);
        Set<String> remaining = new HashSet<>(actualPlans);
        for (String expected : expectedPlans) {
            String match = null;
            for (String actual : remaining) {
                if (plansEqual(expected, actual)) {
                    match = actual;
                    break;
                }
            }
            assertNotNull(match, () -> "expected final plan not found: " + expected + " in " + actualPlans);
            remaining.remove(match);
        }
    }

    private static boolean plansEqual(String expected, String actual) throws ParseException {
        if (expected.equals(actual)) {
            return true;
        }
        ASTJexlScript expectedTree = TreeFlatteningRebuildingVisitor.flatten(JexlASTHelper.parseJexlQuery(expected));
        ASTJexlScript actualTree = TreeFlatteningRebuildingVisitor.flatten(JexlASTHelper.parseJexlQuery(actual));
        return TreeEqualityVisitor.isEqual(expectedTree, actualTree);
    }

    @ExtendWith(SpringExtension.class)
    @ComponentScan(basePackages = "datawave.query")
    // @formatter:off
    @ContextConfiguration(locations = {
            "classpath:datawave/query/QueryLogicFactory.xml",
            "classpath:beanRefContext.xml",
            "classpath:MarkingFunctionsContext.xml",
            "classpath:MetadataHelperContext.xml",
            "classpath:CacheContext.xml"})
    // @formatter:on
    @Nested
    class SingleTermTests {

        @Test
        void noHoles() throws Exception {
            writeEvent("20260701", DATATYPE_A, 1);
            writeFullIndex("20260701", DATATYPE_A, FIELD_A);

            givenDate("20260701");
            givenQuery(SINGLE_TERM_QUERY);
            expectResultCount(1);

            planAndExecuteQuery();

            // @formatter:off
            assertPartition(
                            range(date("20260701"), date("20260701")));
            assertPlans(
                            plan("20260701", "FIELD_A == 'value-a'"));
            // @formatter:on
        }

        @Test
        void holeAtStart() throws Exception {
            writeEvent("20260701", DATATYPE_A, 1);
            writeEvent("20260702", DATATYPE_A, 2);
            writeHole("20260701", DATATYPE_A, FIELD_A);
            writeFullIndex("20260702", DATATYPE_A, FIELD_A);

            givenDate("20260701", "20260702");
            givenQuery(SINGLE_TERM_QUERY);
            // Jul 1's event is a valid hit that is missed: its sub-range is entirely unindexed and full table scan is disabled.
            expectResultCount(1);

            planAndExecuteQuery();

            // @formatter:off
            assertPartition(
                            range(date("20260701"), endOfDay("20260701"), FIELD_A),
                            range(date("20260702"), date("20260702")));
            assertPlans(
                            plan("20260701", "(_Eval_ = true) && (FIELD_A == 'value-a')"),
                            plan("20260702", "FIELD_A == 'value-a'"));
            // @formatter:on
        }

        @Test
        void holeAtEnd() throws Exception {
            writeEvent("20260701", DATATYPE_A, 1);
            writeEvent("20260702", DATATYPE_A, 2);
            writeFullIndex("20260701", DATATYPE_A, FIELD_A);
            writeHole("20260702", DATATYPE_A, FIELD_A);

            givenDate("20260701", "20260702");
            givenQuery(SINGLE_TERM_QUERY);
            // Jul 2's event is a valid hit that is missed: its sub-range is entirely unindexed and full table scan is disabled.
            expectResultCount(1);

            planAndExecuteQuery();

            // @formatter:off
            assertPartition(
                            range(date("20260701"), endOfDay("20260701")),
                            range(date("20260702"), date("20260702"), FIELD_A));
            assertPlans(
                            plan("20260701", "FIELD_A == 'value-a'"),
                            plan("20260702", "(_Eval_ = true) && (FIELD_A == 'value-a')"));
            // @formatter:on
        }

        @Test
        void holeMidRange() throws Exception {
            for (String d : List.of("20260701", "20260702", "20260703")) {
                writeEvent(d, DATATYPE_A, 1);
            }
            writeFullIndex("20260701", DATATYPE_A, FIELD_A);
            writeHole("20260702", DATATYPE_A, FIELD_A);
            writeFullIndex("20260703", DATATYPE_A, FIELD_A);

            givenDate("20260701", "20260703");
            givenQuery(SINGLE_TERM_QUERY);
            // Jul 2's event is a valid hit that is missed: its sub-range is entirely unindexed and full table scan is disabled.
            expectResultCount(2);

            planAndExecuteQuery();

            // @formatter:off
            assertPartition(
                            range(date("20260701"), endOfDay("20260701")),
                            range(date("20260702"), endOfDay("20260702"), FIELD_A),
                            range(date("20260703"), date("20260703")));
            assertPlans(
                            plan("20260701", "FIELD_A == 'value-a'"),
                            plan("20260702", "(_Eval_ = true) && (FIELD_A == 'value-a')"),
                            plan("20260703", "FIELD_A == 'value-a'"));
            // @formatter:on
        }

        /**
         * A hole in only one datatype still partitions the range for <b>all</b> datatypes - {@code collapseDatatypes}'s deliberate pessimism.
         */
        @Test
        void holeInOneDatatypeOnly() throws Exception {
            writeEvent("20260701", DATATYPE_A, 1);
            writeEvent("20260701", DATATYPE_B, 2);
            writeEvent("20260702", DATATYPE_A, 3);
            writeEvent("20260702", DATATYPE_B, 4);
            writeFullIndex("20260701", DATATYPE_A, FIELD_A);
            writeFullIndex("20260701", DATATYPE_B, FIELD_A);
            writeHole("20260702", DATATYPE_A, FIELD_A);
            writeFullIndex("20260702", DATATYPE_B, FIELD_A);

            givenDate("20260701", "20260702");
            givenQuery(SINGLE_TERM_QUERY);
            // Jul 2's datatype-b event is a valid hit (datatype-b was actually indexed that day) that is nonetheless missed: collapseDatatypes'
            // pessimism marks the whole Jul 2 sub-range unindexed because datatype-a has a hole there.
            expectResultCount(2);

            planAndExecuteQuery();

            // @formatter:off
            assertPartition(
                            range(date("20260701"), endOfDay("20260701")),
                            range(date("20260702"), date("20260702"), FIELD_A));
            assertPlans(
                            plan("20260701", "FIELD_A == 'value-a'"),
                            plan("20260702", "(_Eval_ = true) && (FIELD_A == 'value-a')"));
            // @formatter:on
        }
    }

    @ExtendWith(SpringExtension.class)
    @ComponentScan(basePackages = "datawave.query")
    // @formatter:off
    @ContextConfiguration(locations = {
            "classpath:datawave/query/QueryLogicFactory.xml",
            "classpath:beanRefContext.xml",
            "classpath:MarkingFunctionsContext.xml",
            "classpath:MetadataHelperContext.xml",
            "classpath:CacheContext.xml"})
    // @formatter:on
    @Nested
    class UnionTests {

        @Test
        void noHoles() throws Exception {
            writeEvent("20260701", DATATYPE_A, 1);
            writeFullIndex("20260701", DATATYPE_A, FIELD_A);
            writeFullIndex("20260701", DATATYPE_A, FIELD_B);

            givenDate("20260701");
            givenQuery(UNION_QUERY);
            expectResultCount(1);

            planAndExecuteQuery();

            // @formatter:off
            assertPartition(
                            range(date("20260701"), date("20260701")));
            assertPlans(
                            plan("20260701", "FIELD_A == 'value-a' || FIELD_B == 'value-b'"));
            // @formatter:on
        }

        @Test
        void holeAtStart() throws Exception {
            writeEvent("20260701", DATATYPE_A, 1);
            writeEvent("20260702", DATATYPE_A, 2);
            writeHole("20260701", DATATYPE_A, FIELD_A);
            writeHole("20260701", DATATYPE_A, FIELD_B);
            writeFullIndex("20260702", DATATYPE_A, FIELD_A);
            writeFullIndex("20260702", DATATYPE_A, FIELD_B);

            givenDate("20260701", "20260702");
            givenQuery(UNION_QUERY);
            // Jul 1's event is a valid hit that is missed: an OR needs every field indexed to avoid full table scan, and both fields are unindexed
            // that day.
            expectResultCount(1);

            planAndExecuteQuery();

            // @formatter:off
            assertPartition(
                            range(date("20260701"), endOfDay("20260701"), FIELD_A, FIELD_B),
                            range(date("20260702"), date("20260702")));
            assertPlans(
                            plan("20260701", "(_Eval_ = true) && (FIELD_A == 'value-a') || (_Eval_ = true) && (FIELD_B == 'value-b')"),
                            plan("20260702", "FIELD_A == 'value-a' || FIELD_B == 'value-b'"));
            // @formatter:on
        }

        @Test
        void holeAtEnd() throws Exception {
            writeEvent("20260701", DATATYPE_A, 1);
            writeEvent("20260702", DATATYPE_A, 2);
            writeFullIndex("20260701", DATATYPE_A, FIELD_A);
            writeFullIndex("20260701", DATATYPE_A, FIELD_B);
            writeHole("20260702", DATATYPE_A, FIELD_A);
            writeHole("20260702", DATATYPE_A, FIELD_B);

            givenDate("20260701", "20260702");
            givenQuery(UNION_QUERY);
            // Jul 2's event is a valid hit that is missed: both fields are unindexed that day, and an OR needs every field indexed to avoid full
            // table scan.
            expectResultCount(1);

            planAndExecuteQuery();

            // @formatter:off
            assertPartition(
                            range(date("20260701"), endOfDay("20260701")),
                            range(date("20260702"), date("20260702"), FIELD_A, FIELD_B));
            assertPlans(
                            plan("20260701", "FIELD_A == 'value-a' || FIELD_B == 'value-b'"),
                            plan("20260702", "(_Eval_ = true) && (FIELD_A == 'value-a') || (_Eval_ = true) && (FIELD_B == 'value-b')"));
            // @formatter:on
        }

        @Test
        void holeMidRange() throws Exception {
            for (String d : List.of("20260701", "20260702", "20260703")) {
                writeEvent(d, DATATYPE_A, 1);
            }
            writeFullIndex("20260701", DATATYPE_A, FIELD_A);
            writeFullIndex("20260701", DATATYPE_A, FIELD_B);
            writeHole("20260702", DATATYPE_A, FIELD_A);
            writeHole("20260702", DATATYPE_A, FIELD_B);
            writeFullIndex("20260703", DATATYPE_A, FIELD_A);
            writeFullIndex("20260703", DATATYPE_A, FIELD_B);

            givenDate("20260701", "20260703");
            givenQuery(UNION_QUERY);
            // Jul 2's event is a valid hit that is missed: both fields are unindexed that day, and an OR needs every field indexed to avoid full
            // table scan.
            expectResultCount(2);

            planAndExecuteQuery();

            // @formatter:off
            assertPartition(
                            range(date("20260701"), endOfDay("20260701")),
                            range(date("20260702"), endOfDay("20260702"), FIELD_A, FIELD_B),
                            range(date("20260703"), date("20260703")));
            assertPlans(
                            plan("20260701", "FIELD_A == 'value-a' || FIELD_B == 'value-b'"),
                            plan("20260702", "(_Eval_ = true) && (FIELD_A == 'value-a') || (_Eval_ = true) && (FIELD_B == 'value-b')"),
                            plan("20260703", "FIELD_A == 'value-a' || FIELD_B == 'value-b'"));
            // @formatter:on
        }

        @Test
        void holeInOneDatatypeOnly() throws Exception {
            writeEvent("20260701", DATATYPE_A, 1);
            writeEvent("20260701", DATATYPE_B, 2);
            writeEvent("20260702", DATATYPE_A, 3);
            writeEvent("20260702", DATATYPE_B, 4);
            writeFullIndex("20260701", DATATYPE_A, FIELD_A);
            writeFullIndex("20260701", DATATYPE_B, FIELD_A);
            writeFullIndex("20260701", DATATYPE_A, FIELD_B);
            writeFullIndex("20260701", DATATYPE_B, FIELD_B);
            writeHole("20260702", DATATYPE_A, FIELD_A);
            writeFullIndex("20260702", DATATYPE_B, FIELD_A);
            writeFullIndex("20260702", DATATYPE_A, FIELD_B);
            writeFullIndex("20260702", DATATYPE_B, FIELD_B);

            givenDate("20260701", "20260702");
            givenQuery(UNION_QUERY);
            // Jul 2's 2 events are valid hits that are missed: collapseDatatypes' pessimism marks FIELD_A unindexed for the whole day because
            // datatype-a has a hole there, and an OR needs every field indexed to avoid full table scan - even though FIELD_B and datatype-b's
            // FIELD_A were both actually indexed that day.
            expectResultCount(2);

            planAndExecuteQuery();

            // @formatter:off
            assertPartition(
                            range(date("20260701"), endOfDay("20260701")),
                            range(date("20260702"), date("20260702"), FIELD_A));
            assertPlans(
                            plan("20260701", "FIELD_A == 'value-a' || FIELD_B == 'value-b'"),
                            plan("20260702", "FIELD_B == 'value-b' || (_Eval_ = true) && (FIELD_A == 'value-a')"));
            // @formatter:on
        }

        @Test
        void oneFieldHoleOnly() throws Exception {
            writeEvent("20260701", DATATYPE_A, 1);
            writeEvent("20260702", DATATYPE_A, 2);
            writeFullIndex("20260701", DATATYPE_A, FIELD_A);
            writeFullIndex("20260701", DATATYPE_A, FIELD_B);
            writeHole("20260702", DATATYPE_A, FIELD_A);
            writeFullIndex("20260702", DATATYPE_A, FIELD_B);

            givenDate("20260701", "20260702");
            givenQuery(UNION_QUERY);
            // Jul 2's event is a valid hit that is missed: FIELD_A alone is unindexed that day, but an OR needs every field indexed to avoid full
            // table scan even when only one field has a hole.
            expectResultCount(1);

            planAndExecuteQuery();

            // @formatter:off
            assertPartition(
                            range(date("20260701"), endOfDay("20260701")),
                            range(date("20260702"), date("20260702"), FIELD_A));
            assertPlans(
                            plan("20260701", "FIELD_A == 'value-a' || FIELD_B == 'value-b'"),
                            plan("20260702", "FIELD_B == 'value-b' || (_Eval_ = true) && (FIELD_A == 'value-a')"));
            // @formatter:on
        }

        /**
         * FIELD_A's hole (Jul 1-2) and FIELD_B's hole (Jul 2-3) overlap on Jul 2 but leave no day where both fields are indexed. Since an OR needs every field
         * indexed to avoid full table scan, every one of the 3 sub-ranges fails; with more than one sub-plan failure, {@code evaluateFailures} throws
         * DatawaveFatalQueryException (with each failure suppressed) rather than unwrapping a single DatawaveQueryException.
         */
        @Test
        void holesOverlappingAcrossFields() throws Exception {
            for (String d : List.of("20260701", "20260702", "20260703")) {
                writeEvent(d, DATATYPE_A, 1);
            }
            writeHole("20260701", DATATYPE_A, FIELD_A);
            writeHole("20260702", DATATYPE_A, FIELD_A);
            writeFullIndex("20260703", DATATYPE_A, FIELD_A);
            writeFullIndex("20260701", DATATYPE_A, FIELD_B);
            writeHole("20260702", DATATYPE_A, FIELD_B);
            writeHole("20260703", DATATYPE_A, FIELD_B);

            givenDate("20260701", "20260703");
            givenQuery(UNION_QUERY);

            assertThrows(DatawaveFatalQueryException.class, DatePartitionedQueryPlannerIT.this::planAndExecuteQuery);
        }

        @Test
        void holesDisjointAcrossFields() throws Exception {
            for (String d : List.of("20260701", "20260702", "20260703")) {
                writeEvent(d, DATATYPE_A, 1);
            }
            writeHole("20260701", DATATYPE_A, FIELD_A);
            writeFullIndex("20260701", DATATYPE_A, FIELD_B);
            writeFullIndex("20260702", DATATYPE_A, FIELD_A);
            writeFullIndex("20260702", DATATYPE_A, FIELD_B);
            writeFullIndex("20260703", DATATYPE_A, FIELD_A);
            writeHole("20260703", DATATYPE_A, FIELD_B);

            givenDate("20260701", "20260703");
            givenQuery(UNION_QUERY);
            // Jul 1's and Jul 3's events are valid hits that are missed: each day has one unindexed field, and an OR needs every field indexed to
            // avoid full table scan. Jul 2 has no holes and resolves normally.
            expectResultCount(1);

            planAndExecuteQuery();

            // @formatter:off
            assertPartition(
                            range(date("20260701"), endOfDay("20260701"), FIELD_A),
                            range(date("20260702"), endOfDay("20260702")),
                            range(date("20260703"), date("20260703"), FIELD_B));
            assertPlans(
                            plan("20260701", "FIELD_B == 'value-b' || (_Eval_ = true) && (FIELD_A == 'value-a')"),
                            plan("20260702", "FIELD_A == 'value-a' || FIELD_B == 'value-b'"),
                            plan("20260703", "FIELD_A == 'value-a' || (_Eval_ = true) && (FIELD_B == 'value-b')"));
            // @formatter:on
        }
    }

    @ExtendWith(SpringExtension.class)
    @ComponentScan(basePackages = "datawave.query")
    // @formatter:off
    @ContextConfiguration(locations = {
            "classpath:datawave/query/QueryLogicFactory.xml",
            "classpath:beanRefContext.xml",
            "classpath:MarkingFunctionsContext.xml",
            "classpath:MetadataHelperContext.xml",
            "classpath:CacheContext.xml"})
    // @formatter:on
    @Nested
    class IntersectionTests {

        @Test
        void noHoles() throws Exception {
            writeEvent("20260701", DATATYPE_A, 1);
            writeFullIndex("20260701", DATATYPE_A, FIELD_A);
            writeFullIndex("20260701", DATATYPE_A, FIELD_B);

            givenDate("20260701");
            givenQuery(INTERSECTION_QUERY);
            expectResultCount(1);

            planAndExecuteQuery();

            // @formatter:off
            assertPartition(
                            range(date("20260701"), date("20260701")));
            assertPlans(
                            plan("20260701", "FIELD_A == 'value-a' && FIELD_B == 'value-b'"));
            // @formatter:on
        }

        @Test
        void holeAtStart() throws Exception {
            writeEvent("20260701", DATATYPE_A, 1);
            writeEvent("20260702", DATATYPE_A, 2);
            writeHole("20260701", DATATYPE_A, FIELD_A);
            writeHole("20260701", DATATYPE_A, FIELD_B);
            writeFullIndex("20260702", DATATYPE_A, FIELD_A);
            writeFullIndex("20260702", DATATYPE_A, FIELD_B);

            givenDate("20260701", "20260702");
            givenQuery(INTERSECTION_QUERY);
            // Jul 1's event is a valid hit that is missed: both fields are unindexed that day, so an AND has no index left to seed a scan from and
            // requires full table scan.
            expectResultCount(1);

            planAndExecuteQuery();

            // @formatter:off
            assertPartition(
                            range(date("20260701"), endOfDay("20260701"), FIELD_A, FIELD_B),
                            range(date("20260702"), date("20260702")));
            assertPlans(
                            plan("20260701", "(_Eval_ = true) && (FIELD_A == 'value-a') && (_Eval_ = true) && (FIELD_B == 'value-b')"),
                            plan("20260702", "FIELD_A == 'value-a' && FIELD_B == 'value-b'"));
            // @formatter:on
        }

        @Test
        void holeAtEnd() throws Exception {
            writeEvent("20260701", DATATYPE_A, 1);
            writeEvent("20260702", DATATYPE_A, 2);
            writeFullIndex("20260701", DATATYPE_A, FIELD_A);
            writeFullIndex("20260701", DATATYPE_A, FIELD_B);
            writeHole("20260702", DATATYPE_A, FIELD_A);
            writeHole("20260702", DATATYPE_A, FIELD_B);

            givenDate("20260701", "20260702");
            givenQuery(INTERSECTION_QUERY);
            // Jul 2's event is a valid hit that is missed: both fields are unindexed that day, so an AND has no index left to seed a scan from and
            // requires full table scan.
            expectResultCount(1);

            planAndExecuteQuery();

            // @formatter:off
            assertPartition(
                            range(date("20260701"), endOfDay("20260701")),
                            range(date("20260702"), date("20260702"), FIELD_A, FIELD_B));
            assertPlans(
                            plan("20260701", "FIELD_A == 'value-a' && FIELD_B == 'value-b'"),
                            plan("20260702", "(_Eval_ = true) && (FIELD_A == 'value-a') && (_Eval_ = true) && (FIELD_B == 'value-b')"));
            // @formatter:on
        }

        @Test
        void holeMidRange() throws Exception {
            for (String d : List.of("20260701", "20260702", "20260703")) {
                writeEvent(d, DATATYPE_A, 1);
            }
            writeFullIndex("20260701", DATATYPE_A, FIELD_A);
            writeFullIndex("20260701", DATATYPE_A, FIELD_B);
            writeHole("20260702", DATATYPE_A, FIELD_A);
            writeHole("20260702", DATATYPE_A, FIELD_B);
            writeFullIndex("20260703", DATATYPE_A, FIELD_A);
            writeFullIndex("20260703", DATATYPE_A, FIELD_B);

            givenDate("20260701", "20260703");
            givenQuery(INTERSECTION_QUERY);
            // Jul 2's event is a valid hit that is missed: both fields are unindexed that day, so an AND has no index left to seed a scan from and
            // requires full table scan.
            expectResultCount(2);

            planAndExecuteQuery();

            // @formatter:off
            assertPartition(
                            range(date("20260701"), endOfDay("20260701")),
                            range(date("20260702"), endOfDay("20260702"), FIELD_A, FIELD_B),
                            range(date("20260703"), date("20260703")));
            assertPlans(
                            plan("20260701", "FIELD_A == 'value-a' && FIELD_B == 'value-b'"),
                            plan("20260702", "(_Eval_ = true) && (FIELD_A == 'value-a') && (_Eval_ = true) && (FIELD_B == 'value-b')"),
                            plan("20260703", "FIELD_A == 'value-a' && FIELD_B == 'value-b'"));
            // @formatter:on
        }

        /**
         * Unlike {@link UnionTests#holeInOneDatatypeOnly()}, no hit is missed here: collapseDatatypes' pessimism still marks Jul 2 unindexed for FIELD_A, but
         * an AND only needs one field indexed to seed a scan, and FIELD_B remains indexed every day.
         */
        @Test
        void holeInOneDatatypeOnly() throws Exception {
            writeEvent("20260701", DATATYPE_A, 1);
            writeEvent("20260701", DATATYPE_B, 2);
            writeEvent("20260702", DATATYPE_A, 3);
            writeEvent("20260702", DATATYPE_B, 4);
            writeFullIndex("20260701", DATATYPE_A, FIELD_A);
            writeFullIndex("20260701", DATATYPE_B, FIELD_A);
            writeFullIndex("20260701", DATATYPE_A, FIELD_B);
            writeFullIndex("20260701", DATATYPE_B, FIELD_B);
            writeHole("20260702", DATATYPE_A, FIELD_A);
            writeFullIndex("20260702", DATATYPE_B, FIELD_A);
            writeFullIndex("20260702", DATATYPE_A, FIELD_B);
            writeFullIndex("20260702", DATATYPE_B, FIELD_B);

            givenDate("20260701", "20260702");
            givenQuery(INTERSECTION_QUERY);
            expectResultCount(4);

            planAndExecuteQuery();

            // @formatter:off
            assertPartition(
                            range(date("20260701"), endOfDay("20260701")),
                            range(date("20260702"), date("20260702"), FIELD_A));
            assertPlans(
                            plan("20260701", "FIELD_A == 'value-a' && FIELD_B == 'value-b'"),
                            plan("20260702", "FIELD_B == 'value-b' && (_Eval_ = true) && (FIELD_A == 'value-a')"));
            // @formatter:on
        }

        /**
         * Unlike {@link UnionTests#oneFieldHoleOnly()}, no hit is missed here: an AND only needs one field indexed to seed a scan, and FIELD_B remains indexed
         * on Jul 2.
         */
        @Test
        void oneFieldHoleOnly() throws Exception {
            writeEvent("20260701", DATATYPE_A, 1);
            writeEvent("20260702", DATATYPE_A, 2);
            writeFullIndex("20260701", DATATYPE_A, FIELD_A);
            writeFullIndex("20260701", DATATYPE_A, FIELD_B);
            writeHole("20260702", DATATYPE_A, FIELD_A);
            writeFullIndex("20260702", DATATYPE_A, FIELD_B);

            givenDate("20260701", "20260702");
            givenQuery(INTERSECTION_QUERY);
            expectResultCount(2);

            planAndExecuteQuery();

            // @formatter:off
            assertPartition(
                            range(date("20260701"), endOfDay("20260701")),
                            range(date("20260702"), date("20260702"), FIELD_A));
            assertPlans(
                            plan("20260701", "FIELD_A == 'value-a' && FIELD_B == 'value-b'"),
                            plan("20260702", "FIELD_B == 'value-b' && (_Eval_ = true) && (FIELD_A == 'value-a')"));
            // @formatter:on
        }

        /**
         * Unlike {@link UnionTests#holesOverlappingAcrossFields()}, this does not fail outright: Jul 1 and Jul 3 each still have one indexed field for an AND
         * to seed a scan from and resolve normally. Only Jul 2, where both fields are unindexed, has no index left to seed from and drops its hit.
         */
        @Test
        void holesOverlappingAcrossFields() throws Exception {
            for (String d : List.of("20260701", "20260702", "20260703")) {
                writeEvent(d, DATATYPE_A, 1);
            }
            writeHole("20260701", DATATYPE_A, FIELD_A);
            writeHole("20260702", DATATYPE_A, FIELD_A);
            writeFullIndex("20260703", DATATYPE_A, FIELD_A);
            writeFullIndex("20260701", DATATYPE_A, FIELD_B);
            writeHole("20260702", DATATYPE_A, FIELD_B);
            writeHole("20260703", DATATYPE_A, FIELD_B);

            givenDate("20260701", "20260703");
            givenQuery(INTERSECTION_QUERY);
            // Jul 2's event is a valid hit that is missed: both fields are unindexed that day, so an AND has no index left to seed a scan from and
            // requires full table scan. Jul 1 and Jul 3 each still have one indexed field and resolve normally.
            expectResultCount(2);

            planAndExecuteQuery();

            // @formatter:off
            assertPartition(
                            range(date("20260701"), endOfDay("20260701"), FIELD_A),
                            range(date("20260702"), endOfDay("20260702"), FIELD_A, FIELD_B),
                            range(date("20260703"), date("20260703"), FIELD_B));
            assertPlans(
                            plan("20260701", "FIELD_B == 'value-b' && (_Eval_ = true) && (FIELD_A == 'value-a')"),
                            plan("20260702", "(_Eval_ = true) && (FIELD_A == 'value-a') && (_Eval_ = true) && (FIELD_B == 'value-b')"),
                            plan("20260703", "FIELD_A == 'value-a' && (_Eval_ = true) && (FIELD_B == 'value-b')"));
            // @formatter:on
        }

        @Test
        void holesDisjointAcrossFields() throws Exception {
            for (String d : List.of("20260701", "20260702", "20260703")) {
                writeEvent(d, DATATYPE_A, 1);
            }
            writeHole("20260701", DATATYPE_A, FIELD_A);
            writeFullIndex("20260701", DATATYPE_A, FIELD_B);
            writeFullIndex("20260702", DATATYPE_A, FIELD_A);
            writeFullIndex("20260702", DATATYPE_A, FIELD_B);
            writeFullIndex("20260703", DATATYPE_A, FIELD_A);
            writeHole("20260703", DATATYPE_A, FIELD_B);

            givenDate("20260701", "20260703");
            givenQuery(INTERSECTION_QUERY);
            // No hit is missed: every day has at least one indexed field for an AND to seed a scan from.
            expectResultCount(3);

            planAndExecuteQuery();

            // @formatter:off
            assertPartition(
                            range(date("20260701"), endOfDay("20260701"), FIELD_A),
                            range(date("20260702"), endOfDay("20260702")),
                            range(date("20260703"), date("20260703"), FIELD_B));
            assertPlans(
                            plan("20260701", "FIELD_B == 'value-b' && (_Eval_ = true) && (FIELD_A == 'value-a')"),
                            plan("20260702", "FIELD_A == 'value-a' && FIELD_B == 'value-b'"),
                            plan("20260703", "FIELD_A == 'value-a' && (_Eval_ = true) && (FIELD_B == 'value-b')"));
            // @formatter:on
        }
    }

    /**
     * Adjacent cross-datatype holes for the same field merge into a single sub-range. Left unmerged they would produce two back-to-back sub-ranges with the
     * identical unindexed-field set {@code {FIELD_A}}, which {@code ensureConsistency}'s {@code matchingFieldSetsFound} check rejects as fatal. This is
     * independent of query form - it happens inside {@code getSubQueryDateRanges}, before any query-specific plan is built - so it is covered once rather than
     * once per query form. A third, fully indexed day follows so the merged range's boundaries are asserted rather than inferred from a whole-range hole.
     */
    @Test
    void holesAdjacentAcrossDatatypes() throws Exception {
        writeEvent("20260701", DATATYPE_A, 1);
        writeEvent("20260702", DATATYPE_B, 2);
        writeEvent("20260703", DATATYPE_A, 3);
        writeHole("20260701", DATATYPE_A, FIELD_A);
        writeFullIndex("20260701", DATATYPE_B, FIELD_A);
        writeFullIndex("20260702", DATATYPE_A, FIELD_A);
        writeHole("20260702", DATATYPE_B, FIELD_A);
        writeFullIndex("20260703", DATATYPE_A, FIELD_A);

        givenDate("20260701", "20260703");
        givenQuery(SINGLE_TERM_QUERY);
        // Jul 1 and Jul 2 merge into one entirely unindexed sub-range, so both their events are valid hits that are missed without full table scan.
        expectResultCount(1);

        planAndExecuteQuery();

        // @formatter:off
        assertPartition(
                        range(date("20260701"), endOfDay("20260702"), FIELD_A),
                        range(date("20260703"), date("20260703")));
        assertPlans(
                        plan("20260701", "(_Eval_ = true) && (FIELD_A == 'value-a')"),
                        plan("20260702", "(_Eval_ = true) && (FIELD_A == 'value-a')"),
                        plan("20260703", "FIELD_A == 'value-a'"));
        // @formatter:on
    }

    /**
     * Holes a full day apart across datatypes are not adjacent, so they stay separate with an indexed sub-range between them.
     */
    @Test
    void holesOneDayApartAcrossDatatypes() throws Exception {
        writeEvent("20260701", DATATYPE_A, 1);
        writeEvent("20260702", DATATYPE_A, 2);
        writeEvent("20260703", DATATYPE_B, 3);
        writeHole("20260701", DATATYPE_A, FIELD_A);
        writeFullIndex("20260701", DATATYPE_B, FIELD_A);
        writeFullIndex("20260702", DATATYPE_A, FIELD_A);
        writeFullIndex("20260702", DATATYPE_B, FIELD_A);
        writeFullIndex("20260703", DATATYPE_A, FIELD_A);
        writeHole("20260703", DATATYPE_B, FIELD_A);

        givenDate("20260701", "20260703");
        givenQuery(SINGLE_TERM_QUERY);
        // Jul 1's and Jul 3's events fall in entirely unindexed sub-ranges and are missed without full table scan.
        expectResultCount(1);

        planAndExecuteQuery();

        // @formatter:off
        assertPartition(
                        range(date("20260701"), endOfDay("20260701"), FIELD_A),
                        range(date("20260702"), endOfDay("20260702")),
                        range(date("20260703"), date("20260703"), FIELD_A));
        assertPlans(
                        plan("20260701", "(_Eval_ = true) && (FIELD_A == 'value-a')"),
                        plan("20260702", "FIELD_A == 'value-a'"),
                        plan("20260703", "(_Eval_ = true) && (FIELD_A == 'value-a')"));
        // @formatter:on
    }

    @Test
    void datatypeFilterExcludesHoleDatatype() throws Exception {
        writeEvent("20260701", DATATYPE_A, 1);
        writeEvent("20260701", DATATYPE_B, 2);
        writeFullIndex("20260701", DATATYPE_A, FIELD_A);
        writeHole("20260701", DATATYPE_B, FIELD_A);

        givenDate("20260701");
        givenQuery(SINGLE_TERM_QUERY);
        givenParameter(QueryParameters.DATATYPE_FILTER_SET, DATATYPE_A);
        expectResultCount(1);

        planAndExecuteQuery();

        // the hole is in datatype-b, which is filtered out entirely, so no partitioning is needed and no hits are missed
        // @formatter:off
        assertPartition(
                        range(date("20260701"), date("20260701")));
        assertPlans(
                        plan("20260701", "FIELD_A == 'value-a'"));
        // @formatter:on
    }

    @Test
    void datatypeFilterIncludesHoleDatatype() throws Exception {
        writeEvent("20260701", DATATYPE_A, 1);
        writeEvent("20260701", DATATYPE_B, 2);
        writeFullIndex("20260701", DATATYPE_A, FIELD_A);
        writeFullIndex("20260701", DATATYPE_B, FIELD_A);
        writeEvent("20260702", DATATYPE_A, 3);
        writeEvent("20260702", DATATYPE_B, 4);
        writeFullIndex("20260702", DATATYPE_A, FIELD_A);
        writeHole("20260702", DATATYPE_B, FIELD_A);

        givenDate("20260701", "20260702");
        givenQuery(SINGLE_TERM_QUERY);
        givenParameter(QueryParameters.DATATYPE_FILTER_SET, DATATYPE_B);
        // Jul 2's datatype-b event is a valid hit that is missed: the filtered-in datatype has a hole that day and full table scan is disabled.
        expectResultCount(1);

        planAndExecuteQuery();

        // @formatter:off
        assertPartition(
                        range(date("20260701"), endOfDay("20260701")),
                        range(date("20260702"), date("20260702"), FIELD_A));
        assertPlans(
                        plan("20260701", "FIELD_A == 'value-a'"),
                        plan("20260702", "(_Eval_ = true) && (FIELD_A == 'value-a')"));
        // @formatter:on
    }

    @Test
    void thresholdExactlyMet() throws Exception {
        writeEvent("20260701", DATATYPE_A, 1);
        // ratio == threshold is NOT a hole (strict '<' comparison)
        ingest.writeMetadataCounts(FIELD_A, DATATYPE_A, "20260701", 20L, 18L);
        logic.setIndexFieldHoleMinThreshold(0.9);

        givenDate("20260701");
        givenQuery(SINGLE_TERM_QUERY);
        expectResultCount(1);

        planAndExecuteQuery();

        // @formatter:off
        assertPartition(
                        range(date("20260701"), date("20260701")));
        assertPlans(
                        plan("20260701", "FIELD_A == 'value-a'"));
        // @formatter:on
    }

    @Test
    void thresholdZeroFrequency() throws Exception {
        writeEvent("20260701", DATATYPE_A, 1);
        // a 0/0 ratio must not be treated as a hole
        ingest.writeMetadataCounts(FIELD_A, DATATYPE_A, "20260701", 0L, 0L);

        givenDate("20260701");
        givenQuery(SINGLE_TERM_QUERY);
        expectResultCount(1);

        planAndExecuteQuery();

        // @formatter:off
        assertPartition(
                        range(date("20260701"), date("20260701")));
        assertPlans(
                        plan("20260701", "FIELD_A == 'value-a'"));
        // @formatter:on
    }

    @Test
    void allSubPlansFail() throws Exception {
        writeEvent("20260701", DATATYPE_A, 1);
        writeHole("20260701", DATATYPE_A, FIELD_A);

        givenDate("20260701");
        givenQuery(SINGLE_TERM_QUERY);

        // the entire (single-day) query range is a hole, so the sole sub-plan fails without full table scan and the whole query throws
        assertThrows(DatawaveQueryException.class, this::planAndExecuteQuery);
    }

    @Test
    void oneSubPlanFailsOthersSucceed() throws Exception {
        writeEvent("20260701", DATATYPE_A, 1);
        writeFullIndex("20260701", DATATYPE_A, FIELD_A);
        writeEvent("20260702", DATATYPE_A, 2);
        writeHole("20260702", DATATYPE_A, FIELD_A);

        givenDate("20260701", "20260702");
        givenQuery(SINGLE_TERM_QUERY);
        // Jul 2's event is a valid hit that is missed: its sub-range is entirely unindexed and fails without full table scan. Since not every
        // sub-plan failed, the query still returns Jul 1's result rather than throwing.
        expectResultCount(1);

        planAndExecuteQuery();

        // @formatter:off
        assertPartition(
                        range(date("20260701"), endOfDay("20260701")),
                        range(date("20260702"), date("20260702"), FIELD_A));
        assertPlans(
                        plan("20260701", "FIELD_A == 'value-a'"),
                        plan("20260702", "(_Eval_ = true) && (FIELD_A == 'value-a')"));
        // @formatter:on
    }
}
