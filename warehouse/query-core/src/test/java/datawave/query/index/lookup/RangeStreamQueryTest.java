package datawave.query.index.lookup;

import static datawave.core.query.jexl.visitors.JexlStringBuildingVisitor.buildQuery;
import static datawave.query.index.lookup.RangeStreamQueryTest.TERM_CONTEXT.ANCHOR;
import static datawave.query.index.lookup.RangeStreamQueryTest.TERM_CONTEXT.ANCHOR_INTERSECT;
import static datawave.query.index.lookup.RangeStreamQueryTest.TERM_CONTEXT.ANCHOR_UNION;
import static datawave.query.index.lookup.RangeStreamQueryTest.TERM_CONTEXT.DELAYED;
import static datawave.query.index.lookup.RangeStreamQueryTest.TERM_CONTEXT.DELAYED_INTERSECT;
import static datawave.query.index.lookup.RangeStreamQueryTest.TERM_CONTEXT.DELAYED_UNION;
import static datawave.util.TableName.SHARD_INDEX;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.common.test.integration.IntegrationTest;
import datawave.core.query.jexl.JexlNodeFactory;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.Type;
import datawave.ingest.protobuf.Uid;
import datawave.query.CloseableIterable;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.TreeEqualityVisitor;
import datawave.query.planner.QueryPlan;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.MockMetadataHelper;

/**
 * Integration test for asserting correct query plans coming off the RangeStream
 * <p>
 * Underlying index streams are assumed to have data based on the field
 */
@Category(IntegrationTest.class)
public class RangeStreamQueryTest {

    private static final Logger log = Logger.getLogger(RangeStreamQueryTest.class);

    private static InMemoryInstance instance = new InMemoryInstance(RangeStreamQueryTest.class.toString());
    private static AccumuloClient connector;
    private ShardQueryConfiguration config;

    private MockMetadataHelper helper;
    private Multimap<String,Type<?>> fieldToDataType;

    private ASTJexlScript script;
    private RangeStream rangeStream;
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

    private static int count = 0;

    // is the query a top level union or a top level intersection?
    public enum QUERY_CONTEXT {
        UNION, INTERSECTION
    }

    // is the term an anchor or delayed? is it a single term or a top level union/intersection?
    public enum TERM_CONTEXT {
        ANCHOR, ANCHOR_INTERSECT, ANCHOR_UNION, DELAYED, DELAYED_INTERSECT, DELAYED_UNION
    }

    @BeforeClass
    public static void setupAccumulo() throws Exception {
        connector = new InMemoryAccumuloClient("root", instance);
        connector.tableOperations().create(SHARD_INDEX);

        BatchWriter bw = connector.createBatchWriter(SHARD_INDEX,
                        new BatchWriterConfig().setMaxLatency(10, TimeUnit.SECONDS).setMaxMemory(100000L).setMaxWriteThreads(1));

        Value shardValue = buildShardRange();
        Value docValue = buildDocRange("a.b.c");
        Value docValue2 = buildDocRange("x.y.z");

        Text cq = new Text("20200101_7\0datatype");

        Mutation m = new Mutation("shard");
        m.put(new Text("FOO"), cq, shardValue);
        m.put(new Text("FOO2"), cq, shardValue);
        m.put(new Text("FOO3"), cq, shardValue);
        bw.addMutation(m);

        m = new Mutation("uid");
        m.put(new Text("FOO"), cq, docValue);
        m.put(new Text("FOO2"), cq, docValue);
        m.put(new Text("FOO3"), cq, docValue);
        bw.addMutation(m);

        m = new Mutation("uid2");
        m.put(new Text("FOO"), cq, docValue2);
        m.put(new Text("FOO2"), cq, docValue2);
        m.put(new Text("FOO3"), cq, docValue2);
        bw.addMutation(m);

        bw.flush();
        bw.close();
    }

    private static Value buildDocRange(String uid) {
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.addUID(uid);
        builder.setIGNORE(false);
        builder.setCOUNT(1);
        Uid.List list = builder.build();
        return new Value(list.toByteArray());
    }

    private static Value buildShardRange() {
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.setIGNORE(true);
        builder.setCOUNT(31);
        Uid.List list = builder.build();
        return new Value(list.toByteArray());
    }

    @Before
    public void setupTest() throws ParseException {
        helper = new MockMetadataHelper();
        helper.setIndexedFields(Sets.newHashSet("FOO", "FOO2", "FOO3"));

        fieldToDataType = HashMultimap.create();
        fieldToDataType.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));
        fieldToDataType.putAll("FOO2", Sets.newHashSet(new LcNoDiacriticsType()));
        fieldToDataType.putAll("FOO3", Sets.newHashSet(new LcNoDiacriticsType()));

        config = new ShardQueryConfiguration();
        config.setClient(connector);
        config.setBeginDate(sdf.parse("20200101"));
        config.setEndDate(sdf.parse("20200102"));
        config.setDatatypeFilter(Collections.singleton("datatype"));
        config.setQueryFieldsDatatypes(fieldToDataType);
        config.setIndexedFields(fieldToDataType);
    }

    @AfterClass
    public static void afterClass() {
        log.info("ran " + count + " queries");
    }

    @Test
    public void testSimpleQueries() throws Exception {
        test("FOO3 == 'shard'", ANCHOR);
        test("FOO3 == 'uid'", ANCHOR);
    }

    @Test
    public void testQueriesWithFilterIsNulls() throws Exception {
        test("FOO == null", DELAYED);
        test("(FOO == null && FOO2 == null)", DELAYED_INTERSECT);
        test("(FOO == null || FOO2 == null)", DELAYED_UNION);
    }

    @Test
    public void testQueriesWithFilterIsNotNulls() throws Exception {
        test("!(FOO == null)", DELAYED);
        test("(!(FOO == null) && !(FOO2 == null))", DELAYED_INTERSECT);
        test("(!(FOO == null) || !(FOO2 == null))", DELAYED_UNION);
    }

    @Test
    public void testQueriesWithFilterIncludeRegex() throws Exception {
        test("filter:includeRegex(FOO, 'ba.*')", DELAYED);
        test("(filter:includeRegex(FOO, 'ba.*') && filter:includeRegex(FOO2, 'ba.*'))", DELAYED_INTERSECT);
        test("(filter:includeRegex(FOO, 'ba.*') || filter:includeRegex(FOO2, 'ba.*'))", DELAYED_UNION);
    }

    @Test
    public void testQueriesWithFilterExcludeRegex() throws Exception {
        test("filter:excludeRegex(FOO, 'ba.*')", DELAYED);
        test("(filter:excludeRegex(FOO, 'ba.*') && filter:excludeRegex(FOO2, 'ba.*'))", DELAYED_INTERSECT);
        test("(filter:excludeRegex(FOO, 'ba.*') || filter:excludeRegex(FOO2, 'ba.*'))", DELAYED_UNION);
    }

    @Test
    public void testQueriesWithExceededValueMarkers() throws Exception {
        // the range stream creates a stream of day shards for these terms, thus they are treated as anchor terms
        test("((_Value_ = true) && (FOO =~ 'ba.*'))", ANCHOR);
        test("(((_Value_ = true) && (FOO =~ 'ba.*')) && ((_Value_ = true) && (FOO2 =~ 'ba.*')))", ANCHOR_INTERSECT);
        test("(((_Value_ = true) && (FOO =~ 'ba.*')) || ((_Value_ = true) && (FOO2 =~ 'ba.*')))", ANCHOR_UNION);
    }

    @Test
    public void testQueriesWithExceededTermMarkers() throws Exception {
        test("((_Term_ = true) && (FOO =~ 'ba.*'))", DELAYED);
        test("(((_Term_ = true) && (FOO =~ 'ba.*')) && ((_Term_ = true) && (FOO2 =~ 'ba.*')))", DELAYED_INTERSECT);
        test("(((_Term_ = true) && (FOO =~ 'ba.*')) || ((_Term_ = true) && (FOO2 =~ 'ba.*')))", DELAYED_UNION);
    }

    @Test
    public void testQueriesWithDelayedRegexNodes() throws Exception {
        test("((_Delayed_ = true) && (FOO =~ 'ba.*'))", DELAYED);
        test("(((_Delayed_ = true) && (FOO =~ 'ba.*')) && ((_Delayed_ = true) && (FOO2 =~ 'ba.*')))", DELAYED_INTERSECT);
        test("(((_Delayed_ = true) && (FOO =~ 'ba.*')) || ((_Delayed_ = true) && (FOO2 =~ 'ba.*')))", DELAYED_UNION);
    }

    @Test
    public void testQueriesWithBoundedRangeMarkers() throws Exception {
        test("((_Bounded_ = true) && (FOO > '3' && FOO < 7))", DELAYED);
        test("(((_Bounded_ = true) && (FOO > '3' && FOO < 7)) && ((_Bounded_ = true) && (FOO2 > '3' && FOO2 < 7)))", DELAYED_INTERSECT);
        test("(((_Bounded_ = true) && (FOO > '3' && FOO < 7)) || ((_Bounded_ = true) && (FOO2 > '3' && FOO2 < 7)))", DELAYED_UNION);
    }

    @Test
    public void testQueriesWithMixedDelays() throws Exception {
        test("(!(FOO == null) && filter:includeRegex(FOO, 'ba.*'))", DELAYED_INTERSECT);
        test("(!(FOO == null) && filter:includeRegex(FOO, 'ba.*') && ((_Delayed_ = true) && (FOO =~ 'ba.*')))", DELAYED_INTERSECT);
        test("(!(FOO == null) || filter:includeRegex(FOO, 'ba.*'))", DELAYED_UNION);
        test("(!(FOO == null) || filter:includeRegex(FOO, 'ba.*') || ((_Delayed_ = true) && (FOO =~ 'ba.*')))", DELAYED_UNION);

        // additional test case with value exceeded
        test("(!(FOO == null) && filter:includeRegex(FOO, 'ba.*') && ((_Value_ = true) && (FOO =~ 'ba.*')))", ANCHOR_INTERSECT);
        test("(!(FOO == null) || filter:includeRegex(FOO, 'ba.*') || ((_Value_ = true) && (FOO =~ 'ba.*')))", DELAYED_UNION);
    }

    @Test
    public void testQueriesWithMixedMarkers() throws Exception {
        // intersection of delayed markers is all delayed
        test("(((_Term_ = true) && (FOO =~ 'ba.*')) && ((_Delayed_ = true) && (FOO2 =~ 'ba.*')))", DELAYED_INTERSECT);
        // presence of value exceeded makes this an anchor
        test("(((_Value_ = true) && (FOO =~ 'ba.*')) && ((_Delayed_ = true) && (FOO2 =~ 'ba.*')))", ANCHOR_INTERSECT);

        // value exceeded or not, union is always delayed if at least one term is delayed
        test("(((_Term_ = true) && (FOO =~ 'ba.*')) || ((_Delayed_ = true) && (FOO2 =~ 'ba.*')))", DELAYED_UNION);
        test("(((_Value_ = true) && (FOO =~ 'ba.*')) || ((_Delayed_ = true) && (FOO2 =~ 'ba.*')))", DELAYED_UNION);
    }

    @Test
    public void testQueriesWithMixedMarkersAndAnchors() throws Exception {
        // each intersection should pivot on the anchor term and add in the delayed markers
        test("(FOO3 == 'shard' && !(FOO == null) && filter:includeRegex(FOO, 'ba.*'))", ANCHOR_INTERSECT);
        test("(FOO3 == 'shard' && !(FOO == null) && filter:includeRegex(FOO, 'ba.*') && ((_Delayed_ = true) && (FOO2 =~ 'ba.*')))", ANCHOR_INTERSECT);
        test("(FOO3 == 'shard' && ((_Value_ = true) && (FOO =~ 'ba.*')) && ((_Delayed_ = true) && (FOO2 =~ 'ba.*')))", ANCHOR_INTERSECT);

        // each whole union should be added into queries with top level intersections
        test("(FOO3 == 'shard' || !(FOO == null) || filter:includeRegex(FOO, 'ba.*'))", DELAYED_UNION);
        test("(FOO3 == 'shard' || !(FOO == null) || filter:includeRegex(FOO, 'ba.*') || ((_Delayed_ = true) && (FOO2 =~ 'ba.*')))", DELAYED_UNION);
        test("(FOO3 == 'shard' || ((_Value_ = true) && (FOO =~ 'ba.*')) || ((_Delayed_ = true) && (FOO2 =~ 'ba.*')))", DELAYED_UNION);
    }

    private void test(String append, TERM_CONTEXT termContext) throws Exception {
        testIntersections(append, termContext);
        testUnions(append, termContext);
    }

    private void testIntersections(String append, TERM_CONTEXT termContext) throws Exception {
        String[] queries = {
                // single terms
                "FOO == 'shard'", "FOO == 'uid'",
                // two terms: shard-shard, shard-uid, uid-shard, uid-uid
                "FOO == 'shard' && FOO2 == 'shard'", "FOO == 'shard' && FOO2 == 'uid'", "FOO == 'uid' && FOO2 == 'shard'", "FOO == 'uid' && FOO2 == 'uid'",
                // two terms, one side is a nested union
                "FOO == 'shard' && (FOO2 == 'shard' || FOO3 == 'shard')", "FOO == 'shard' && (FOO2 == 'shard' || FOO3 == 'uid')",
                "FOO == 'shard' && (FOO2 == 'uid' || FOO3 == 'shard')", "FOO == 'shard' && (FOO2 == 'uid' || FOO3 == 'uid')",
                "FOO == 'uid' && (FOO2 == 'shard' || FOO3 == 'shard')", "FOO == 'uid' && (FOO2 == 'shard' || FOO3 == 'uid')",
                "FOO == 'uid' && (FOO2 == 'uid' || FOO3 == 'shard')", "FOO == 'uid' && (FOO2 == 'uid' || FOO3 == 'uid')",
                // three terms, one term is a nested union
                "FOO == 'shard' && FOO2 == 'shard' && (FOO2 == 'shard' || FOO3 == 'shard')",
                "FOO == 'shard' && FOO2 == 'shard' && (FOO2 == 'shard' || FOO3 == 'uid')",
                "FOO == 'shard' && FOO2 == 'shard' && (FOO2 == 'uid' || FOO3 == 'shard')",
                "FOO == 'shard' && FOO2 == 'shard' && (FOO2 == 'uid' || FOO3 == 'uid')",
                "FOO == 'uid' && FOO2 == 'uid' && (FOO2 == 'shard' || FOO3 == 'shard')", "FOO == 'uid' && FOO2 == 'uid' && (FOO2 == 'shard' || FOO3 == 'uid')",
                "FOO == 'uid' && FOO2 == 'uid' && (FOO2 == 'uid' || FOO3 == 'shard')", "FOO == 'uid' && FOO2 == 'uid' && (FOO2 == 'uid' || FOO3 == 'uid')",
                // three terms, two terms are a nested union
                "FOO == 'shard' && (FOO == 'shard' || FOO2 == 'shard') && (FOO2 == 'shard' || FOO3 == 'shard')",
                "FOO == 'shard' && (FOO == 'shard' || FOO2 == 'shard') && (FOO2 == 'shard' || FOO3 == 'uid')",
                "FOO == 'shard' && (FOO == 'shard' || FOO2 == 'shard') && (FOO2 == 'uid' || FOO3 == 'shard')",
                "FOO == 'shard' && (FOO == 'shard' || FOO2 == 'shard') && (FOO2 == 'uid' || FOO3 == 'uid')",
                "FOO == 'uid' && (FOO == 'uid' || FOO2 == 'uid') && (FOO2 == 'shard' || FOO3 == 'shard')",
                "FOO == 'uid' && (FOO == 'uid' || FOO2 == 'uid') && (FOO2 == 'shard' || FOO3 == 'uid')",
                "FOO == 'uid' && (FOO == 'uid' || FOO2 == 'uid') && (FOO2 == 'uid' || FOO3 == 'shard')",
                "FOO == 'uid' && (FOO == 'uid' || FOO2 == 'uid') && (FOO2 == 'uid' || FOO3 == 'uid')",
                // three terms, all terms nested unions
                "(FOO == 'shard' || FOO3 == 'shard') && (FOO == 'shard' || FOO2 == 'shard') && (FOO2 == 'shard' || FOO3 == 'shard')",
                "(FOO == 'shard' || FOO3 == 'shard') && (FOO == 'shard' || FOO2 == 'shard') && (FOO2 == 'shard' || FOO3 == 'uid')",
                "(FOO == 'shard' || FOO3 == 'shard') && (FOO == 'shard' || FOO2 == 'shard') && (FOO2 == 'uid' || FOO3 == 'shard')",
                "(FOO == 'shard' || FOO3 == 'shard') && (FOO == 'shard' || FOO2 == 'shard') && (FOO2 == 'uid' || FOO3 == 'uid')",
                "(FOO == 'uid' || FOO3 == 'uid') && (FOO == 'uid' || FOO2 == 'uid') && (FOO2 == 'shard' || FOO3 == 'shard')",
                "(FOO == 'uid' || FOO3 == 'uid') && (FOO == 'uid' || FOO2 == 'uid') && (FOO2 == 'shard' || FOO3 == 'uid')",
                "(FOO == 'uid' || FOO3 == 'uid') && (FOO == 'uid' || FOO2 == 'uid') && (FOO2 == 'uid' || FOO3 == 'shard')",
                "(FOO == 'uid' || FOO3 == 'uid') && (FOO == 'uid' || FOO2 == 'uid') && (FOO2 == 'uid' || FOO3 == 'uid')"};

        for (String query : queries) {
            String appended = query + " && " + append;
            switch (termContext) {
                case ANCHOR:
                case ANCHOR_INTERSECT:
                case ANCHOR_UNION:
                case DELAYED:
                case DELAYED_INTERSECT:
                case DELAYED_UNION:
                    test(appended, appended, QUERY_CONTEXT.INTERSECTION, termContext);
                    break;
                default:
                    throw new IllegalStateException("unknown term context: " + termContext);
            }
        }
    }

    private void testUnions(String append, TERM_CONTEXT termContext) throws Exception {
        String[] queries = {
                // single terms
                "FOO == 'shard'", "FOO == 'uid'",
                // two terms: shard-shard, shard-uid, uid-shard, uid-uid
                "FOO == 'shard' || FOO2 == 'shard'", "FOO == 'shard' || FOO2 == 'uid'", "FOO == 'uid' || FOO2 == 'shard'", "FOO == 'uid' || FOO2 == 'uid'",
                // two terms, one side is a nested intersection
                "FOO == 'shard' || (FOO2 == 'shard' && FOO3 == 'shard')", "FOO == 'shard' || (FOO2 == 'shard' && FOO3 == 'uid')",
                "FOO == 'shard' || (FOO2 == 'uid' && FOO3 == 'shard')", "FOO == 'shard' || (FOO2 == 'uid' && FOO3 == 'uid')",
                "FOO == 'uid' || (FOO2 == 'shard' && FOO3 == 'shard')", "FOO == 'uid' || (FOO2 == 'shard' && FOO3 == 'uid')",
                "FOO == 'uid' || (FOO2 == 'uid' && FOO3 == 'shard')", "FOO == 'uid' || (FOO2 == 'uid' && FOO3 == 'uid')",
                // three terms, one term is a nested intersection
                "FOO == 'shard' || FOO2 == 'shard' || (FOO2 == 'shard' && FOO3 == 'shard')",
                "FOO == 'shard' || FOO2 == 'shard' || (FOO2 == 'shard' && FOO3 == 'uid')",
                "FOO == 'shard' || FOO2 == 'shard' || (FOO2 == 'uid' && FOO3 == 'shard')",
                "FOO == 'shard' || FOO2 == 'shard' || (FOO2 == 'uid' && FOO3 == 'uid')",
                "FOO == 'uid' || FOO2 == 'uid' || (FOO2 == 'shard' && FOO3 == 'shard')", "FOO == 'uid' || FOO2 == 'uid' || (FOO2 == 'shard' && FOO3 == 'uid')",
                "FOO == 'uid' || FOO2 == 'uid' || (FOO2 == 'uid' && FOO3 == 'shard')", "FOO == 'uid' || FOO2 == 'uid' || (FOO2 == 'uid' && FOO3 == 'uid')",
                // three terms, two terms are a nested intersection
                "FOO == 'shard' || (FOO == 'shard' && FOO2 == 'shard') || (FOO2 == 'shard' && FOO3 == 'shard')",
                "FOO == 'shard' || (FOO == 'shard' && FOO2 == 'shard') || (FOO2 == 'shard' && FOO3 == 'uid')",
                "FOO == 'shard' || (FOO == 'shard' && FOO2 == 'shard') || (FOO2 == 'uid' && FOO3 == 'shard')",
                "FOO == 'shard' || (FOO == 'shard' && FOO2 == 'shard') || (FOO2 == 'uid' && FOO3 == 'uid')",
                "FOO == 'uid' || (FOO == 'uid' && FOO2 == 'uid') || (FOO2 == 'shard' && FOO3 == 'shard')",
                "FOO == 'uid' || (FOO == 'uid' && FOO2 == 'uid') || (FOO2 == 'shard' && FOO3 == 'uid')",
                "FOO == 'uid' || (FOO == 'uid' && FOO2 == 'uid') || (FOO2 == 'uid' && FOO3 == 'shard')",
                "FOO == 'uid' || (FOO == 'uid' && FOO2 == 'uid') || (FOO2 == 'uid' && FOO3 == 'uid')",
                // three terms, all terms nested intersection
                "(FOO == 'shard' && FOO3 == 'shard') || (FOO == 'shard' && FOO2 == 'shard') || (FOO2 == 'shard' && FOO3 == 'shard')",
                "(FOO == 'shard' && FOO3 == 'shard') || (FOO == 'shard' && FOO2 == 'shard') || (FOO2 == 'shard' && FOO3 == 'uid')",
                "(FOO == 'shard' && FOO3 == 'shard') || (FOO == 'shard' && FOO2 == 'shard') || (FOO2 == 'uid' && FOO3 == 'shard')",
                "(FOO == 'shard' && FOO3 == 'shard') || (FOO == 'shard' && FOO2 == 'shard') || (FOO2 == 'uid' && FOO3 == 'uid')",
                "(FOO == 'uid' && FOO3 == 'uid') || (FOO == 'uid' && FOO2 == 'uid') || (FOO2 == 'shard' && FOO3 == 'shard')",
                "(FOO == 'uid' && FOO3 == 'uid') || (FOO == 'uid' && FOO2 == 'uid') || (FOO2 == 'shard' && FOO3 == 'uid')",
                "(FOO == 'uid' && FOO3 == 'uid') || (FOO == 'uid' && FOO2 == 'uid') || (FOO2 == 'uid' && FOO3 == 'shard')",
                "(FOO == 'uid' && FOO3 == 'uid') || (FOO == 'uid' && FOO2 == 'uid') || (FOO2 == 'uid' && FOO3 == 'uid')"};

        for (String query : queries) {
            String appended = query + " || " + append;
            switch (termContext) {
                case ANCHOR:
                case ANCHOR_UNION:
                case ANCHOR_INTERSECT:
                    test(appended, appended, QUERY_CONTEXT.UNION, termContext);
                    break;
                case DELAYED:
                case DELAYED_UNION:
                case DELAYED_INTERSECT:
                    // top level unions with a delayed term do not produce any query plans
                    test(appended, null, QUERY_CONTEXT.UNION, termContext);
                    break;
                default:
                    throw new IllegalStateException("unknown term context: " + termContext);
            }
        }
    }

    private void test(String query, String expected, QUERY_CONTEXT queryContext, TERM_CONTEXT termContext) throws Exception {

        // Run a standard limited-scanner range stream.
        count++;
        ScannerFactory scannerFactory = new ScannerFactory(config);
        rangeStream = new RangeStream(config, scannerFactory, helper);
        rangeStream.setLimitScanners(true);

        script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        CloseableIterable<QueryPlan> queryPlans = rangeStream.streamPlans(script);

        // a stream context of VARIABLE will change to PRESENT for intersections and ABSENT for unions
        Iterator<QueryPlan> queryPlanIter = queryPlans.iterator();

        // check for a top level union and a delayed term. These queries are not executable
        if (queryContext == QUERY_CONTEXT.UNION
                        && (termContext.equals(DELAYED) || termContext.equals(DELAYED_UNION) || termContext.equals(DELAYED_INTERSECT))) {
            assertFalse("top level union and delayed term should have produced no query plans, but got one for query " + query, queryPlanIter.hasNext());
            queryPlans.close();
            rangeStream.close();
            return;
        } else if (queryContext == QUERY_CONTEXT.INTERSECTION && rangeStream.context() != IndexStream.StreamContext.PRESENT) {
            queryPlans.close();
            rangeStream.close();
            fail("RangeStream context was: " + rangeStream.context() + " for query: " + query);
        }

        assertTrue(query + " did not produce any query plans", queryPlanIter.hasNext());

        QueryPlan plan = queryPlanIter.next();
        assertNotNull("expected a query plan, but was null for query: " + query, plan);

        ASTJexlScript plannedScript = JexlNodeFactory.createScript(plan.getQueryTree());
        ASTJexlScript expectedScript = JexlASTHelper.parseAndFlattenJexlQuery(expected);
        if (!TreeEqualityVisitor.isEqual(expectedScript, plannedScript)) {
            fail("Expected [" + buildQuery(expectedScript) + "] but got [" + buildQuery(plannedScript) + "]");
        }
        queryPlans.close();
        rangeStream.close();
    }
}
