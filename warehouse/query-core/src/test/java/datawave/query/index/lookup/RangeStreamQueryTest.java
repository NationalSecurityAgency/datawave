package datawave.query.index.lookup;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.common.test.integration.IntegrationTest;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.Type;
import datawave.ingest.protobuf.Uid;
import datawave.query.CloseableIterable;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.TreeEqualityVisitor;
import datawave.query.planner.QueryPlan;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.MockMetadataHelper;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static datawave.util.TableName.SHARD_INDEX;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Integration test for asserting correct query plans coming off the RangeStream
 * <p>
 * Underlying index streams are assumed to have data based on the field
 */
@Category(IntegrationTest.class)
public class RangeStreamQueryTest {
    
    private static InMemoryInstance instance = new InMemoryInstance(RangeStreamQueryTest.class.toString());
    private static Connector connector;
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
    
    // is the term added to the query an anchor term or is it delayed?
    public enum TERM_CONTEXT {
        ANCHOR, DELAYED
    }
    
    @BeforeClass
    public static void setupAccumulo() throws Exception {
        connector = instance.getConnector("", new PasswordToken(new byte[0]));
        connector.tableOperations().create(SHARD_INDEX);
        
        BatchWriter bw = connector.createBatchWriter(SHARD_INDEX, new BatchWriterConfig().setMaxLatency(10, TimeUnit.SECONDS).setMaxMemory(100000L)
                        .setMaxWriteThreads(1));
        
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
        config.setConnector(connector);
        config.setBeginDate(sdf.parse("20200101"));
        config.setEndDate(sdf.parse("20200102"));
        config.setDatatypeFilter(Collections.singleton("datatype"));
        config.setQueryFieldsDatatypes(fieldToDataType);
        config.setIndexedFields(fieldToDataType);
        config.setShardsPerDayThreshold(2);
    }
    
    @AfterClass
    public static void afterClass() {
        System.out.println("Queries run: " + count);
    }
    
    @Test
    public void testSimpleQueries() throws Exception {
        test("FOO3 == 'shard'");
        test("FOO3 == 'uid'");
    }
    
    @Test
    public void testQueriesWithFilterIsNulls() throws Exception {
        test("FOO == null", TERM_CONTEXT.DELAYED);
        test("(FOO == null && FOO2 == null)", TERM_CONTEXT.DELAYED);
        test("(FOO == null || FOO2 == null)", TERM_CONTEXT.DELAYED);
    }
    
    @Test
    public void testQueriesWithFilterIsNotNulls() throws Exception {
        test("!(FOO == null)", TERM_CONTEXT.DELAYED);
        test("(!(FOO == null) && !(FOO2 == null))", TERM_CONTEXT.DELAYED);
        test("(!(FOO == null) || !(FOO2 == null))", TERM_CONTEXT.DELAYED);
    }
    
    @Test
    public void testQueriesWithFilterIncludeRegex() throws Exception {
        test("filter:includeRegex(FOO, 'ba.*')", TERM_CONTEXT.DELAYED);
        test("(filter:includeRegex(FOO, 'ba.*') && filter:includeRegex(FOO2, 'ba.*'))", TERM_CONTEXT.DELAYED);
        test("(filter:includeRegex(FOO, 'ba.*') || filter:includeRegex(FOO2, 'ba.*'))", TERM_CONTEXT.DELAYED);
    }
    
    @Test
    public void testQueriesWithFilterExcludeRegex() throws Exception {
        test("filter:excludeRegex(FOO, 'ba.*')", TERM_CONTEXT.DELAYED);
        test("(filter:excludeRegex(FOO, 'ba.*') && filter:excludeRegex(FOO2, 'ba.*'))", TERM_CONTEXT.DELAYED);
        test("(filter:excludeRegex(FOO, 'ba.*') || filter:excludeRegex(FOO2, 'ba.*'))", TERM_CONTEXT.DELAYED);
    }
    
    @Test
    public void testQueriesWithExceededValueMarkers() throws Exception {
        // the range stream creates a stream of day shards for these terms, thus they are treated as anchor terms
        test("((_Value_ = true) && (FOO =~ 'ba.*'))");
        test("(((_Value_ = true) && (FOO =~ 'ba.*')) && ((_Value_ = true) && (FOO2 =~ 'ba.*')))");
        test("(((_Value_ = true) && (FOO =~ 'ba.*')) || ((_Value_ = true) && (FOO2 =~ 'ba.*')))");
    }
    
    @Test
    public void testQueriesWithExceededTermMarkers() throws Exception {
        test("((_Term_ = true) && (FOO =~ 'ba.*'))", TERM_CONTEXT.DELAYED);
        test("(((_Term_ = true) && (FOO =~ 'ba.*')) && ((_Term_ = true) && (FOO2 =~ 'ba.*')))", TERM_CONTEXT.DELAYED);
        test("(((_Term_ = true) && (FOO =~ 'ba.*')) || ((_Term_ = true) && (FOO2 =~ 'ba.*')))", TERM_CONTEXT.DELAYED);
    }
    
    @Test
    public void testQueriesWithDelayedRegexNodes() throws Exception {
        test("((_Delayed_ = true) && (FOO =~ 'ba.*'))", TERM_CONTEXT.DELAYED);
        test("(((_Delayed_ = true) && (FOO =~ 'ba.*')) && ((_Delayed_ = true) && (FOO2 =~ 'ba.*')))", TERM_CONTEXT.DELAYED);
        test("(((_Delayed_ = true) && (FOO =~ 'ba.*')) || ((_Delayed_ = true) && (FOO2 =~ 'ba.*')))", TERM_CONTEXT.DELAYED);
    }
    
    @Test
    public void testQueriesWithBoundedRangeMarkers() throws Exception {
        test("((_Bounded_ = true) && (FOO > '3' && FOO < 7))", TERM_CONTEXT.DELAYED);
        test("(((_Bounded_ = true) && (FOO > '3' && FOO < 7)) && ((_Bounded_ = true) && (FOO2 > '3' && FOO2 < 7)))", TERM_CONTEXT.DELAYED);
        test("(((_Bounded_ = true) && (FOO > '3' && FOO < 7)) || ((_Bounded_ = true) && (FOO2 > '3' && FOO2 < 7)))", TERM_CONTEXT.DELAYED);
    }
    
    @Test
    public void testQueriesWithMixedDelays() throws Exception {
        test("(!(FOO == null) && filter:includeRegex(FOO, 'ba.*'))", TERM_CONTEXT.DELAYED);
        test("(!(FOO == null) && filter:includeRegex(FOO, 'ba.*') && ((_Delayed_ = true) && (FOO =~ 'ba.*')))", TERM_CONTEXT.DELAYED);
        test("(!(FOO == null) || filter:includeRegex(FOO, 'ba.*'))", TERM_CONTEXT.DELAYED);
        test("(!(FOO == null) || filter:includeRegex(FOO, 'ba.*') || ((_Delayed_ = true) && (FOO =~ 'ba.*')))", TERM_CONTEXT.DELAYED);
    }
    
    @Test
    public void testQueriesWithMixedMarkers() throws Exception {
        test("(((_Value_ = true) && (FOO =~ 'ba.*')) && ((_Delayed_ = true) && (FOO2 =~ 'ba.*')))");
        test("(((_Value_ = true) && (FOO =~ 'ba.*')) || ((_Delayed_ = true) && (FOO2 =~ 'ba.*')))", TERM_CONTEXT.DELAYED);
    }
    
    @Test
    public void testQueriesWithMixedMarkersAndAnchors() throws Exception {
        // each intersection should pivot on the anchor term and add in the delayed markers
        test("(FOO3 == 'shard' && !(FOO == null) && filter:includeRegex(FOO, 'ba.*'))");
        test("(FOO3 == 'shard' && !(FOO == null) && filter:includeRegex(FOO, 'ba.*') && ((_Delayed_ = true) && (FOO2 =~ 'ba.*')))");
        test("(FOO3 == 'shard' && ((_Value_ = true) && (FOO =~ 'ba.*')) && ((_Delayed_ = true) && (FOO2 =~ 'ba.*')))");
        
        // each whole union should be added into queries with top level intersections
        test("(FOO3 == 'shard' || !(FOO == null) || filter:includeRegex(FOO, 'ba.*'))", TERM_CONTEXT.DELAYED);
        test("(FOO3 == 'shard' || !(FOO == null) || filter:includeRegex(FOO, 'ba.*') || ((_Delayed_ = true) && (FOO2 =~ 'ba.*')))", TERM_CONTEXT.DELAYED);
        test("(FOO3 == 'shard' || ((_Value_ = true) && (FOO =~ 'ba.*')) || ((_Delayed_ = true) && (FOO2 =~ 'ba.*')))", TERM_CONTEXT.DELAYED);
    }
    
    // default term context to ANCHOR
    private void test(String append) throws Exception {
        test(append, TERM_CONTEXT.ANCHOR);
    }
    
    private void test(String append, TERM_CONTEXT termContext) throws Exception {
        testIntersections(append, termContext);
        testUnions(append, termContext);
    }
    
    private void testIntersections(String append, TERM_CONTEXT termContext) throws Exception {
        String[] queries = {
                // single terms
                "FOO == 'shard'",
                "FOO == 'uid'",
                // two terms: shard-shard, shard-uid, uid-shard, uid-uid
                "FOO == 'shard' && FOO2 == 'shard'",
                "FOO == 'shard' && FOO2 == 'uid'",
                "FOO == 'uid' && FOO2 == 'shard'",
                "FOO == 'uid' && FOO2 == 'uid'",
                // two terms, one side is a nested union
                "FOO == 'shard' && (FOO2 == 'shard' || FOO3 == 'shard')",
                "FOO == 'shard' && (FOO2 == 'shard' || FOO3 == 'uid')",
                "FOO == 'shard' && (FOO2 == 'uid' || FOO3 == 'shard')",
                "FOO == 'shard' && (FOO2 == 'uid' || FOO3 == 'uid')",
                "FOO == 'uid' && (FOO2 == 'shard' || FOO3 == 'shard')",
                "FOO == 'uid' && (FOO2 == 'shard' || FOO3 == 'uid')",
                "FOO == 'uid' && (FOO2 == 'uid' || FOO3 == 'shard')",
                "FOO == 'uid' && (FOO2 == 'uid' || FOO3 == 'uid')",
                // three terms, one term is a nested union
                "FOO == 'shard' && FOO2 == 'shard' && (FOO2 == 'shard' || FOO3 == 'shard')",
                "FOO == 'shard' && FOO2 == 'shard' && (FOO2 == 'shard' || FOO3 == 'uid')",
                "FOO == 'shard' && FOO2 == 'shard' && (FOO2 == 'uid' || FOO3 == 'shard')",
                "FOO == 'shard' && FOO2 == 'shard' && (FOO2 == 'uid' || FOO3 == 'uid')",
                "FOO == 'uid' && FOO2 == 'uid' && (FOO2 == 'shard' || FOO3 == 'shard')",
                "FOO == 'uid' && FOO2 == 'uid' && (FOO2 == 'shard' || FOO3 == 'uid')",
                "FOO == 'uid' && FOO2 == 'uid' && (FOO2 == 'uid' || FOO3 == 'shard')",
                "FOO == 'uid' && FOO2 == 'uid' && (FOO2 == 'uid' || FOO3 == 'uid')",
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
            query += " && " + append;
            test(query, query, QUERY_CONTEXT.INTERSECTION, termContext);
        }
    }
    
    private void testUnions(String append, TERM_CONTEXT termContext) throws Exception {
        String[] queries = {
                // single terms
                "FOO == 'shard'",
                "FOO == 'uid'",
                // two terms: shard-shard, shard-uid, uid-shard, uid-uid
                "FOO == 'shard' || FOO2 == 'shard'",
                "FOO == 'shard' || FOO2 == 'uid'",
                "FOO == 'uid' || FOO2 == 'shard'",
                "FOO == 'uid' || FOO2 == 'uid'",
                // two terms, one side is a nested intersection
                "FOO == 'shard' || (FOO2 == 'shard' && FOO3 == 'shard')",
                "FOO == 'shard' || (FOO2 == 'shard' && FOO3 == 'uid')",
                "FOO == 'shard' || (FOO2 == 'uid' && FOO3 == 'shard')",
                "FOO == 'shard' || (FOO2 == 'uid' && FOO3 == 'uid')",
                "FOO == 'uid' || (FOO2 == 'shard' && FOO3 == 'shard')",
                "FOO == 'uid' || (FOO2 == 'shard' && FOO3 == 'uid')",
                "FOO == 'uid' || (FOO2 == 'uid' && FOO3 == 'shard')",
                "FOO == 'uid' || (FOO2 == 'uid' && FOO3 == 'uid')",
                // three terms, one term is a nested intersection
                "FOO == 'shard' || FOO2 == 'shard' || (FOO2 == 'shard' && FOO3 == 'shard')",
                "FOO == 'shard' || FOO2 == 'shard' || (FOO2 == 'shard' && FOO3 == 'uid')",
                "FOO == 'shard' || FOO2 == 'shard' || (FOO2 == 'uid' && FOO3 == 'shard')",
                "FOO == 'shard' || FOO2 == 'shard' || (FOO2 == 'uid' && FOO3 == 'uid')",
                "FOO == 'uid' || FOO2 == 'uid' || (FOO2 == 'shard' && FOO3 == 'shard')",
                "FOO == 'uid' || FOO2 == 'uid' || (FOO2 == 'shard' && FOO3 == 'uid')",
                "FOO == 'uid' || FOO2 == 'uid' || (FOO2 == 'uid' && FOO3 == 'shard')",
                "FOO == 'uid' || FOO2 == 'uid' || (FOO2 == 'uid' && FOO3 == 'uid')",
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
            query += " || " + append;
            test(query, query, QUERY_CONTEXT.UNION, termContext);
        }
    }
    
    private void test(String query, String expected, QUERY_CONTEXT queryContext, TERM_CONTEXT termContext) throws Exception {
        
        // Run a standard limited-scanner range stream.
        count++;
        rangeStream = new RangeStream(config, new ScannerFactory(config.getConnector(), 1), helper);
        rangeStream.setLimitScanners(true);
        
        script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        CloseableIterable<QueryPlan> queryPlans = rangeStream.streamPlans(script);
        
        // call to 'iterator()' will modify the stream context
        Iterator<QueryPlan> queryPlanIter = queryPlans.iterator();
        
        switch (rangeStream.context()) {
            case PRESENT:
            case INITIALIZED:
            case EXCEEDED_VALUE_THRESHOLD:
                // these are good
                break;
            case UNINDEXED:
            case EXCEEDED_TERM_THRESHOLD:
                // top level unions with a delayed term cannot be executed. Capture that case here.
                if (termContext.equals(TERM_CONTEXT.DELAYED) && queryContext.equals(QUERY_CONTEXT.UNION)) {
                    queryPlans.close();
                    rangeStream.close();
                    return;
                }
                break;
            default:
                queryPlans.close();
                rangeStream.close();
                fail("RangeStream context was: " + rangeStream.context() + " for query: " + query);
                return;
        }
        
        assertTrue(query + " did not produce any query plans", queryPlanIter.hasNext());
        
        QueryPlan plan = queryPlanIter.next();
        assertNotNull("expected a query plan, but was null for query: " + query, plan);
        
        ASTJexlScript plannedScript = JexlNodeFactory.createScript(plan.getQueryTree());
        ASTJexlScript expectedScript = JexlASTHelper.parseAndFlattenJexlQuery(expected);
        if (!TreeEqualityVisitor.isEqual(expectedScript, plannedScript)) {
            fail("Expected [" + JexlStringBuildingVisitor.buildQuery(expectedScript) + "] but got [" + JexlStringBuildingVisitor.buildQuery(plannedScript)
                            + "]");
        }
        queryPlans.close();
        rangeStream.close();
    }
}
