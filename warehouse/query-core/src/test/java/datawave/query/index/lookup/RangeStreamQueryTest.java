package datawave.query.index.lookup;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import datawave.accumulo.inmemory.InMemoryInstance;
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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static datawave.query.index.lookup.IndexStream.StreamContext.INITIALIZED;
import static datawave.util.TableName.SHARD_INDEX;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Integration test for asserting correct query plans coming off the RangeStream
 * <p>
 * Underlying index streams are assumed to have data based on the field
 * 
 * <pre>
 *     FOO  - has data, doc range
 *     FOO2 - has data, alternate doc range
 *     FOO3 - has data, shard range
 *     FOO4 - has no data
 * </pre>
 */
public class RangeStreamQueryTest {
    
    private static InMemoryInstance instance = new InMemoryInstance(RangeStreamQueryTest.class.toString());
    private static Connector connector;
    private ShardQueryConfiguration config;
    
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
        
        Mutation m = new Mutation("bar");
        m.put(new Text("FOO"), cq, docValue);
        m.put(new Text("FOO2"), cq, docValue2);
        m.put(new Text("FOO3"), cq, shardValue);
        bw.addMutation(m);
        
        m = new Mutation("baz");
        m.put(new Text("FOO"), cq, docValue);
        m.put(new Text("FOO2"), cq, docValue2);
        m.put(new Text("FOO3"), cq, shardValue);
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
    public void setupTest() {
        config = new ShardQueryConfiguration();
        config.setConnector(connector);
        config.setShardsPerDayThreshold(20);
    }
    
    @Test
    public void testSimpleQueries() throws Exception {
        test("FOO == 'bar'");
        test("FOO == 'baz'");
        test("FOO2 == 'bar'");
        test("FOO2 == 'baz'");
        test("FOO3 == 'bar'");
        test("FOO3 == 'baz'");
    }
    
    @Test
    public void testSimpleQueriesWithDelayedTerms() throws Exception {
        test("FOO == 'bar' && !(FOO == null)");
        test("FOO == 'bar' && FOO3 == 'baz' && !(FOO == null)");
        test("(FOO == 'bar' || FOO3 == 'baz') && !(FOO == null)");
        test("(FOO == 'bar' || FOO3 == 'baz') && FOO == 'bar' && !(FOO == null)");
        test("(FOO == 'bar' || FOO3 == 'baz') && (FOO == 'bar' || FOO3 == 'bar') && !(FOO == null)");
        
        test("FOO == 'bar' && (!(FOO == null) || !(FOO == null))");
        test("FOO == 'bar' && FOO3 == 'baz' && (!(FOO == null) || !(FOO == null))");
        test("(FOO == 'bar' || FOO3 == 'baz') && (!(FOO == null) || !(FOO == null))");
        test("(FOO == 'bar' || FOO3 == 'baz') && FOO == 'bar' && (!(FOO == null) || !(FOO == null))");
        test("(FOO == 'bar' || FOO3 == 'baz') && (FOO == 'bar' || FOO3 == 'bar') && (!(FOO == null) || !(FOO == null))");
    }
    
    @Test
    public void testLargerQueriesWithDelayedTerms() throws Exception {
        test("FOO == 'bar' && FOO3 == 'baz' && FOO3 == 'bar' && !(FOO == null)");
        test("FOO == 'bar' && FOO3 == 'baz' && FOO3 == 'bar' && filter:includeRegex(FOO2, 'ba.*')");
        test("FOO == 'bar' && FOO3 == 'baz' && FOO3 == 'bar' && (filter:includeRegex(FOO, 'ba.*') || filter:includeRegex(FOO2, 'ba.*'))");
        test("FOO == 'bar' && FOO3 == 'baz' && FOO3 == 'bar' && (!(FOO == null) || !(FOO2 == null)) && (filter:includeRegex(FOO, 'ba.*') || filter:includeRegex(FOO2, 'ba.*'))");
    }
    
    private void test(String query) throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        config.setBeginDate(sdf.parse("20200101"));
        config.setEndDate(sdf.parse("20200102"));
        
        config.setDatatypeFilter(Collections.singleton("datatype"));
        
        Multimap<String,Type<?>> fieldToDataType = HashMultimap.create();
        fieldToDataType.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));
        fieldToDataType.putAll("FOO2", Sets.newHashSet(new LcNoDiacriticsType()));
        fieldToDataType.putAll("FOO3", Sets.newHashSet(new LcNoDiacriticsType()));
        
        config.setQueryFieldsDatatypes(fieldToDataType);
        config.setIndexedFields(fieldToDataType);
        config.setShardsPerDayThreshold(2);
        
        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(fieldToDataType.keySet());
        
        // Run a standard limited-scanner range stream.
        RangeStream rangeStream = new RangeStream(config, new ScannerFactory(config.getConnector(), 1), helper);
        rangeStream.setLimitScanners(true);
        
        CloseableIterable<QueryPlan> queryPlans = rangeStream.streamPlans(script);
        assertTrue(INITIALIZED.equals(rangeStream.context()) || (IndexStream.StreamContext.PRESENT.equals(rangeStream.context())));
        
        QueryPlan plan = queryPlans.iterator().next();
        assertNotNull("expected a query plan, but was null for query: " + query, plan);
        
        ASTJexlScript planned = JexlNodeFactory.createScript(plan.getQueryTree());
        ASTJexlScript expected = JexlASTHelper.parseAndFlattenJexlQuery(query);
        assertTrue(JexlStringBuildingVisitor.buildQueryWithoutParse(planned), TreeEqualityVisitor.isEqual(expected, planned));
    }
}
