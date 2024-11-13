package datawave.query.index.lookup;

import static datawave.common.test.utils.query.RangeFactoryForTests.makeShardedRange;
import static datawave.common.test.utils.query.RangeFactoryForTests.makeTestRange;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NoOpType;
import datawave.data.type.NumberType;
import datawave.data.type.Type;
import datawave.data.type.util.NumericalEncoder;
import datawave.ingest.protobuf.Uid;
import datawave.query.CloseableIterable;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.planner.QueryPlan;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MockMetadataHelper;
import datawave.query.util.Tuple2;
import datawave.test.JexlNodeAssert;

/**
 * Given a known set of data, assert that the RangeStream class generates the correct ranges for a basic set of queries.
 */
public class RangeStreamTest {

    private static final InMemoryInstance instance = new InMemoryInstance(RangeStreamTest.class.toString());
    private static AccumuloClient client;
    private ShardQueryConfiguration config;

    @BeforeClass
    public static void setupAccumulo() throws Exception {

        final String SHARD_INDEX = "shardIndex";

        client = new InMemoryAccumuloClient("", new InMemoryInstance());
        client.tableOperations().create(SHARD_INDEX);

        BatchWriter bw = client.createBatchWriter(SHARD_INDEX,
                        new BatchWriterConfig().setMaxLatency(10, TimeUnit.SECONDS).setMaxMemory(100000L).setMaxWriteThreads(1));

        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.addUID("123");
        builder.setIGNORE(false);
        builder.setCOUNT(1);
        Uid.List list = builder.build();

        Mutation m = new Mutation("ba");
        m.put(new Text("FOO"), new Text("20190314\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        // ---------------

        builder = Uid.List.newBuilder();
        builder.addUID("234");
        builder.addUID("345");
        builder.setIGNORE(false);
        builder.setCOUNT(2);
        list = builder.build();

        m = new Mutation("bag");
        m.put(new Text("FOO"), new Text("20190314\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        // ---------------

        builder = Uid.List.newBuilder();
        builder.addUID("234");
        builder.addUID("345");
        builder.setIGNORE(false);
        builder.setCOUNT(2);
        list = builder.build();

        m = new Mutation("candy corn");
        m.put(new Text("CANDY_TYPE"), new Text("20190315\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        // ---------------

        builder = Uid.List.newBuilder();
        builder.addUID("345");
        builder.setIGNORE(false);
        builder.setCOUNT(1);
        list = builder.build();

        m = new Mutation("bar");
        m.put(new Text("FOO"), new Text("20190314\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        // ---------------

        builder = Uid.List.newBuilder();
        builder.addUID("345");
        builder.addUID("456");
        builder.addUID("567");
        builder.setIGNORE(false);
        builder.setCOUNT(3);
        list = builder.build();

        m = new Mutation("bard");
        m.put(new Text("FOO"), new Text("20190314_0\0" + "datatype2"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190314_1\0" + "datatype2"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190314_10\0" + "datatype2"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190314_100\0" + "datatype2"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190314_9\0" + "datatype2"), new Value(list.toByteArray()));
        bw.addMutation(m);

        m = new Mutation("bardy");
        m.put(new Text("FOO"), new Text("20190314_0\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190314_1\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190314_10\0" + "datatype2"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190314_100\0" + "datatype2"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190314_9\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        builder = Uid.List.newBuilder();
        builder.addUID("345");
        builder.addUID("456");
        builder.addUID("567");
        builder.addUID("1345");
        builder.addUID("2456");
        builder.addUID("3567");
        builder.setIGNORE(false);
        builder.setCOUNT(6);
        list = builder.build();

        m = new Mutation("boohoo");
        m.put(new Text("FOO"), new Text("20190314_0\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190314_1\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190314_10\0" + "datatype2"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190314_100\0" + "datatype2"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190314_9\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        // ---------------

        // Too many laughs
        builder = Uid.List.newBuilder();
        builder.setIGNORE(true);
        builder.setCOUNT(30);
        list = builder.build();

        m = new Mutation("bahahaha");
        m.put(new Text("LAUGH"), new Text("20190314_0\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("LAUGH"), new Text("20190314_1\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("LAUGH"), new Text("20190314_100\0" + "datatype2"), new Value(list.toByteArray()));
        m.put(new Text("LAUGH"), new Text("20190314_9\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        // ---------------

        builder = Uid.List.newBuilder();
        builder.addUID("567");
        builder.setIGNORE(false);
        builder.setCOUNT(1);
        list = builder.build();

        m = new Mutation("barz");
        m.put(new Text("FOO"), new Text("20190314\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        // ---------------

        builder = Uid.List.newBuilder();
        builder.addUID("678");
        builder.setIGNORE(false);
        builder.setCOUNT(1);
        list = builder.build();

        m = new Mutation("bat");
        m.put(new Text("FOO"), new Text("20190314\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        // ---------------

        builder = Uid.List.newBuilder();
        builder.addUID("efg");
        builder.addUID("fgh");
        builder.setIGNORE(false);
        builder.setCOUNT(2);
        list = builder.build();

        m = new Mutation("+aE1");
        m.put(new Text("NUM"), new Text("20190314\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        // ---------------

        builder = Uid.List.newBuilder();
        builder.addUID("def");
        builder.addUID("egh");
        builder.setIGNORE(false);
        builder.setCOUNT(2);
        list = builder.build();

        m = new Mutation("+aE2");
        m.put(new Text("NUM"), new Text("20190314\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        // ---------------

        builder = Uid.List.newBuilder();
        builder.addUID("cde");
        builder.addUID("def");
        builder.setIGNORE(false);
        builder.setCOUNT(2);
        list = builder.build();

        m = new Mutation("+aE3");
        m.put(new Text("NUM"), new Text("20190314\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        // ---------------

        builder = Uid.List.newBuilder();
        builder.addUID("bcd");
        builder.addUID("cde");
        builder.setIGNORE(false);
        builder.setCOUNT(2);
        list = builder.build();

        m = new Mutation("+aE4");
        m.put(new Text("NUM"), new Text("20190314\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        // ---------------

        builder = Uid.List.newBuilder();
        builder.addUID("abc");
        builder.addUID("bcd");
        builder.setIGNORE(false);
        builder.setCOUNT(2);
        list = builder.build();

        m = new Mutation("+aE5");
        m.put(new Text("NUM"), new Text("20190314\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        // ---------------

        builder = Uid.List.newBuilder();
        builder.addUID("negnum1");
        builder.addUID("negnum2");
        builder.setIGNORE(false);
        builder.setCOUNT(2);
        list = builder.build();

        m = new Mutation(NumericalEncoder.encode("-1"));
        m.put(new Text("KELVIN"), new Text("20190314\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        // ---------------

        builder = Uid.List.newBuilder();
        builder.addUID("123");
        builder.addUID("345");
        builder.setIGNORE(false);
        builder.setCOUNT(2);
        list = builder.build();

        m = new Mutation("barter");
        m.put(new Text("FOO"), new Text("20190314_1\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        builder = Uid.List.newBuilder();
        builder.addUID("123");
        builder.addUID("345");
        builder.setIGNORE(false);
        builder.setCOUNT(2);
        list = builder.build();

        m = new Mutation("baggy");
        m.put(new Text("FOO"), new Text("20190414_1\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        m = new Mutation("oreo");
        m.put(new Text("FOO"), new Text("20190314_1\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        // ---------------

        // Terms for high-low cardinality test with query (FOO == 'low_card' && FOO == 'high_card')
        // Four terms {'highest_card', 'high_card', 'low_card', 'lowest_card'}
        // Ranges fall across 8 days, each day has up to 50 shards.
        builder = Uid.List.newBuilder();
        builder.addUID("a.b.c");
        builder.setIGNORE(false);
        builder.setCOUNT(2);
        list = builder.build();

        m = new Mutation("lowest_card");
        m.put(new Text("FOO"), new Text("20190310_1\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190314_22\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190315_49\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        m = new Mutation("low_card");
        m.put(new Text("FOO"), new Text("20190310_1\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190312_1\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190314_22\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190315_33\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190317_1\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        builder = Uid.List.newBuilder();
        builder.addUID("a.b.c");
        builder.addUID("d.e.f");
        builder.setIGNORE(false);
        builder.setCOUNT(2);
        list = builder.build();

        m = new Mutation("high_card");
        for (int day = 0; day < 8; day += 2) {
            for (int ii = 1; ii < 50; ii++) {
                m.put(new Text("FOO"), new Text("2019031" + day + "_" + ii + "\0" + "datatype1"), new Value(list.toByteArray()));
            }
        }
        bw.addMutation(m);

        m = new Mutation("highest_card");
        for (int day = 0; day < 8; day++) {
            for (int ii = 1; ii < 50; ii++) {
                m.put(new Text("FOO"), new Text("2019031" + day + "_" + ii + "\0" + "datatype1"), new Value(list.toByteArray()));
            }
        }
        bw.addMutation(m);

        // ---------------

        // Keep it simple, just have one hit.
        builder = Uid.List.newBuilder();
        builder.addUID("a.b.c");
        builder.setIGNORE(true);
        builder.setCOUNT(5000);
        list = builder.build();

        // With shards per day set to zero, these will roll up
        m = new Mutation("day_ranges");
        m.put(new Text("FOO"), new Text("20190310_0\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190310_1\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190310_2\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190310_3\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190310_4\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190310_5\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190310_6\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190310_7\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190310_8\0" + "datatype1"), new Value(list.toByteArray()));

        m.put(new Text("FOO"), new Text("20190311_0\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190312_0\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190313_0\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190314_0\0" + "datatype1"), new Value(list.toByteArray()));

        m.put(new Text("FOO"), new Text("20190315_0\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190315_1\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190315_2\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190315_3\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190315_4\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190315_5\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190315_6\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190315_7\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190315_8\0" + "datatype1"), new Value(list.toByteArray()));

        m.put(new Text("FOO"), new Text("20190316_0\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        builder = Uid.List.newBuilder();
        builder.addUID("a.b.c");
        builder.setIGNORE(false);
        builder.setCOUNT(1);
        list = builder.build();

        m = new Mutation("shard_range");
        m.put(new Text("FOO"), new Text("20190310_21\0" + "datatype1"), new Value(list.toByteArray()));
        m.put(new Text("FOO"), new Text("20190315_51\0" + "datatype1"), new Value(list.toByteArray()));
        bw.addMutation(m);

        // ---------------

        bw.flush();
        bw.close();
    }

    @Before
    public void setupTest() {
        config = new ShardQueryConfiguration();
        config.setClient(client);
    }

    @Test
    public void testTheSimplestOfQueries() throws Exception {
        String originalQuery = "FOO == 'bag'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);

        config.setBeginDate(new Date(0));
        config.setEndDate(new Date(System.currentTimeMillis()));

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));
        dataTypes.putAll("NUM", Sets.newHashSet(new NumberType()));

        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());

        Set<Range> expectedRanges = Sets.newHashSet(makeTestRange("20190314", "datatype1\u0000234"), makeTestRange("20190314", "datatype1\u0000345"));
        for (QueryPlan queryPlan : getRangeStream(helper).streamPlans(script)) {
            for (Range range : queryPlan.getRanges()) {
                assertTrue("Tried to remove unexpected range from expected ranges: " + range.toString(), expectedRanges.remove(range));
            }
        }
        assertTrue("Expected ranges not found in query plan: " + expectedRanges.toString(), expectedRanges.isEmpty());
    }

    @Test
    public void testShardAndDaysHint() throws Exception {
        String originalQuery = "(FOO == 'bardy') && (SHARDS_AND_DAYS = '20190314_2,20190314_1')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);

        config.setBeginDate(new Date(0));
        config.setEndDate(new Date(System.currentTimeMillis()));

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));
        dataTypes.putAll("NUM", Sets.newHashSet(new NumberType()));

        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());

        Set<Range> expectedRanges = Sets.newHashSet(makeShardedRange("20190314_1"));
        for (QueryPlan queryPlan : getRangeStream(helper).streamPlans(script)) {
            for (Range range : queryPlan.getRanges()) {
                assertTrue("Tried to remove unexpected range " + range.toString() + " from expected ranges: " + expectedRanges, expectedRanges.remove(range));
            }
        }
        assertTrue("Expected ranges not found in query plan: " + expectedRanges, expectedRanges.isEmpty());
    }

    public void testShardAndDaysHints(String originalQuery) throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);

        config.setBeginDate(new Date(0));
        config.setEndDate(new Date(System.currentTimeMillis()));

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));
        dataTypes.putAll("NUM", Sets.newHashSet(new NumberType()));

        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());

        Set<Range> expectedRanges = Sets.newHashSet();
        for (String shard : Lists.newArrayList("20190314_0", "20190314_1", "20190314_9", "20190314_10", "20190314_100")) {
            expectedRanges.add(makeShardedRange(shard));
        }

        for (QueryPlan queryPlan : getRangeStream(helper).streamPlans(script)) {
            for (Range range : queryPlan.getRanges()) {
                assertTrue("Tried to remove unexpected range " + range.toString() + " from expected ranges: " + expectedRanges, expectedRanges.remove(range));
            }
        }
        assertTrue(expectedRanges.size() + " expected ranges not found in query plan: " + expectedRanges, expectedRanges.isEmpty());
    }

    @Test
    public void testShardAndDaysHints1() throws Exception {
        String originalQuery = "(FOO == 'bardy') && (SHARDS_AND_DAYS = '20190311,20190313,20190314')";
        testShardAndDaysHints(originalQuery);
    }

    @Test
    public void testShardAndDaysHints2() throws Exception {
        String originalQuery = "(FOO == 'bardy') && ((SHARDS_AND_DAYS = '20190311,20190313,20190314') || (SHARDS_AND_DAYS = '20190311,20190313,20190314'))";
        testShardAndDaysHints(originalQuery);
    }

    @Test
    public void testShardAndDaysHints3() throws Exception {
        String originalQuery = "(FOO == 'bardy') && ( (filter:include(FOO, 'bardy') && (SHARDS_AND_DAYS = '20190311,20190313,20190314')) )";
        testShardAndDaysHints(originalQuery);
    }

    @Test
    public void testShardAndDaysHints4() throws Exception {
        String originalQuery = "(FOO == 'bardy') && ( (filter:include(FOO, 'bardy') && (SHARDS_AND_DAYS = '20190313,20190314')) || (filter:include(FOO, 'bardy') && (SHARDS_AND_DAYS = '20190313,20190314')) )";
        testShardAndDaysHints(originalQuery);
    }

    @Test
    public void testShardAndDaysHints5() throws Exception {
        String originalQuery = "(FOO == 'bardy') && ( (filter:include(FOO, 'bardy') && (SHARDS_AND_DAYS = '20190312,20190313,20190314')) || (filter:include(FOO, 'bardy') && (SHARDS_AND_DAYS = '20190312,20190313,20190314')) )";
        testShardAndDaysHints(originalQuery);
    }

    @Test
    public void testShardAndDaysHints6() throws Exception {
        String originalQuery = "(FOO == 'oreo') && ((filter:include(FOO, 'tardy') && (SHARDS_AND_DAYS = '20190312,20190313,20190314')) || (filter:include(FOO, 'bardy') && (SHARDS_AND_DAYS = '20190312,20190313,20190314')) )";

        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);

        config.setBeginDate(new Date(0));
        config.setEndDate(new Date(System.currentTimeMillis()));

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));
        dataTypes.putAll("NUM", Sets.newHashSet(new NumberType()));

        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());

        Set<Range> expectedRanges = Sets.newHashSet(makeShardedRange("20190314_1"));

        for (QueryPlan queryPlan : getRangeStream(helper).streamPlans(script)) {
            // verify the query plan dropped no terms
            JexlNode queryTree = JexlASTHelper.parseJexlQuery(queryPlan.getQueryString());
            JexlNode expectedTree = JexlASTHelper.parseJexlQuery(
                            "(((SHARDS_AND_DAYS = '20190314') && filter:include(FOO, 'tardy')) || ((SHARDS_AND_DAYS = '20190314') && filter:include(FOO, 'bardy'))) && FOO == 'oreo'");
            JexlNodeAssert.assertThat(queryTree).isEqualTo(expectedTree);

            // verify the range
            for (Range range : queryPlan.getRanges()) {
                assertTrue("Tried to remove unexpected range " + range.toString() + " from expected ranges: " + expectedRanges, expectedRanges.remove(range));
            }
        }

        assertTrue(expectedRanges.size() + " expected ranges not found in query plan: " + expectedRanges, expectedRanges.isEmpty());
    }

    @Test
    public void testOrBothIndexed() throws Exception {
        String originalQuery = "(FOO == 'bag' || FOO == 'ba')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);

        config.setBeginDate(new Date(0));
        config.setEndDate(new Date(System.currentTimeMillis()));

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));
        dataTypes.putAll("NUM", Sets.newHashSet(new NumberType()));

        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());

        Range range1 = makeTestRange("20190314", "datatype1\u0000234");
        Range range2 = makeTestRange("20190314", "datatype1\u0000345");
        Range range3 = makeTestRange("20190314", "datatype1\u0000123");
        Set<Range> expectedRanges = Sets.newHashSet(range1, range2, range3);
        for (QueryPlan queryPlan : getRangeStream(helper).streamPlans(script)) {
            for (Range range : queryPlan.getRanges()) {
                assertTrue("Tried to remove unexpected range " + range.toString() + " from expected ranges: " + expectedRanges.toString(),
                                expectedRanges.remove(range));
            }
        }
        assertTrue("Expected ranges not found in query plan: " + expectedRanges.toString(), expectedRanges.isEmpty());
    }

    @Test
    public void testNestedOr() throws Exception {
        String originalQuery = "(FOO == 'bag' || FOO == 'ba' || FOO == 'barglegarglebarsh')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);

        config.setBeginDate(new Date(0));
        config.setEndDate(new Date(System.currentTimeMillis()));

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));
        dataTypes.putAll("NUM", Sets.newHashSet(new NumberType()));

        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());

        Range range1 = makeTestRange("20190314", "datatype1\u0000234");
        Range range2 = makeTestRange("20190314", "datatype1\u0000345");
        Range range3 = makeTestRange("20190314", "datatype1\u0000123");
        Set<Range> expectedRanges = Sets.newHashSet(range1, range2, range3);

        RangeStream rangeStream = getRangeStream(helper);
        rangeStream.setLimitScanners(true);
        for (QueryPlan queryPlan : rangeStream.streamPlans(script)) {
            for (Range range : queryPlan.getRanges()) {
                assertTrue("Tried to remove unexpected range " + range.toString() + " from expected ranges: " + expectedRanges.toString(),
                                expectedRanges.remove(range));
            }
        }
        assertTrue("Expected ranges not found in query plan: " + expectedRanges.toString(), expectedRanges.isEmpty());
    }

    @Test
    public void testBothIndexedPrune() throws Exception {
        String originalQuery = "(FOO == 'barter' || FOO == 'baggy')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);

        config.setBeginDate(new Date(0));
        config.setEndDate(new Date(System.currentTimeMillis()));

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));
        dataTypes.putAll("NUM", Sets.newHashSet(new NumberType()));

        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());

        Range range1 = makeTestRange("20190314_1", "datatype1\u0000123");
        Range range2 = makeTestRange("20190314_1", "datatype1\u0000345");
        Range range3 = makeTestRange("20190414_1", "datatype1\u0000123");
        Range range4 = makeTestRange("20190414_1", "datatype1\u0000345");
        Set<Range> expectedRanges = Sets.newHashSet(range1, range2, range3, range4);

        RangeStream rangeStream = getRangeStream(helper).setLimitScanners(true);
        for (QueryPlan queryPlan : rangeStream.streamPlans(script)) {
            for (Range range : queryPlan.getRanges()) {
                assertTrue("Tried to remove unexpected range " + range.toString() + " from expected ranges: " + expectedRanges.toString(),
                                expectedRanges.remove(range));
            }
        }
        assertTrue("Expected ranges not found in query plan: " + expectedRanges.toString(), expectedRanges.isEmpty());
    }

    @Test
    public void testOrOneFieldIndexed() throws Exception {
        String originalQuery = "(FOO == 'bag' || TACO == 'ba')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);

        config.setBeginDate(new Date(0));
        config.setEndDate(new Date(System.currentTimeMillis()));

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));
        dataTypes.putAll("NUM", Sets.newHashSet(new NumberType()));

        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());
        helper.addFields(ImmutableSet.of("TACO"));

        assertFalse(getRangeStream(helper).streamPlans(script).iterator().hasNext());
    }

    @Test
    public void testOrNoFieldIndexed() throws Exception {
        String originalQuery = "(TACO == 'bag' || TACO == 'ba')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);

        config.setBeginDate(new Date(0));
        config.setEndDate(new Date(System.currentTimeMillis()));

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));
        dataTypes.putAll("NUM", Sets.newHashSet(new NumberType()));

        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());

        assertFalse(getRangeStream(helper).streamPlans(script).iterator().hasNext());
    }

    @Test
    public void testAndTwoFieldsIndexed() throws Exception {
        String originalQuery = "(FOO == 'bag' && FOO == 'ba')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);

        config.setBeginDate(new Date(0));
        config.setEndDate(new Date(System.currentTimeMillis()));

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));
        dataTypes.putAll("NUM", Sets.newHashSet(new NumberType()));

        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());

        assertFalse(getRangeStream(helper).streamPlans(script).iterator().hasNext());
    }

    @Test
    public void testAndOneFieldIndexed() throws Exception {
        String originalQuery = "(FOO == 'bag' && TACO == 'ba')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);

        config.setBeginDate(new Date(0));
        config.setEndDate(new Date(System.currentTimeMillis()));

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));
        dataTypes.putAll("NUM", Sets.newHashSet(new NumberType()));

        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());
        helper.addFields(ImmutableSet.of("TACO"));

        Range range1 = makeTestRange("20190314", "datatype1\u0000234");
        Range range2 = makeTestRange("20190314", "datatype1\u0000345");
        Set<Range> expectedRanges = Sets.newHashSet(range1, range2);

        RangeStream rangeStream = getRangeStream(helper);
        for (QueryPlan queryPlan : rangeStream.streamPlans(script)) {
            for (Range range : queryPlan.getRanges()) {
                assertTrue("Tried to remove unexpected range " + range.toString() + " from expected ranges: " + expectedRanges.toString(),
                                expectedRanges.remove(range));
            }
        }
        assertTrue("Expected ranges not found in query plan: " + expectedRanges.toString(), expectedRanges.isEmpty());
    }

    @Test
    public void testAndNoFieldIndexed() throws Exception {
        String originalQuery = "(TACO == 'bag' && TACO == 'ba')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);

        config.setBeginDate(new Date(0));
        config.setEndDate(new Date(System.currentTimeMillis()));

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));
        dataTypes.putAll("NUM", Sets.newHashSet(new NumberType()));

        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());

        assertFalse(getRangeStream(helper).streamPlans(script).iterator().hasNext());
    }

    @Test
    public void testNonIndexedNumeric() throws Exception {
        String originalQuery = "(FOO == 'bag' && BAR == 4)";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);

        config.setBeginDate(new Date(0));
        config.setEndDate(new Date(System.currentTimeMillis()));

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));

        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());
        helper.addFields(Lists.newArrayList("BAR"));

        Range range1 = makeTestRange("20190314", "datatype1\u0000234");
        Range range2 = makeTestRange("20190314", "datatype1\u0000345");
        Set<Range> expectedRanges = Sets.newHashSet(range1, range2);

        RangeStream rangeStream = getRangeStream(helper);
        for (QueryPlan queryPlan : rangeStream.streamPlans(script)) {
            for (Range range : queryPlan.getRanges()) {
                assertTrue("Tried to remove unexpected range " + range.toString() + " from expected ranges: " + expectedRanges.toString(),
                                expectedRanges.remove(range));
            }
        }
        assertTrue("Expected ranges not found in query plan: " + expectedRanges.toString(), expectedRanges.isEmpty());
    }

    @Test
    public void testGuardAgainstVeryOddlyFormedJexlQueriesLikeFoo() throws Exception {
        String originalQuery = "foo";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);

        config.setBeginDate(new Date(0));
        config.setEndDate(new Date(System.currentTimeMillis()));

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));

        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());

        RangeStream rangeStream = getRangeStream(helper);
        rangeStream.streamPlans(script);
        assertEquals(IndexStream.StreamContext.UNINDEXED, rangeStream.context());
        assertEquals(Collections.emptyIterator(), rangeStream.iterator());
    }

    @Test
    public void testPrune() throws Exception {
        String originalQuery = "FOO=='bag' || FOO=='qwertylikeskeyboards'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);

        config.setBeginDate(new Date(0));
        config.setEndDate(new Date(System.currentTimeMillis()));

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));
        dataTypes.putAll("BAR", Sets.newHashSet(new NoOpType()));
        dataTypes.putAll("BAR", Sets.newHashSet(new NumberType()));

        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());
        helper.addFields(Lists.newArrayList("BAR"));

        Range range1 = makeTestRange("20190314", "datatype1\u0000234");
        Range range2 = makeTestRange("20190314", "datatype1\u0000345");
        Set<Range> expectedRanges = Sets.newHashSet(range1, range2);

        RangeStream rangeStream = getRangeStream(helper);
        for (QueryPlan queryPlan : rangeStream.streamPlans(script)) {
            assertEquals("FOO == 'bag'", JexlStringBuildingVisitor.buildQuery(queryPlan.getQueryTree()));
            for (Range range : queryPlan.getRanges()) {
                assertTrue("Tried to remove unexpected range " + range.toString() + " from expected ranges: " + expectedRanges.toString(),
                                expectedRanges.remove(range));
            }
        }
        assertTrue("Expected ranges not found in query plan: " + expectedRanges.toString(), expectedRanges.isEmpty());
    }

    @Test
    public void testUnIndexedNumericAsString() throws Exception {
        String originalQuery = "(FOO == 'bag' && BAR == '+aE1')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);

        config.setBeginDate(new Date(0));
        config.setEndDate(new Date(System.currentTimeMillis()));

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));

        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());
        helper.addFields(Lists.newArrayList("BAR"));

        Range range1 = makeTestRange("20190314", "datatype1\u0000234");
        Range range2 = makeTestRange("20190314", "datatype1\u0000345");
        Set<Range> expectedRanges = Sets.newHashSet(range1, range2);

        RangeStream rangeStream = getRangeStream(helper);
        for (QueryPlan queryPlan : rangeStream.streamPlans(script)) {
            for (Range range : queryPlan.getRanges()) {
                assertTrue("Tried to remove unexpected range " + range.toString() + " from expected ranges: " + expectedRanges.toString(),
                                expectedRanges.remove(range));
            }
        }
        assertTrue("Expected ranges not found in query plan: " + expectedRanges.toString(), expectedRanges.isEmpty());
    }

    @Test
    public void testNegatedIndexWithResults() throws Exception {
        String originalQuery = "FOO == 'bag' && BAR != 'bar'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);

        config.setBeginDate(new Date(0));
        config.setEndDate(new Date(System.currentTimeMillis()));

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));

        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());

        Range range1 = makeTestRange("20190314", "datatype1\u0000234");
        Range range2 = makeTestRange("20190314", "datatype1\u0000345");
        Set<Range> expectedRanges = Sets.newHashSet(range1, range2);

        RangeStream rangeStream = getRangeStream(helper).setLimitScanners(true);
        for (QueryPlan queryPlan : rangeStream.streamPlans(script)) {
            for (Range range : queryPlan.getRanges()) {
                assertTrue("Tried to remove unexpected range " + range.toString() + " from expected ranges: " + expectedRanges.toString(),
                                expectedRanges.remove(range));
            }
        }
        assertTrue("Expected ranges not found in query plan: " + expectedRanges.toString(), expectedRanges.isEmpty());
    }

    @Test
    public void testNegatedIndexWithNoResults() throws Exception {
        String originalQuery = "FOO == 'bag' && BAR != 'tacocat'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);

        config.setBeginDate(new Date(0));
        config.setEndDate(new Date(System.currentTimeMillis()));

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));

        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());

        Range range1 = makeTestRange("20190314", "datatype1\u0000234");
        Range range2 = makeTestRange("20190314", "datatype1\u0000345");
        Set<Range> expectedRanges = Sets.newHashSet(range1, range2);

        RangeStream rangeStream = getRangeStream(helper).setLimitScanners(true);
        for (QueryPlan queryPlan : rangeStream.streamPlans(script)) {
            for (Range range : queryPlan.getRanges()) {
                assertTrue("Tried to remove unexpected range " + range.toString() + " from expected ranges: " + expectedRanges.toString(),
                                expectedRanges.remove(range));
            }
        }
        assertTrue("Expected ranges not found in query plan: " + expectedRanges.toString(), expectedRanges.isEmpty());
    }

    @Test
    public void testNegatedNonIndexed() throws Exception {
        String originalQuery = "FOO == 'bag' && TACO != 'tacocat'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);

        config.setBeginDate(new Date(0));
        config.setEndDate(new Date(System.currentTimeMillis()));

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));

        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());

        Range range1 = makeTestRange("20190314", "datatype1\u0000234");
        Range range2 = makeTestRange("20190314", "datatype1\u0000345");
        Set<Range> expectedRanges = Sets.newHashSet(range1, range2);

        RangeStream rangeStream = getRangeStream(helper).setLimitScanners(true);
        for (QueryPlan queryPlan : rangeStream.streamPlans(script)) {
            for (Range range : queryPlan.getRanges()) {
                assertTrue("Tried to remove unexpected range " + range.toString() + " from expected ranges: " + expectedRanges.toString(),
                                expectedRanges.remove(range));
            }
        }
        assertTrue("Expected ranges not found in query plan: " + expectedRanges.toString(), expectedRanges.isEmpty());
    }

    @Test
    public void testNegatedIndexWithResultsStandaloneNot() throws Exception {
        String originalQuery = "FOO == 'bag' && !(FOO == 'bar')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);

        config.setBeginDate(new Date(0));
        config.setEndDate(new Date(System.currentTimeMillis()));

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));

        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());

        Range range1 = makeTestRange("20190314", "datatype1\u0000234");
        Range range2 = makeTestRange("20190314", "datatype1\u0000345");
        Set<Range> expectedRanges = Sets.newHashSet(range1, range2);

        RangeStream rangeStream = getRangeStream(helper).setLimitScanners(true);
        for (QueryPlan queryPlan : rangeStream.streamPlans(script)) {
            for (Range range : queryPlan.getRanges()) {
                assertTrue("Tried to remove unexpected range " + range.toString() + " from expected ranges: " + expectedRanges.toString(),
                                expectedRanges.remove(range));
            }
        }
        assertTrue("Expected ranges not found in query plan: " + expectedRanges.toString(), expectedRanges.isEmpty());
    }

    @Test
    public void testNegatedIndexWithNoResultsStandaloneNot() throws Exception {
        String originalQuery = "FOO == 'bag' && !(FOO == 'tacocat')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);

        config.setBeginDate(new Date(0));
        config.setEndDate(new Date(System.currentTimeMillis()));

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));

        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());

        Range range1 = makeTestRange("20190314", "datatype1\u0000234");
        Range range2 = makeTestRange("20190314", "datatype1\u0000345");
        Set<Range> expectedRanges = Sets.newHashSet(range1, range2);

        RangeStream rangeStream = getRangeStream(helper).setLimitScanners(true);
        for (QueryPlan queryPlan : rangeStream.streamPlans(script)) {
            for (Range range : queryPlan.getRanges()) {
                assertTrue("Tried to remove unexpected range " + range.toString() + " from expected ranges: " + expectedRanges.toString(),
                                expectedRanges.remove(range));
            }
        }
        assertTrue("Expected ranges not found in query plan: " + expectedRanges.toString(), expectedRanges.isEmpty());
    }

    /**
     * Interesting case when the right side terms are indexed and the left side terms are not indexed.
     *
     * @throws Exception
     *             for unexpected issues
     */
    @Test
    public void testIntersectionOfTwoUnions() throws Exception {
        String originalQuery = "(FOO == 'bag' || FOO == 'bar') && (TACO == 'shell' || TACO == 'tacocat')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);

        config.setBeginDate(new Date(0));
        config.setEndDate(new Date(System.currentTimeMillis()));

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));

        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());
        helper.addFields(ImmutableSet.of("TACO"));

        Range range1 = makeTestRange("20190314", "datatype1\u0000234");
        Range range2 = makeTestRange("20190314", "datatype1\u0000345");
        Set<Range> expectedRanges = Sets.newHashSet(range1, range2);

        RangeStream rangeStream = getRangeStream(helper).setLimitScanners(true);
        CloseableIterable<QueryPlan> queryPlans = rangeStream.streamPlans(script);

        assertEquals(IndexStream.StreamContext.VARIABLE, rangeStream.context());
        Iterator<QueryPlan> iter = queryPlans.iterator();
        assertEquals(IndexStream.StreamContext.PRESENT, rangeStream.context());

        while (iter.hasNext()) {
            QueryPlan queryPlan = iter.next();
            for (Range range : queryPlan.getRanges()) {
                assertTrue("Tried to remove unexpected range " + range.toString() + " from expected ranges: " + expectedRanges.toString(),
                                expectedRanges.remove(range));
            }
        }
        assertTrue("Expected ranges not found in query plan: " + expectedRanges.toString(), expectedRanges.isEmpty());
    }

    @Test
    public void testIntersectionOfTwoUnionsAllIndexed() throws Exception {
        String originalQuery = "(FOO == 'bag' || FOO == 'bar') && (NUM == 'shell' || NUM == 'tacocat')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);

        config.setBeginDate(new Date(0));
        config.setEndDate(new Date(System.currentTimeMillis()));

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));
        dataTypes.putAll("NUM", Sets.newHashSet(new LcNoDiacriticsType()));

        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());

        RangeStream rangeStream = getRangeStream(helper).setLimitScanners(true);
        rangeStream.streamPlans(script);
        // streamPlans(script) to populate the StreamContext.
        assertEquals(IndexStream.StreamContext.ABSENT, rangeStream.context());
        assertFalse(rangeStream.iterator().hasNext());
    }

    @Test
    public void testNonExistentFieldInOr() throws Exception {
        String originalQuery = "FOO == 'bag' || CANDY_TYPE == 'candy corn'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);

        config.setBeginDate(new Date(0));
        config.setEndDate(new Date(System.currentTimeMillis()));

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));

        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());

        RangeStream rangeStream = getRangeStream(helper).setLimitScanners(true);
        rangeStream.streamPlans(script);
        // streamPlans(script) to populate the StreamContext.
        assertFalse(rangeStream.iterator().hasNext());
        assertEquals(IndexStream.StreamContext.ABSENT, rangeStream.context());
        // a non-existent field in a top level union means we cannot run this query. Logic changes if this union is nested within an intersection
    }

    @Test
    public void testNonExistentFieldInAnd() throws Exception {
        String originalQuery = "FOO == 'bag' && CANDY_TYPE == 'candy corn'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);

        config.setBeginDate(new Date(0));
        config.setEndDate(new Date(System.currentTimeMillis()));

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));
        dataTypes.putAll("CANDY_TYPE", Sets.newHashSet(new LcNoDiacriticsType()));

        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());

        RangeStream rangeStream = getRangeStream(helper).setLimitScanners(true);
        rangeStream.streamPlans(script);
        // streamPlans(script) to populate the StreamContext.
        assertFalse(rangeStream.iterator().hasNext());
        assertEquals(IndexStream.StreamContext.ABSENT, rangeStream.context());
    }

    @Test
    public void testDropTwoPredicates() throws Exception {
        String originalQuery = "LAUGH == 'bahahaha' && ( FOO == 'boohoo' || FOO == 'idontexist' || FOO == 'neitherdoi!' )";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        config.setBeginDate(sdf.parse("20190314"));
        config.setEndDate(sdf.parse("20190315"));
        config.setDatatypeFilter(Sets.newHashSet("datatype1", "datatype2"));

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));
        dataTypes.putAll("LAUGH", Sets.newHashSet(new LcNoDiacriticsType()));

        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());
        helper.addFields(Arrays.asList("FOO", "LAUGH"));

        // "20190314_10" never actually hit
        Set<Range> expectedRanges = Sets.newHashSet();
        for (String shard : Arrays.asList("20190314_0", "20190314_1", "20190314_100", "20190314_9")) {
            expectedRanges.add(makeShardedRange(shard));
        }

        RangeStream rangeStream = getRangeStream(helper).setLimitScanners(true);
        CloseableIterable<QueryPlan> queryPlans = rangeStream.streamPlans(script);
        // streamPlans(script) to populate the StreamContext.
        assertEquals(IndexStream.StreamContext.PRESENT, rangeStream.context());
        for (QueryPlan queryPlan : queryPlans) {
            for (Range range : queryPlan.getRanges()) {
                assertTrue("Tried to remove unexpected range " + range.toString() + "\nfrom expected ranges: " + expectedRanges, expectedRanges.remove(range));
            }
        }
        assertTrue("Expected ranges not found in query plan: " + expectedRanges, expectedRanges.isEmpty());
    }

    @Test
    public void whatDayIsItAnywayTest() throws ParseException {
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss");
        config.setBeginDate(sdf.parse("20200201 060000"));
        config.setEndDate(sdf.parse("20200202 040000"));
        List<Tuple2<String,IndexInfo>> fullFieldIndexScanList = RangeStream.createFullFieldIndexScanList(config, null);

        Assert.assertEquals(2, fullFieldIndexScanList.size());
    }

    // (A && B)
    @Test
    public void testIntersection_HighAndLowCardinality_withSeek() throws Exception {
        String originalQuery = "(FOO == 'lowest_card' && FOO == 'highest_card')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        config.setBeginDate(sdf.parse("20190310"));
        config.setEndDate(sdf.parse("20190320"));

        config.setDatatypeFilter(Sets.newHashSet("datatype1", "datatype2"));

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));
        dataTypes.putAll("LAUGH", Sets.newHashSet(new LcNoDiacriticsType()));

        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());

        // Create expected ranges verbosely, so it is obvious which shards contribute to the results.
        Range range1 = makeTestRange("20190310_1", "datatype1\u0000a.b.c");
        Range range2 = makeTestRange("20190314_22", "datatype1\u0000a.b.c");
        Range range3 = makeTestRange("20190315_49", "datatype1\u0000a.b.c");
        Set<Range> expectedRanges = Sets.newHashSet(range1, range2, range3);

        ScannerFactory scannerFactory = new ScannerFactory(config);
        RangeStream rangeStream = getRangeStream(helper);
        rangeStream.setLimitScanners(true);
        CloseableIterable<QueryPlan> queryPlans = rangeStream.streamPlans(script);
        assertEquals(IndexStream.StreamContext.PRESENT, rangeStream.context());
        for (QueryPlan queryPlan : queryPlans) {
            Iterable<Range> ranges = queryPlan.getRanges();
            for (Range range : ranges) {
                assertTrue("Tried to remove unexpected range " + range.toString() + "\nfrom expected ranges: " + expectedRanges.toString(),
                                expectedRanges.remove(range));
            }
        }
        assertTrue("Expected ranges not found in query plan: " + expectedRanges.toString(), expectedRanges.isEmpty());
    }

    // (A && (B || C))
    @Test
    public void testIntersection_NestedUnionOfHighCardinalityTerm_withSeek() throws Exception {
        String originalQuery = "(FOO == 'lowest_card' && (FOO == 'high_card' || FOO == 'highest_card'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        config.setBeginDate(sdf.parse("20190310"));
        config.setEndDate(sdf.parse("20190320"));

        config.setDatatypeFilter(Sets.newHashSet("datatype1", "datatype2"));

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));
        dataTypes.putAll("LAUGH", Sets.newHashSet(new LcNoDiacriticsType()));

        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());

        // Create expected ranges verbosely, so it is obvious which shards contribute to the results.
        Range range1 = makeTestRange("20190310_1", "datatype1\u0000a.b.c");
        Range range2 = makeTestRange("20190314_22", "datatype1\u0000a.b.c");
        Range range3 = makeTestRange("20190315_49", "datatype1\u0000a.b.c");
        Set<Range> expectedRanges = Sets.newHashSet(range1, range2, range3);

        RangeStream rangeStream = getRangeStream(helper);
        rangeStream.setLimitScanners(true);
        CloseableIterable<QueryPlan> queryPlans = rangeStream.streamPlans(script);
        assertEquals(IndexStream.StreamContext.PRESENT, rangeStream.context());
        for (QueryPlan queryPlan : queryPlans) {
            Iterable<Range> ranges = queryPlan.getRanges();
            for (Range range : ranges) {
                assertTrue("Tried to remove unexpected range " + range.toString() + "\nfrom expected ranges: " + expectedRanges.toString(),
                                expectedRanges.remove(range));
            }
        }
        assertTrue("Expected ranges not found in query plan: " + expectedRanges.toString(), expectedRanges.isEmpty());
    }

    // A && (B || C)
    @Test
    public void testIntersection_NestedUnionOfLowCardinalityTerm_withSeek() throws Exception {
        String originalQuery = "(FOO == 'highest_card' && (FOO == 'low_card' || FOO == 'lowest_card'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        config.setBeginDate(sdf.parse("20190310"));
        config.setEndDate(sdf.parse("20190320"));

        config.setDatatypeFilter(Sets.newHashSet("datatype1", "datatype2"));

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));
        dataTypes.putAll("LAUGH", Sets.newHashSet(new LcNoDiacriticsType()));

        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());

        Range range1 = makeTestRange("20190310_1", "datatype1\u0000a.b.c");
        Range range2 = makeTestRange("20190312_1", "datatype1\u0000a.b.c");
        Range range3 = makeTestRange("20190314_22", "datatype1\u0000a.b.c");
        Range range4 = makeTestRange("20190315_33", "datatype1\u0000a.b.c");
        Range range5 = makeTestRange("20190315_49", "datatype1\u0000a.b.c");
        Range range6 = makeTestRange("20190317_1", "datatype1\u0000a.b.c");
        Set<Range> expectedRanges = Sets.newHashSet(range1, range2, range3, range4, range5, range6);

        RangeStream rangeStream = getRangeStream(helper);
        rangeStream.setLimitScanners(true);
        CloseableIterable<QueryPlan> queryPlans = rangeStream.streamPlans(script);
        assertEquals(IndexStream.StreamContext.PRESENT, rangeStream.context());
        for (QueryPlan queryPlan : queryPlans) {
            Iterable<Range> ranges = queryPlan.getRanges();
            for (Range range : ranges) {
                assertTrue("Tried to remove unexpected range " + range.toString() + "\nfrom expected ranges: " + expectedRanges.toString(),
                                expectedRanges.remove(range));
            }
        }
        assertTrue("Expected ranges not found in query plan: " + expectedRanges.toString(), expectedRanges.isEmpty());
    }

    // A || (B && C)
    @Test
    public void testUnion_HighCardWithNestedIntersectionOfLowCardTerms_withSeek() throws Exception {
        String originalQuery = "(FOO == 'low_card' || (FOO == 'high_card' && FOO == 'highest_card'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        config.setBeginDate(sdf.parse("20190310"));
        config.setEndDate(sdf.parse("20190320"));

        config.setDatatypeFilter(Sets.newHashSet("datatype1", "datatype2"));

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));
        dataTypes.putAll("LAUGH", Sets.newHashSet(new LcNoDiacriticsType()));

        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());

        // Union with the Intersection of the high & highest cardinality terms means we hit every 'high_card' shard.
        Set<Range> expectedRanges = new HashSet<>();
        for (int day = 0; day < 8; day += 2) {
            for (int ii = 1; ii < 50; ii++) {
                expectedRanges.add(makeTestRange("2019031" + day + "_" + ii, "datatype1\u0000a.b.c"));
                expectedRanges.add(makeTestRange("2019031" + day + "_" + ii, "datatype1\u0000d.e.f"));
            }
        }
        expectedRanges.add(makeTestRange("20190315_33", "datatype1\u0000a.b.c"));
        expectedRanges.add(makeTestRange("20190317_1", "datatype1\u0000a.b.c"));

        RangeStream rangeStream = getRangeStream(helper);
        rangeStream.setLimitScanners(true);
        CloseableIterable<QueryPlan> queryPlans = rangeStream.streamPlans(script);
        assertEquals(IndexStream.StreamContext.PRESENT, rangeStream.context());
        for (QueryPlan queryPlan : queryPlans) {
            Iterable<Range> ranges = queryPlan.getRanges();
            for (Range range : ranges) {
                assertTrue("Tried to remove unexpected range " + range.toString() + "\nfrom expected ranges: " + expectedRanges.toString(),
                                expectedRanges.remove(range));
            }
        }
        assertTrue("Expected ranges not found in query plan: " + expectedRanges.toString(), expectedRanges.isEmpty());
    }

    // (A && B) || (C && D)
    @Test
    public void testUnion_OfTwoNestedIntersections_LeftLowCardTerms_withSeek() throws Exception {
        String originalQuery = "(FOO == 'low_card' && FOO == 'lowest_card') || (FOO == 'high_card' && FOO == 'highest_card')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        config.setBeginDate(sdf.parse("20190310"));
        config.setEndDate(sdf.parse("20190320"));

        config.setDatatypeFilter(Sets.newHashSet("datatype1", "datatype2"));

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));
        dataTypes.putAll("LAUGH", Sets.newHashSet(new LcNoDiacriticsType()));

        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());

        Set<Range> expectedRanges = new HashSet<>();
        for (int day = 0; day < 8; day += 2) {
            for (int ii = 1; ii < 50; ii++) {
                expectedRanges.add(makeTestRange("2019031" + day + "_" + ii, "datatype1\u0000a.b.c"));
                expectedRanges.add(makeTestRange("2019031" + day + "_" + ii, "datatype1\u0000d.e.f"));
            }
        }

        RangeStream rangeStream = getRangeStream(helper);
        rangeStream.setLimitScanners(true);
        CloseableIterable<QueryPlan> queryPlans = rangeStream.streamPlans(script);
        assertEquals(IndexStream.StreamContext.PRESENT, rangeStream.context());
        for (QueryPlan queryPlan : queryPlans) {
            Iterable<Range> ranges = queryPlan.getRanges();
            for (Range range : ranges) {
                assertTrue("Tried to remove unexpected range " + range.toString() + "\nfrom expected ranges: " + expectedRanges.toString(),
                                expectedRanges.remove(range));
            }
        }
        assertTrue("Expected ranges not found in query plan: " + expectedRanges.toString(), expectedRanges.isEmpty());
    }

    // (A || B) && (C || D)
    @Test
    public void testIntersection_OfTwoNestedUnions_LeftLowCardTerms_withSeek() throws Exception {
        String originalQuery = "(FOO == 'low_card' || FOO == 'lowest_card') && (FOO == 'high_card' || FOO == 'highest_card')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        config.setBeginDate(sdf.parse("20190310"));
        config.setEndDate(sdf.parse("20190320"));

        config.setDatatypeFilter(Sets.newHashSet("datatype1", "datatype2"));

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));
        dataTypes.putAll("LAUGH", Sets.newHashSet(new LcNoDiacriticsType()));

        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());

        // Create expected ranges verbosely, so it is obvious which shards contribute to the results.
        Range range1 = makeTestRange("20190310_1", "datatype1\u0000a.b.c");
        Range range2 = makeTestRange("20190312_1", "datatype1\u0000a.b.c");
        Range range3 = makeTestRange("20190314_22", "datatype1\u0000a.b.c");
        Range range4 = makeTestRange("20190315_33", "datatype1\u0000a.b.c");
        Range range5 = makeTestRange("20190315_49", "datatype1\u0000a.b.c");
        Range range6 = makeTestRange("20190317_1", "datatype1\u0000a.b.c");
        Set<Range> expectedRanges = Sets.newHashSet(range1, range2, range3, range4, range5, range6);

        RangeStream rangeStream = getRangeStream(helper);
        rangeStream.setLimitScanners(true);
        CloseableIterable<QueryPlan> queryPlans = rangeStream.streamPlans(script);
        assertEquals(IndexStream.StreamContext.PRESENT, rangeStream.context());
        for (QueryPlan queryPlan : queryPlans) {
            Iterable<Range> ranges = queryPlan.getRanges();
            for (Range range : ranges) {
                assertTrue("Tried to remove unexpected range " + range.toString() + "\nfrom expected ranges: " + expectedRanges.toString(),
                                expectedRanges.remove(range));
            }
        }
        assertTrue("Expected ranges not found in query plan: " + expectedRanges.toString(), expectedRanges.isEmpty());
    }

    // A && B when A term is day ranges and B term is a single shard range within the last day.
    @Test
    public void testIntersection_ofDayRangesAndShardRange() throws Exception {
        String originalQuery = "FOO == 'day_ranges' && FOO == 'shard_range'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        config.setBeginDate(sdf.parse("20190310"));
        config.setEndDate(sdf.parse("20190320"));

        config.setDatatypeFilter(Sets.newHashSet("datatype1", "datatype2"));

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));
        dataTypes.putAll("LAUGH", Sets.newHashSet(new LcNoDiacriticsType()));

        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());

        RangeStream rangeStream = getRangeStream(helper);
        rangeStream.setLimitScanners(true);
        CloseableIterable<QueryPlan> queryPlans = rangeStream.streamPlans(script);
        assertEquals(IndexStream.StreamContext.ABSENT, rangeStream.context());
        assertFalse(queryPlans.iterator().hasNext());
    }

    private RangeStream getRangeStream(MetadataHelper helper) {
        ScannerFactory scannerFactory = new ScannerFactory(config);
        return new RangeStream(config, scannerFactory, helper);
    }
}
