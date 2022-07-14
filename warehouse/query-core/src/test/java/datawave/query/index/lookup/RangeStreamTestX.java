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
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static datawave.common.test.utils.query.RangeFactoryForTests.makeDayRange;
import static datawave.common.test.utils.query.RangeFactoryForTests.makeShardedRange;
import static datawave.common.test.utils.query.RangeFactoryForTests.makeTestRange;
import static datawave.util.TableName.SHARD_INDEX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Cover some basic tests involving streams of shards for a basic set of query structures. Only tests for correctness of shard intersection, not that the
 * underlying IndexInfo objects correctly intersect.
 *
 * 6 Basic Types of Query Structures
 *
 * 3 Stream Type Combinations (All shards, shards and days, all days)
 *
 * 2 Types of Unequal Stream Start/Stop (different start/end day)
 *
 * 2 Types of Uneven Stream Start/Stop (same start/end day, different shard)
 * 
 * 1 Type of Tick-Tock Shards (alternating shards such that no hits are produced for a day)
 *
 * 1 Type of Missing Shards (missing shards should drop terms from query)
 */
public class RangeStreamTestX {
    
    // A && B
    // A || B
    // A && ( B || C )
    // A || ( B && C )
    // (A && B) || (C && D)
    // (A || B) && (C || D)
    
    private static InMemoryInstance instance = new InMemoryInstance(RangeStreamTestX.class.toString());
    private static Connector connector;
    private ShardQueryConfiguration config;
    
    @BeforeClass
    public static void setupAccumulo() throws Exception {
        // Zero byte password, so secure it hurts.
        connector = instance.getConnector("", new PasswordToken(new byte[0]));
        connector.tableOperations().create(SHARD_INDEX);
        
        BatchWriter bw = connector.createBatchWriter(SHARD_INDEX, new BatchWriterConfig().setMaxLatency(10, TimeUnit.SECONDS).setMaxMemory(100000L)
                        .setMaxWriteThreads(1));
        
        // Some values
        Value valueForShard = buildValueForShard();
        Value valueForDay = buildValueForDay();
        
        // --------------- Hits on every shard for every day
        Mutation m = new Mutation("all");
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                m.put(new Text("A"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForShard);
                m.put(new Text("B"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForShard);
                m.put(new Text("C"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForShard);
                m.put(new Text("D"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForShard);
            }
        }
        bw.addMutation(m);
        
        // --------------- Hits on every shard for every day, shards will roll to a day range.
        m = new Mutation("all_day");
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                m.put(new Text("A"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForDay);
                m.put(new Text("B"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForDay);
                m.put(new Text("C"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForDay);
                m.put(new Text("D"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForDay);
            }
        }
        bw.addMutation(m);
        
        // --------------- Unequal start, each term misses day 1
        m = new Mutation("unequal_start");
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                if (ii > 1) {
                    m.put(new Text("A"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForShard);
                    m.put(new Text("B"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForShard);
                    m.put(new Text("C"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForShard);
                    m.put(new Text("D"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForShard);
                }
            }
        }
        bw.addMutation(m);
        
        // --------------- Unequal start, each term misses day 1, each term rolls to a day range
        m = new Mutation("unequal_start_day");
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                if (ii > 1) {
                    m.put(new Text("A"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForDay);
                    m.put(new Text("B"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForDay);
                    m.put(new Text("C"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForDay);
                    m.put(new Text("D"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForDay);
                }
            }
        }
        bw.addMutation(m);
        
        // --------------- Unequal end, each term misses day 5
        m = new Mutation("unequal_stop");
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                if (ii < 5) {
                    m.put(new Text("A"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForShard);
                    m.put(new Text("B"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForShard);
                    m.put(new Text("C"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForShard);
                    m.put(new Text("D"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForShard);
                }
            }
        }
        bw.addMutation(m);
        
        // --------------- Unequal end, each term misses day 5
        m = new Mutation("unequal_stop_day");
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                if (ii < 5) {
                    m.put(new Text("A"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForDay);
                    m.put(new Text("B"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForDay);
                    m.put(new Text("C"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForDay);
                    m.put(new Text("D"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForDay);
                }
            }
        }
        bw.addMutation(m);
        
        // --------------- Uneven start, each term will miss the first two shards of each day
        m = new Mutation("uneven_start");
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                if (jj > 0) {
                    m.put(new Text("A"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForShard);
                    m.put(new Text("B"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForShard);
                    m.put(new Text("C"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForShard);
                    m.put(new Text("D"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForShard);
                }
            }
        }
        bw.addMutation(m);
        
        // --------------- Uneven start, each term will miss the first two shards of each day, each term will roll to a day range
        m = new Mutation("uneven_start_day");
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                if (jj > 0) {
                    m.put(new Text("A"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForDay);
                    m.put(new Text("B"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForDay);
                    m.put(new Text("C"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForDay);
                    m.put(new Text("D"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForDay);
                }
            }
        }
        bw.addMutation(m);
        
        // --------------- Uneven end, each term will miss the last two shards of each day
        m = new Mutation("uneven_stop");
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                if (jj < 9) {
                    m.put(new Text("A"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForShard);
                    m.put(new Text("B"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForShard);
                    m.put(new Text("C"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForShard);
                    m.put(new Text("D"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForShard);
                }
            }
        }
        bw.addMutation(m);
        
        // --------------- Uneven end, each term will miss the last two shards of each day, each term will roll to a day range.
        m = new Mutation("uneven_stop_day");
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                if (jj < 9) {
                    m.put(new Text("A"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForDay);
                    m.put(new Text("B"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForDay);
                    m.put(new Text("C"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForDay);
                    m.put(new Text("D"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForDay);
                }
            }
        }
        bw.addMutation(m);
        
        // --------------- Tick-tock shard 3, each term takes a turn
        m = new Mutation("tick_tock");
        int counter = 0;
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                if (ii == 3) {
                    int mod = counter % 4;
                    switch (mod) {
                        case 0:
                            m.put(new Text("A"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForShard);
                            break;
                        case 1:
                            m.put(new Text("B"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForShard);
                            break;
                        case 2:
                            m.put(new Text("C"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForShard);
                            break;
                        case 3:
                        default:
                            m.put(new Text("D"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForShard);
                    }
                } else {
                    m.put(new Text("A"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForShard);
                    m.put(new Text("B"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForShard);
                    m.put(new Text("C"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForShard);
                    m.put(new Text("D"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForShard);
                }
                counter++;
            }
        }
        bw.addMutation(m);
        
        // --------------- Tick-tock shard 3, each term takes a turn, shards roll to days except on day 3.
        m = new Mutation("tick_tock_day");
        counter = 0;
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                if (ii == 3) {
                    int mod = counter % 4;
                    switch (mod) {
                        case 0:
                            m.put(new Text("A"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForDay);
                            break;
                        case 1:
                            m.put(new Text("B"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForDay);
                            break;
                        case 2:
                            m.put(new Text("C"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForDay);
                            break;
                        case 3:
                        default:
                            m.put(new Text("D"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForDay);
                    }
                    counter++;
                } else {
                    m.put(new Text("A"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForDay);
                    m.put(new Text("B"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForDay);
                    m.put(new Text("C"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForDay);
                    m.put(new Text("D"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForDay);
                }
            }
        }
        bw.addMutation(m);
        
        // --------------- Missing shards for day 3
        m = new Mutation("missing_shards");
        for (int ii = 1; ii <= 5; ii++) {
            if (ii == 3) {
                continue;
            }
            for (int jj = 0; jj < 10; jj++) {
                m.put(new Text("A"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForShard);
                m.put(new Text("B"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForShard);
                m.put(new Text("C"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForShard);
                m.put(new Text("D"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForShard);
            }
        }
        bw.addMutation(m);
        
        // --------------- Missing shards for day 3, existing shards roll to day range
        m = new Mutation("missing_shards_day");
        for (int ii = 1; ii <= 5; ii++) {
            if (ii == 3) {
                continue;
            }
            for (int jj = 0; jj < 10; jj++) {
                m.put(new Text("A"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForDay);
                m.put(new Text("B"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForDay);
                m.put(new Text("C"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForDay);
                m.put(new Text("D"), new Text("2020010" + ii + "_" + jj + "\0datatype1"), valueForDay);
            }
        }
        bw.addMutation(m);
        
        // ---------------
        
        bw.flush();
        bw.close();
    }
    
    private static Value buildValueForShard() {
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.addUID("a.b.c");
        builder.setIGNORE(false);
        builder.setCOUNT(1);
        Uid.List list = builder.build();
        return new Value(list.toByteArray());
    }
    
    // A value that will roll into a day range.
    private static Value buildValueForDay() {
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.setIGNORE(true);
        builder.setCOUNT(5432);
        Uid.List list = builder.build();
        return new Value(list.toByteArray());
    }
    
    @Before
    public void setupTest() {
        config = new ShardQueryConfiguration();
        config.setConnector(connector);
        config.setShardsPerDayThreshold(20);
    }
    
    // A && B
    @Test
    public void testIntersection_ofShards() throws Exception {
        String query = "A == 'all' && B == 'all'";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                expectedQueryStrings.add("A == 'all' && B == 'all'");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && B
    @Test
    public void testIntersection_ofShards_unequalStart() throws Exception {
        String query = "A == 'all' && B == 'unequal_start'";
        
        // B term skips day 1
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 2; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                expectedQueryStrings.add("A == 'all' && B == 'unequal_start'");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && B
    @Test
    public void testIntersection_ofShards_unequalStop() throws Exception {
        String query = "A == 'all' && B == 'unequal_stop'";
        
        // Day 1 is skipped in the unequal start and Day 5 is skipped in the unequal end
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 4; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                expectedQueryStrings.add("A == 'all' && B == 'unequal_stop'");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && B
    @Test
    public void testIntersection_ofShards_unevenStart() throws Exception {
        String query = "A == 'all' && B == 'uneven_start'";
        
        // First shard is skipped for each day in the uneven start case.
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 1; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                expectedQueryStrings.add("A == 'all' && B == 'uneven_start'");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && B
    @Test
    public void testIntersection_ofShards_unevenStop() throws Exception {
        String query = "A == 'all' && B == 'uneven_stop'";
        
        // First and last shards are skipped for each day in the uneven start/end case.
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 9; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                expectedQueryStrings.add("A == 'all' && B == 'uneven_stop'");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && B
    @Test
    public void testIntersection_ofShards_missingShards() throws Exception {
        String query = "A == 'all' && B == 'missing_shards'";
        
        // Shards are missing for day 3
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            if (ii == 3)
                continue;
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                expectedQueryStrings.add("A == 'all' && B == 'missing_shards'");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && B
    @Test
    public void testIntersection_ofShards_tickTockShards() throws Exception {
        String query = "A == 'tick_tock' && B == 'tick_tock'";
        
        // No intersection exists between A & B for tick-tock shards for day 3
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                if (ii != 3) {
                    expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                    expectedQueryStrings.add("(A == 'tick_tock' && B == 'tick_tock')");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && B
    @Test
    public void testIntersection_ofShardAndDay() throws Exception {
        String query = "A == 'all_day' && B == 'all'";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                expectedQueryStrings.add("(B == 'all' && ((_Delayed_ = true) && (A == 'all_day')))");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && B
    @Test
    public void testIntersection_ofShardAndDay_unequalStart() throws Exception {
        String query = "A == 'unequal_start' && B == 'all_day'";
        
        // Day 1 is skipped in the unequal start and Day 5 is skipped in the unequal end
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 2; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                expectedQueryStrings.add("(A == 'unequal_start' && ((_Delayed_ = true) && (B == 'all_day')))");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && B
    @Test
    public void testIntersection_ofShardAndDay_unequalStop() throws Exception {
        String query = "A == 'unequal_stop' && B == 'all_day'";
        
        // Day 1 is skipped in the unequal start and Day 5 is skipped in the unequal end
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 4; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                expectedQueryStrings.add("(A == 'unequal_stop' && ((_Delayed_ = true) && (B == 'all_day')))");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && B
    @Test
    public void testIntersection_ofShardAndDay_unevenStart() throws Exception {
        String query = "A == 'uneven_start' && B == 'all_day'";
        
        // Shard 1 is skipped for every day
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 1; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                expectedQueryStrings.add("(A == 'uneven_start' && ((_Delayed_ = true) && (B == 'all_day')))");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && B
    @Test
    public void testIntersection_ofShardAndDay_unevenStop() throws Exception {
        String query = "A == 'uneven_stop' && B == 'all_day'";
        
        // Shard 9 is skipped for every day
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 9; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                expectedQueryStrings.add("(A == 'uneven_stop' && ((_Delayed_ = true) && (B == 'all_day')))");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && B
    @Test
    public void testIntersection_ofShardAndDay_missingShards() throws Exception {
        String query = "A == 'all_day' && B == 'missing_shards'";
        
        // Shards are missing for day 3
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            if (ii == 3)
                continue;
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                expectedQueryStrings.add("((_Delayed_ = true) && (A == 'all_day')) && B == 'missing_shards'");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && B
    @Test
    public void testIntersection_ofShardAndDay_tickTockShards() throws Exception {
        String query = "A == 'tick_tock' && B == 'tick_tock_day'";
        
        // No intersection exists between A & B for tick-tock shards for day 3
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                if (ii != 3) {
                    expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                    expectedQueryStrings.add("(A == 'tick_tock' && ((_Delayed_ = true) && (B == 'tick_tock_day')))");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && B
    @Test
    public void testIntersection_ofDays() throws Exception {
        String query = "A == 'all_day' && B == 'all_day'";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            expectedQueryStrings.add("(((_Delayed_ = true) && (A == 'all_day')) && ((_Delayed_ = true) && (B == 'all_day')))");
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && B
    @Test
    public void testIntersection_ofDays_unequalStart() throws Exception {
        String query = "A == 'all_day' && B == 'unequal_start_day'";
        
        // Day 1 is skipped in the unequal start and Day 5 is skipped in the unequal end
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 2; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            expectedQueryStrings.add("(((_Delayed_ = true) && (A == 'all_day')) && ((_Delayed_ = true) && (B == 'unequal_start_day')))");
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && B
    @Test
    public void testIntersection_ofDays_unequalStop() throws Exception {
        String query = "A == 'all_day' && B == 'unequal_stop_day'";
        
        // Day 1 is skipped in the unequal start and Day 5 is skipped in the unequal end
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 4; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            expectedQueryStrings.add("(((_Delayed_ = true) && (A == 'all_day')) && ((_Delayed_ = true) && (B == 'unequal_stop_day')))");
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && B
    @Test
    public void testIntersection_ofDays_unevenStart() throws Exception {
        String query = "A == 'all_day' && B == 'uneven_start_day'";
        
        // First and last shards are skipped for each day in the uneven start/end case.
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            expectedQueryStrings.add("(((_Delayed_ = true) && (A == 'all_day')) && ((_Delayed_ = true) && (B == 'uneven_start_day')))");
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && B
    @Test
    public void testIntersection_ofDays_unevenStop() throws Exception {
        String query = "A == 'all_day' && B == 'uneven_stop_day'";
        
        // First and last shards are skipped for each day in the uneven start/end case.
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            expectedQueryStrings.add("(((_Delayed_ = true) && (A == 'all_day')) && ((_Delayed_ = true) && (B == 'uneven_stop_day')))");
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && B
    @Test
    public void testIntersection_ofDays_missingShards() throws Exception {
        String query = "A == 'all_day' && B == 'missing_shards_day'";
        
        // Shards are missing for day 3
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            if (ii == 3)
                continue;
            expectedRanges.add(makeDayRange("2020010" + ii));
            expectedQueryStrings.add("(((_Delayed_ = true) && (A == 'all_day')) && ((_Delayed_ = true) && (B == 'missing_shards_day')))");
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && B
    @Test
    public void testIntersection_ofDays_tickTockShards() throws Exception {
        String query = "A == 'tick_tock_day' && B == 'tick_tock_day'";
        
        // No intersection exists between A & B for tick-tock shards for day 3
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        
        for (int ii = 1; ii <= 5; ii++) {
            if (ii != 3) {
                expectedRanges.add(makeDayRange("2020010" + ii));
                expectedQueryStrings.add("(((_Delayed_ = true) && (A == 'tick_tock_day')) && ((_Delayed_ = true) && (B == 'tick_tock_day')))");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || B
    @Test
    public void testUnion_ofShards() throws Exception {
        String query = "A == 'all' || B == 'all'";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                expectedQueryStrings.add("A == 'all' || B == 'all'");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || B
    @Test
    public void testUnion_ofShards_unequalStart() throws Exception {
        String query = "A == 'all' || B == 'unequal_start'";
        
        // Day 1 is skipped in the unequal start and Day 5 is skipped in the unequal end
        List<Range> expectedRanges = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
            }
        }
        
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 0; ii < 10; ii++)
            expectedQueryStrings.add("A == 'all'");
        
        for (int ii = 0; ii < 40; ii++)
            expectedQueryStrings.add("A == 'all' || B == 'unequal_start'");
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || B
    @Test
    public void testUnion_ofShards_unequalStop() throws Exception {
        String query = "A == 'all' || B == 'unequal_stop'";
        
        // Day 1 is skipped in the unequal start and Day 5 is skipped in the unequal end
        List<Range> expectedRanges = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
            }
        }
        
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 0; ii < 40; ii++)
            expectedQueryStrings.add("A == 'all' || B == 'unequal_stop'");
        
        for (int ii = 0; ii < 10; ii++)
            expectedQueryStrings.add("A == 'all'");
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || B
    @Test
    public void testUnion_ofShards_unevenStart() throws Exception {
        String query = "A == 'all' || B == 'uneven_start'";
        
        // First and last shards are skipped for each day in the uneven start/end case.
        List<Range> expectedRanges = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
            }
        }
        
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 0; ii < 50; ii++) {
            int mod = ii % 10;
            if (mod == 0) {
                expectedQueryStrings.add("A == 'all'");
            } else {
                expectedQueryStrings.add("A == 'all' || B == 'uneven_start'");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || B
    @Test
    public void testUnion_ofShards_unevenStop() throws Exception {
        String query = "A == 'all' || B == 'uneven_stop'";
        
        // First and last shards are skipped for each day in the uneven start/end case.
        List<Range> expectedRanges = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
            }
        }
        
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 0; ii < 50; ii++) {
            int mod = ii % 10;
            if (mod == 9) {
                expectedQueryStrings.add("A == 'all'");
            } else {
                expectedQueryStrings.add("A == 'all' || B == 'uneven_stop'");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || B
    @Test
    public void testUnion_ofShards_missingShards() throws Exception {
        String query = "A == 'all' || B == 'missing_shards'";
        
        // Shards are missing for day 3
        List<Range> expectedRanges = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
            }
        }
        
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 0; ii < 50; ii++) {
            if (ii >= 20 && ii < 30) {
                expectedQueryStrings.add("A == 'all'");
            } else {
                expectedQueryStrings.add("A == 'all' || B == 'missing_shards'");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || B
    @Test
    public void testUnion_ofShards_tickTockShards() throws Exception {
        String query = "A == 'tick_tock' || B == 'tick_tock'";
        
        // Only hit on B's shards
        List<Range> expectedRanges = new ArrayList<>();
        int mod;
        int counter = 0;
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                if (ii == 3) {
                    mod = counter % 4;
                    if (mod == 0 || mod == 1) {
                        expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                    }
                } else {
                    expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                }
                counter++;
            }
        }
        
        List<String> expectedQueryStrings = new ArrayList<>();
        counter = 0;
        for (int ii = 0; ii < 50; ii++) {
            if (ii >= 20 && ii < 30) {
                mod = counter % 4;
                if (mod == 0) {
                    expectedQueryStrings.add("A == 'tick_tock'");
                } else if (mod == 1) {
                    expectedQueryStrings.add("B == 'tick_tock'");
                }
            } else {
                expectedQueryStrings.add("(A == 'tick_tock' || B == 'tick_tock')");
            }
            counter++;
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || B
    @Test
    public void testUnion_ofShardAndDay() throws Exception {
        String query = "A == 'all' || B == 'all_day'";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            expectedQueryStrings.add("(A == 'all' || ((_Delayed_ = true) && (B == 'all_day')))");
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || B
    @Test
    public void testUnion_ofShardAndDay_unequalStart() throws Exception {
        String query = "A == 'all' || B == 'unequal_start_day'";
        
        // B term skips day 1
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            if (ii == 1) {
                for (int jj = 0; jj < 10; jj++) {
                    expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                    expectedQueryStrings.add("A == 'all'");
                }
            } else {
                expectedRanges.add(makeDayRange("2020010" + ii));
                expectedQueryStrings.add("A == 'all' || ((_Delayed_ = true) && (B == 'unequal_start_day'))");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || B
    @Test
    public void testUnion_ofShardAndDay_unequalStop() throws Exception {
        String query = "A == 'all' || B == 'unequal_stop_day'";
        
        // B term skips day 1
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            if (ii == 5) {
                for (int jj = 0; jj < 10; jj++) {
                    expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                    expectedQueryStrings.add("A == 'all'");
                }
            } else {
                expectedRanges.add(makeDayRange("2020010" + ii));
                expectedQueryStrings.add("A == 'all' || ((_Delayed_ = true) && (B == 'unequal_stop_day'))");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || B
    @Test
    public void testUnion_ofShardAndDay_unevenStart() throws Exception {
        String query = "A == 'all' || B == 'uneven_start_day'";
        
        // B term skips day 1
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            expectedQueryStrings.add("A == 'all' || ((_Delayed_ = true) && (B == 'uneven_start_day'))");
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || B
    @Test
    public void testUnion_ofShardAndDay_unevenStop() throws Exception {
        String query = "A == 'all' || B == 'uneven_stop_day'";
        
        // B term skips day 1
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            expectedQueryStrings.add("A == 'all' || ((_Delayed_ = true) && (B == 'uneven_stop_day'))");
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || B
    @Test
    public void testUnion_ofShardAndDay_missingShards() throws Exception {
        String query = "A == 'all' || B == 'missing_shards_day'";
        
        // Shards are missing for day 3
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            if (ii == 3) {
                for (int jj = 0; jj < 10; jj++) {
                    expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                    expectedQueryStrings.add("A == 'all'");
                }
            } else {
                expectedRanges.add(makeDayRange("2020010" + ii));
                expectedQueryStrings.add("A == 'all' || ((_Delayed_ = true) && (B == 'missing_shards_day'))");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || B
    @Test
    public void testUnion_ofShardAndDay_tickTockShards() throws Exception {
        String query = "A == 'all' || B == 'tick_tock_day'";
        
        // Shards are missing for day 3
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        
        expectedRanges.add(makeDayRange("20200101"));
        expectedRanges.add(makeDayRange("20200102"));
        expectedRanges.add(makeTestRange("20200103_0", "datatype1\0a.b.c"));
        expectedRanges.add(makeShardedRange("20200103_1"));
        expectedRanges.add(makeTestRange("20200103_2", "datatype1\0a.b.c"));
        expectedRanges.add(makeTestRange("20200103_3", "datatype1\0a.b.c"));
        expectedRanges.add(makeTestRange("20200103_4", "datatype1\0a.b.c"));
        expectedRanges.add(makeShardedRange("20200103_5"));
        expectedRanges.add(makeTestRange("20200103_6", "datatype1\0a.b.c"));
        expectedRanges.add(makeTestRange("20200103_7", "datatype1\0a.b.c"));
        expectedRanges.add(makeTestRange("20200103_8", "datatype1\0a.b.c"));
        expectedRanges.add(makeShardedRange("20200103_9"));
        expectedRanges.add(makeDayRange("20200104"));
        expectedRanges.add(makeDayRange("20200105"));
        
        expectedQueryStrings.add("A == 'all' || ((_Delayed_ = true) && (B == 'tick_tock_day'))");
        expectedQueryStrings.add("A == 'all' || ((_Delayed_ = true) && (B == 'tick_tock_day'))");
        expectedQueryStrings.add("A == 'all'");
        expectedQueryStrings.add("A == 'all' || B == 'tick_tock_day'");
        expectedQueryStrings.add("A == 'all'");
        expectedQueryStrings.add("A == 'all'");
        expectedQueryStrings.add("A == 'all'");
        expectedQueryStrings.add("A == 'all' || B == 'tick_tock_day'");
        expectedQueryStrings.add("A == 'all'");
        expectedQueryStrings.add("A == 'all'");
        expectedQueryStrings.add("A == 'all'");
        expectedQueryStrings.add("A == 'all' || B == 'tick_tock_day'");
        expectedQueryStrings.add("A == 'all' || ((_Delayed_ = true) && (B == 'tick_tock_day'))");
        expectedQueryStrings.add("A == 'all' || ((_Delayed_ = true) && (B == 'tick_tock_day'))");
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || B
    @Test
    public void testUnion_ofDays() throws Exception {
        String query = "A == 'all_day' || B == 'all_day'";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            expectedQueryStrings.add("(((_Delayed_ = true) && (A == 'all_day')) || ((_Delayed_ = true) && (B == 'all_day')))");
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || B
    @Test
    public void testUnion_ofDays_unequalStart() throws Exception {
        String query = "A == 'all_day' || B == 'unequal_start_day'";
        
        // Day 1 is skipped in the unequal start and Day 5 is skipped in the unequal end
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            if (ii == 1) {
                expectedQueryStrings.add("((_Delayed_ = true) && (A == 'all_day'))");
            } else {
                expectedQueryStrings.add("(((_Delayed_ = true) && (A == 'all_day')) || ((_Delayed_ = true) && (B == 'unequal_start_day')))");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || B
    @Test
    public void testUnion_ofDays_unequalStop() throws Exception {
        String query = "A == 'all_day' || B == 'unequal_stop_day'";
        
        // Day 1 is skipped in the unequal start and Day 5 is skipped in the unequal end
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            if (ii == 5) {
                expectedQueryStrings.add("((_Delayed_ = true) && (A == 'all_day'))");
            } else {
                expectedQueryStrings.add("(((_Delayed_ = true) && (A == 'all_day')) || ((_Delayed_ = true) && (B == 'unequal_stop_day')))");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || B
    @Test
    public void testUnion_ofDays_unevenStart() throws Exception {
        String query = "A == 'all_day' || B == 'uneven_start_day'";
        
        // First and last shards are skipped for each day in the uneven start/end case.
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            expectedQueryStrings.add("(((_Delayed_ = true) && (A == 'all_day')) || ((_Delayed_ = true) && (B == 'uneven_start_day')))");
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || B
    @Test
    public void testUnion_ofDays_unevenStop() throws Exception {
        String query = "A == 'all_day' || B == 'uneven_stop_day'";
        
        // First and last shards are skipped for each day in the uneven start/end case.
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            expectedQueryStrings.add("(((_Delayed_ = true) && (A == 'all_day')) || ((_Delayed_ = true) && (B == 'uneven_stop_day')))");
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || B
    @Test
    public void testUnion_ofDays_missingShards() throws Exception {
        String query = "A == 'all_day' || B == 'missing_shards_day'";
        
        // Shards are missing for day 3
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            if (ii == 3) {
                expectedQueryStrings.add("((_Delayed_ = true) && (A == 'all_day'))");
            } else {
                expectedQueryStrings.add("(((_Delayed_ = true) && (A == 'all_day')) || ((_Delayed_ = true) && (B == 'missing_shards_day')))");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || B
    @Test
    public void testUnion_ofDays_tickTockShards() throws Exception {
        String query = "A == 'tick_tock_day' || B == 'tick_tock_day'";
        
        // No intersection exists between A & B for tick-tock shards for day 3
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        
        for (int ii = 1; ii <= 5; ii++) {
            if (ii != 3) {
                expectedRanges.add(makeDayRange("2020010" + ii));
                expectedQueryStrings.add("(((_Delayed_ = true) && (A == 'tick_tock_day')) || ((_Delayed_ = true) && (B == 'tick_tock_day')))");
            } else {
                expectedRanges.add(makeShardedRange("20200103_0"));
                expectedRanges.add(makeShardedRange("20200103_1"));
                expectedRanges.add(makeShardedRange("20200103_4"));
                expectedRanges.add(makeShardedRange("20200103_5"));
                expectedRanges.add(makeShardedRange("20200103_8"));
                expectedRanges.add(makeShardedRange("20200103_9"));
                expectedQueryStrings.add("A == 'tick_tock_day'");
                expectedQueryStrings.add("B == 'tick_tock_day'");
                expectedQueryStrings.add("A == 'tick_tock_day'");
                expectedQueryStrings.add("B == 'tick_tock_day'");
                expectedQueryStrings.add("A == 'tick_tock_day'");
                expectedQueryStrings.add("B == 'tick_tock_day'");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && ( B || C )
    @Test
    public void testIntersection_withNestedUnion_allShards() throws Exception {
        String query = "A == 'all' && (B == 'all' || C == 'all')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                expectedQueryStrings.add("A == 'all' && (B == 'all' || C == 'all')");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && ( B || C )
    @Test
    public void testIntersection_withNestedUnion_allShards_unequalStart() throws Exception {
        String query = "A == 'all' && (B == 'all' || C == 'unequal_start')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii == 1) {
                    expectedQueryStrings.add("A == 'all' && B == 'all'");
                } else {
                    expectedQueryStrings.add("A == 'all' && (B == 'all' || C == 'unequal_start')");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && ( B || C )
    @Test
    public void testIntersection_withNestedUnion_allShards_unequalStop() throws Exception {
        String query = "A == 'all' && (B == 'all' || C == 'unequal_stop')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii == 5) {
                    expectedQueryStrings.add("A == 'all' && B == 'all'");
                } else {
                    expectedQueryStrings.add("A == 'all' && (B == 'all' || C == 'unequal_stop')");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && ( B || C )
    @Test
    public void testIntersection_withNestedUnion_allShards_unevenStart() throws Exception {
        String query = "A == 'all' && (B == 'all' || C == 'uneven_start')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (jj == 0) {
                    expectedQueryStrings.add("A == 'all' && B == 'all'");
                } else {
                    expectedQueryStrings.add("A == 'all' && (B == 'all' || C == 'uneven_start')");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && ( B || C )
    @Test
    public void testIntersection_withNestedUnion_allShards_unevenStop() throws Exception {
        String query = "A == 'all' && (B == 'all' || C == 'uneven_stop')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (jj == 9) {
                    expectedQueryStrings.add("A == 'all' && B == 'all'");
                } else {
                    expectedQueryStrings.add("A == 'all' && (B == 'all' || C == 'uneven_stop')");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && ( B || C )
    @Test
    public void testIntersection_withNestedUnion_allShards_missingShards() throws Exception {
        String query = "A == 'all' && (B == 'all' || C == 'missing_shards')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii == 3) {
                    expectedQueryStrings.add("A == 'all' && B == 'all'");
                } else {
                    expectedQueryStrings.add("A == 'all' && (B == 'all' || C == 'missing_shards')");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && ( B || C )
    @Test
    public void testIntersection_withNestedUnion_allShards_tickTockShards() throws Exception {
        String query = "A == 'all' && (B == 'all' || C == 'tick_tock')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii != 3 || jj == 2 || jj == 6) {
                    expectedQueryStrings.add("A == 'all' && (B == 'all' || C == 'tick_tock')");
                } else {
                    expectedQueryStrings.add("A == 'all' && B == 'all'");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && ( B || C )
    @Test
    public void testIntersection_withNestedUnion_allDays() throws Exception {
        String query = "A == 'all_day' && (B == 'all_day' || C == 'all_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            expectedQueryStrings
                            .add("(((_Delayed_ = true) && (A == 'all_day')) && (((_Delayed_ = true) && (B == 'all_day')) || ((_Delayed_ = true) && (C == 'all_day'))))");
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && ( B || C )
    @Test
    public void testIntersection_withNestedUnion_allDays_unequalStart() throws Exception {
        String query = "A == 'all_day' && (B == 'all_day' || C == 'unequal_start_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            if (ii == 1) {
                expectedQueryStrings.add("((_Delayed_ = true) && (A == 'all_day')) && (((_Delayed_ = true) && (B == 'all_day')))");
            } else {
                expectedQueryStrings
                                .add("(((_Delayed_ = true) && (A == 'all_day')) && (((_Delayed_ = true) && (B == 'all_day')) || ((_Delayed_ = true) && (C == 'unequal_start_day'))))");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && ( B || C )
    @Test
    public void testIntersection_withNestedUnion_allDays_unequalStop() throws Exception {
        String query = "A == 'all_day' && (B == 'all_day' || C == 'unequal_stop_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            if (ii == 5) {
                expectedQueryStrings.add("((_Delayed_ = true) && (A == 'all_day')) && (((_Delayed_ = true) && (B == 'all_day')))");
            } else {
                expectedQueryStrings
                                .add("(((_Delayed_ = true) && (A == 'all_day')) && (((_Delayed_ = true) && (B == 'all_day')) || ((_Delayed_ = true) && (C == 'unequal_stop_day'))))");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && ( B || C )
    @Test
    public void testIntersection_withNestedUnion_allDays_unevenStart() throws Exception {
        String query = "A == 'all_day' && (B == 'all_day' || C == 'uneven_start_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            expectedQueryStrings
                            .add("(((_Delayed_ = true) && (A == 'all_day')) && (((_Delayed_ = true) && (B == 'all_day')) || ((_Delayed_ = true) && (C == 'uneven_start_day'))))");
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && ( B || C )
    @Test
    public void testIntersection_withNestedUnion_allDays_unevenStop() throws Exception {
        String query = "A == 'all_day' && (B == 'all_day' || C == 'uneven_stop_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            expectedQueryStrings
                            .add("(((_Delayed_ = true) && (A == 'all_day')) && (((_Delayed_ = true) && (B == 'all_day')) || ((_Delayed_ = true) && (C == 'uneven_stop_day'))))");
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && ( B || C )
    @Test
    public void testIntersection_withNestedUnion_allDays_missingShards() throws Exception {
        String query = "A == 'all_day' && (B == 'all_day' || C == 'missing_shards_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            if (ii == 3) {
                expectedQueryStrings.add("(((_Delayed_ = true) && (A == 'all_day')) && ((_Delayed_ = true) && (B == 'all_day')))");
            } else {
                expectedQueryStrings
                                .add("(((_Delayed_ = true) && (A == 'all_day')) && (((_Delayed_ = true) && (B == 'all_day')) || ((_Delayed_ = true) && (C == 'missing_shards_day'))))");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && ( B || C )
    @Test
    public void testIntersection_withNestedUnion_allDays_tickTockShards() throws Exception {
        String query = "A == 'all_day' && (B == 'all_day' || C == 'tick_tock')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            expectedQueryStrings.add("(((_Delayed_ = true) && (A == 'all_day')) && (((_Delayed_ = true) && (B == 'all_day')) || C == 'tick_tock'))");
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && ( B || C )
    @Test
    public void testIntersection_dayWithNestedUnionOfShards() throws Exception {
        String query = "A == 'all_day' && (B == 'all' || C == 'all')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                expectedQueryStrings.add("(((_Delayed_ = true) && (A == 'all_day')) && (B == 'all' || C == 'all'))");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && ( B || C )
    @Test
    public void testIntersection_dayWithNestedUnionOfShards_unequalStart() throws Exception {
        String query = "A == 'all_day' && (B == 'all' || C == 'unequal_start')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii == 1) {
                    expectedQueryStrings.add("(((_Delayed_ = true) && (A == 'all_day')) && B == 'all')");
                } else {
                    expectedQueryStrings.add("(((_Delayed_ = true) && (A == 'all_day')) && (B == 'all' || C == 'unequal_start'))");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && ( B || C )
    @Test
    public void testIntersection_dayWithNestedUnionOfShards_unequalStop() throws Exception {
        String query = "A == 'all_day' && (B == 'all' || C == 'unequal_stop')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii == 5) {
                    expectedQueryStrings.add("(((_Delayed_ = true) && (A == 'all_day')) && B == 'all')");
                } else {
                    expectedQueryStrings.add("(((_Delayed_ = true) && (A == 'all_day')) && (B == 'all' || C == 'unequal_stop'))");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && ( B || C )
    @Test
    public void testIntersection_dayWithNestedUnionOfShards_unevenStart() throws Exception {
        String query = "A == 'all_day' && (B == 'all' || C == 'uneven_start')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (jj == 0) {
                    expectedQueryStrings.add("(((_Delayed_ = true) && (A == 'all_day')) && B == 'all')");
                } else {
                    expectedQueryStrings.add("(((_Delayed_ = true) && (A == 'all_day')) && (B == 'all' || C == 'uneven_start'))");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && ( B || C )
    @Test
    public void testIntersection_dayWithNestedUnionOfShards_unevenStop() throws Exception {
        String query = "A == 'all_day' && (B == 'all' || C == 'uneven_stop')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (jj == 9) {
                    expectedQueryStrings.add("(((_Delayed_ = true) && (A == 'all_day')) && B == 'all')");
                } else {
                    expectedQueryStrings.add("(((_Delayed_ = true) && (A == 'all_day')) && (B == 'all' || C == 'uneven_stop'))");
                }
            }
        }
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && ( B || C )
    @Test
    public void testIntersection_dayWithNestedUnionOfShards_missingShards() throws Exception {
        String query = "A == 'all_day' && (B == 'all' || C == 'missing_shards')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii == 3) {
                    expectedQueryStrings.add("(((_Delayed_ = true) && (A == 'all_day')) && B == 'all')");
                } else {
                    expectedQueryStrings.add("(((_Delayed_ = true) && (A == 'all_day')) && (B == 'all' || C == 'missing_shards'))");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && ( B || C )
    @Test
    public void testIntersection_dayWithNestedUnionOfShards_tickTockShards() throws Exception {
        String query = "A == 'all_day' && (B == 'all' || C == 'tick_tock')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii != 3 || jj == 2 || jj == 6) {
                    expectedQueryStrings.add("(((_Delayed_ = true) && (A == 'all_day')) && (B == 'all' || C == 'tick_tock'))");
                } else {
                    expectedQueryStrings.add("(((_Delayed_ = true) && (A == 'all_day')) && B == 'all')");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && ( B || C )
    @Test
    public void testIntersection_shardWithNestedUnionOfDays() throws Exception {
        String query = "A == 'all' && (B == 'all_day' || C == 'all_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                expectedQueryStrings.add("A == 'all' && (((_Delayed_ = true) && (B == 'all_day')) || ((_Delayed_ = true) && (C == 'all_day')))");
            }
        }
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && ( B || C )
    @Test
    public void testIntersection_shardWithNestedUnionOfDays_unequalStart() throws Exception {
        String query = "A == 'all' && (B == 'all_day' || C == 'unequal_start_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii == 1) {
                    expectedQueryStrings.add("A == 'all' && (((_Delayed_ = true) && (B == 'all_day')))");
                } else {
                    expectedQueryStrings.add("A == 'all' && (((_Delayed_ = true) && (B == 'all_day')) || ((_Delayed_ = true) && (C == 'unequal_start_day')))");
                }
            }
        }
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && ( B || C )
    @Test
    public void testIntersection_shardWithNestedUnionOfDays_unequalStop() throws Exception {
        String query = "A == 'all' && (B == 'all_day' || C == 'unequal_stop_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii == 5) {
                    expectedQueryStrings.add("A == 'all' && (((_Delayed_ = true) && (B == 'all_day')))");
                } else {
                    expectedQueryStrings.add("A == 'all' && (((_Delayed_ = true) && (B == 'all_day')) || ((_Delayed_ = true) && (C == 'unequal_stop_day')))");
                }
            }
        }
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && ( B || C )
    @Test
    public void testIntersection_shardWithNestedUnionOfDays_unevenStart() throws Exception {
        String query = "A == 'all' && (B == 'all_day' || C == 'uneven_start_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                expectedQueryStrings.add("A == 'all' && (((_Delayed_ = true) && (B == 'all_day')) || ((_Delayed_ = true) && (C == 'uneven_start_day')))");
            }
        }
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && ( B || C )
    @Test
    public void testIntersection_shardWithNestedUnionOfDays_unevenStop() throws Exception {
        String query = "A == 'all' && (B == 'all_day' || C == 'uneven_stop_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                expectedQueryStrings.add("A == 'all' && (((_Delayed_ = true) && (B == 'all_day')) || ((_Delayed_ = true) && (C == 'uneven_stop_day')))");
            }
        }
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && ( B || C )
    @Test
    public void testIntersection_shardWithNestedUnionOfDays_missingShards() throws Exception {
        String query = "A == 'all' && (B == 'all_day' || C == 'missing_shards_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii == 3) {
                    expectedQueryStrings.add("A == 'all' && (((_Delayed_ = true) && (B == 'all_day')))");
                } else {
                    expectedQueryStrings.add("A == 'all' && (((_Delayed_ = true) && (B == 'all_day')) || ((_Delayed_ = true) && (C == 'missing_shards_day')))");
                }
            }
        }
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A && ( B || C )
    @Test
    public void testIntersection_shardWithNestedUnionOfDays_tickTockShards() throws Exception {
        String query = "A == 'all' && (B == 'all_day' || C == 'tick_tock_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii == 3) {
                    // Not enough shards to force rolling the C-term to a day range, thus no delay.
                    expectedQueryStrings.add("A == 'all' && (((_Delayed_ = true) && (B == 'all_day')) || (C == 'tick_tock_day'))");
                } else {
                    expectedQueryStrings.add("A == 'all' && (((_Delayed_ = true) && (B == 'all_day')) || ((_Delayed_ = true) && (C == 'tick_tock_day')))");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || ( B && C )
    @Test
    public void testUnion_withNestedIntersection_allShards() throws Exception {
        String query = "A == 'all' || (B == 'all' && C == 'all')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                expectedQueryStrings.add("A == 'all' || (B == 'all' && C == 'all')");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || ( B && C )
    @Test
    public void testUnion_withNestedIntersection_allShards_unequalStart() throws Exception {
        String query = "A == 'all' || (B == 'all' && C == 'unequal_start')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii == 1) {
                    // Can't intersect B-term with a non-existent C-term. Whole intersection is dropped for day 1.
                    expectedQueryStrings.add("A == 'all'");
                } else {
                    expectedQueryStrings.add("A == 'all' || (B == 'all' && C == 'unequal_start')");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || ( B && C )
    @Test
    public void testUnion_withNestedIntersection_allShards_unequalStop() throws Exception {
        String query = "A == 'all' || (B == 'all' && C == 'unequal_stop')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii == 5) {
                    // Can't intersect B-term with a non-existent C-term. Whole intersection is dropped for day 5.
                    expectedQueryStrings.add("A == 'all'");
                } else {
                    expectedQueryStrings.add("A == 'all' || (B == 'all' && C == 'unequal_stop')");
                }
            }
        }
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || ( B && C )
    @Test
    public void testUnion_withNestedIntersection_allShards_unevenStart() throws Exception {
        String query = "A == 'all' || (B == 'all' && C == 'uneven_start')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (jj == 0) {
                    // Can't intersect B-term with a non-existent C-term. Whole intersection is dropped for first shard of each day.
                    expectedQueryStrings.add("A == 'all'");
                } else {
                    expectedQueryStrings.add("A == 'all' || (B == 'all' && C == 'uneven_start')");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || ( B && C )
    @Test
    public void testUnion_withNestedIntersection_allShards_unevenStop() throws Exception {
        String query = "A == 'all' || (B == 'all' && C == 'uneven_stop')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (jj == 9) {
                    // Can't intersect B-term with a non-existent C-term. Whole intersection is dropped for last shard of each day.
                    expectedQueryStrings.add("A == 'all'");
                } else {
                    expectedQueryStrings.add("A == 'all' || (B == 'all' && C == 'uneven_stop')");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || ( B && C )
    @Test
    public void testUnion_withNestedIntersection_allShards_missingShards() throws Exception {
        String query = "A == 'all' || (B == 'all' && C == 'missing_shards')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii == 3) {
                    expectedQueryStrings.add("A == 'all'");
                } else {
                    expectedQueryStrings.add("A == 'all' || (B == 'all' && C == 'missing_shards')");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || ( B && C )
    @Test
    public void testUnion_withNestedIntersection_allShards_tickTockShards() throws Exception {
        String query = "A == 'all' || (B == 'all' && C == 'tick_tock')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii != 3 || jj == 2 || jj == 6) {
                    expectedQueryStrings.add("A == 'all' || (B == 'all' && C == 'tick_tock')");
                } else {
                    expectedQueryStrings.add("A == 'all'");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || ( B && C )
    @Test
    public void testUnion_withNestedIntersection_allDays() throws Exception {
        String query = "A == 'all_day' || (B == 'all_day' && C == 'all_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            expectedQueryStrings
                            .add("(((_Delayed_ = true) && (A == 'all_day')) || (((_Delayed_ = true) && (B == 'all_day')) && ((_Delayed_ = true) && (C == 'all_day'))))");
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || ( B && C )
    @Test
    public void testUnion_withNestedIntersection_allDays_unequalStart() throws Exception {
        String query = "A == 'all_day' || (B == 'all_day' && C == 'unequal_start_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            if (ii == 1) {
                expectedQueryStrings.add("((_Delayed_ = true) && (A == 'all_day'))");
            } else {
                expectedQueryStrings
                                .add("(((_Delayed_ = true) && (A == 'all_day')) || (((_Delayed_ = true) && (B == 'all_day')) && ((_Delayed_ = true) && (C == 'unequal_start_day'))))");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || ( B && C )
    @Test
    public void testUnion_withNestedIntersection_allDays_unequalStop() throws Exception {
        String query = "A == 'all_day' || (B == 'all_day' && C == 'unequal_stop_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            if (ii == 5) {
                expectedQueryStrings.add("((_Delayed_ = true) && (A == 'all_day'))");
            } else {
                expectedQueryStrings
                                .add("(((_Delayed_ = true) && (A == 'all_day')) || (((_Delayed_ = true) && (B == 'all_day')) && ((_Delayed_ = true) && (C == 'unequal_stop_day'))))");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || ( B && C )
    @Test
    public void testUnion_withNestedIntersection_allDays_unevenStart() throws Exception {
        String query = "A == 'all_day' || (B == 'all_day' && C == 'uneven_start_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            expectedQueryStrings
                            .add("(((_Delayed_ = true) && (A == 'all_day')) || (((_Delayed_ = true) && (B == 'all_day')) && ((_Delayed_ = true) && (C == 'uneven_start_day'))))");
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || ( B && C )
    @Test
    public void testUnion_withNestedIntersection_allDays_unevenStop() throws Exception {
        String query = "A == 'all_day' || (B == 'all_day' && C == 'uneven_stop_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            expectedQueryStrings
                            .add("(((_Delayed_ = true) && (A == 'all_day')) || (((_Delayed_ = true) && (B == 'all_day')) && ((_Delayed_ = true) && (C == 'uneven_stop_day'))))");
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || ( B && C )
    @Test
    public void testUnion_withNestedIntersection_allDays_missingShards() throws Exception {
        String query = "A == 'all_day' || (B == 'all_day' && C == 'missing_shards_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            if (ii == 3) {
                expectedQueryStrings.add("((_Delayed_ = true) && (A == 'all_day'))");
            } else {
                expectedQueryStrings
                                .add("(((_Delayed_ = true) && (A == 'all_day')) || (((_Delayed_ = true) && (B == 'all_day')) && ((_Delayed_ = true) && (C == 'missing_shards_day'))))");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || ( B && C )
    @Test
    public void testUnion_withNestedIntersection_allDays_tickTockShards() throws Exception {
        String query = "A == 'all_day' || (B == 'all_day' && C == 'tick_tock_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            if (ii == 3) {
                expectedQueryStrings.add("(((_Delayed_ = true) && (A == 'all_day')) || (((_Delayed_ = true) && (B == 'all_day')) && C == 'tick_tock_day'))");
            } else {
                expectedQueryStrings
                                .add("(((_Delayed_ = true) && (A == 'all_day')) || (((_Delayed_ = true) && (B == 'all_day')) && ((_Delayed_ = true) && (C == 'tick_tock_day'))))");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || ( B && C )
    @Test
    public void testUnion_dayWithNestedIntersectionOfShards() throws Exception {
        String query = "A == 'all_day' || (B == 'all' && C == 'all')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            expectedQueryStrings.add("((_Delayed_ = true) && (A == 'all_day')) || (B == 'all' && C == 'all')");
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || ( B && C )
    @Test
    public void testUnion_dayWithNestedIntersectionOfShards_unequalStart() throws Exception {
        String query = "A == 'all_day' || (B == 'all' && C == 'unequal_start')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            if (ii == 1) {
                expectedQueryStrings.add("((_Delayed_ = true) && (A == 'all_day'))");
            } else {
                expectedQueryStrings.add("((_Delayed_ = true) && (A == 'all_day')) || (B == 'all' && C == 'unequal_start')");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || ( B && C )
    @Test
    public void testUnion_dayWithNestedIntersectionOfShards_unequalStop() throws Exception {
        String query = "A == 'all_day' || (B == 'all' && C == 'unequal_stop')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            if (ii == 5) {
                expectedQueryStrings.add("((_Delayed_ = true) && (A == 'all_day'))");
            } else {
                expectedQueryStrings.add("((_Delayed_ = true) && (A == 'all_day')) || (B == 'all' && C == 'unequal_stop')");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || ( B && C )
    @Test
    public void testUnion_dayWithNestedIntersectionOfShards_unevenStart() throws Exception {
        String query = "A == 'all_day' || (B == 'all' && C == 'uneven_start')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            if (ii == 0) {
                expectedQueryStrings.add("((_Delayed_ = true) && (A == 'all_day'))");
            } else {
                expectedQueryStrings.add("((_Delayed_ = true) && (A == 'all_day')) || (B == 'all' && C == 'uneven_start')");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || ( B && C )
    @Test
    public void testUnion_dayWithNestedIntersectionOfShards_unevenStop() throws Exception {
        String query = "A == 'all_day' || (B == 'all' && C == 'uneven_stop')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            if (ii == 0) {
                expectedQueryStrings.add("((_Delayed_ = true) && (A == 'all_day'))");
            } else {
                expectedQueryStrings.add("((_Delayed_ = true) && (A == 'all_day')) || (B == 'all' && C == 'uneven_stop')");
            }
        }
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || ( B && C )
    @Test
    public void testUnion_dayWithNestedIntersectionOfShards_missingShards() throws Exception {
        String query = "A == 'all_day' || (B == 'all' && C == 'missing_shards')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            if (ii == 3) {
                expectedQueryStrings.add("((_Delayed_ = true) && (A == 'all_day'))");
            } else {
                expectedQueryStrings.add("((_Delayed_ = true) && (A == 'all_day')) || (B == 'all' && C == 'missing_shards')");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || ( B && C )
    @Test
    public void testUnion_dayWithNestedIntersectionOfShards_tickTockShards() throws Exception {
        String query = "A == 'all_day' || (B == 'all' && C == 'tick_tock')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            expectedQueryStrings.add("((_Delayed_ = true) && (A == 'all_day')) || (B == 'all' && C == 'tick_tock')");
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || ( B && C )
    @Test
    public void testUnion_shardWithNestedIntersectionOfDays() throws Exception {
        String query = "A == 'all' || (B == 'all_day' && C == 'all_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            expectedQueryStrings.add("A == 'all' || (((_Delayed_ = true) && (B == 'all_day')) && ((_Delayed_ = true) && (C == 'all_day')))");
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || ( B && C )
    @Test
    public void testUnion_shardWithNestedIntersectionOfDays_unequalStart() throws Exception {
        String query = "A == 'all' || (B == 'all_day' && C == 'unequal_start_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            if (ii != 1) {
                expectedRanges.add(makeDayRange("2020010" + ii));
                expectedQueryStrings.add("A == 'all' || (((_Delayed_ = true) && (B == 'all_day')) && ((_Delayed_ = true) && (C == 'unequal_start_day')))");
            } else {
                for (int jj = 0; jj < 10; jj++) {
                    expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                    expectedQueryStrings.add("A == 'all'");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || ( B && C )
    @Test
    public void testUnion_shardWithNestedIntersectionOfDays_unequalStop() throws Exception {
        String query = "A == 'all' || (B == 'all_day' && C == 'unequal_stop_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            if (ii != 5) {
                expectedRanges.add(makeDayRange("2020010" + ii));
                expectedQueryStrings.add("A == 'all' || (((_Delayed_ = true) && (B == 'all_day')) && ((_Delayed_ = true) && (C == 'unequal_stop_day')))");
            } else {
                for (int jj = 0; jj < 10; jj++) {
                    expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                    expectedQueryStrings.add("A == 'all'");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || ( B && C )
    @Test
    public void testUnion_shardWithNestedIntersectionOfDays_unevenStart() throws Exception {
        String query = "A == 'all' || (B == 'all_day' && C == 'uneven_start_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            expectedQueryStrings.add("A == 'all' || (((_Delayed_ = true) && (B == 'all_day')) && ((_Delayed_ = true) && (C == 'uneven_start_day')))");
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || ( B && C )
    @Test
    public void testUnion_shardWithNestedIntersectionOfDays_unevenStop() throws Exception {
        String query = "A == 'all' || (B == 'all_day' && C == 'uneven_stop_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            expectedQueryStrings.add("A == 'all' || (((_Delayed_ = true) && (B == 'all_day')) && ((_Delayed_ = true) && (C == 'uneven_stop_day')))");
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || ( B && C )
    @Test
    public void testUnion_shardWithNestedIntersectionOfDays_missingShards() throws Exception {
        String query = "A == 'all' || (B == 'all_day' && C == 'missing_shards_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            if (ii == 3) {
                for (int jj = 0; jj < 10; jj++) {
                    expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                    expectedQueryStrings.add("A == 'all'");
                }
            } else {
                expectedRanges.add(makeDayRange("2020010" + ii));
                expectedQueryStrings.add("A == 'all' || (((_Delayed_ = true) && (B == 'all_day')) && ((_Delayed_ = true) && (C == 'missing_shards_day')))");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // A || ( B && C )
    @Test
    public void testUnion_shardWithNestedIntersectionOfDays_tickTockShards() throws Exception {
        String query = "A == 'all' || (B == 'all_day' && C == 'tick_tock_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            if (ii != 3) {
                expectedRanges.add(makeDayRange("2020010" + ii));
                expectedQueryStrings.add("A == 'all' || (((_Delayed_ = true) && (B == 'all_day')) && ((_Delayed_ = true) && (C == 'tick_tock_day')))");
            } else {
                for (int jj = 0; jj < 10; jj++) {
                    if (jj == 2 || jj == 6) {
                        expectedRanges.add(makeShardedRange("20200103_" + jj));
                        expectedQueryStrings.add("A == 'all' || (((_Delayed_ = true) && (B == 'all_day')) && (C == 'tick_tock_day'))");
                    } else {
                        expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                        expectedQueryStrings.add("A == 'all'");
                    }
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A && B) || (C && D)
    @Test
    public void testUnion_ofNestedIntersections_allShards() throws Exception {
        String query = "(A == 'all' && B == 'all') || (C == 'all' && D == 'all')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                expectedQueryStrings.add("(A == 'all' && B == 'all') || (C == 'all' && D == 'all')");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A && B) || (C && D)
    @Test
    public void testUnion_ofNestedIntersections_allShards_unequalStart() throws Exception {
        String query = "(A == 'all' && B == 'all') || (C == 'all' && D == 'unequal_start')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii == 1) {
                    expectedQueryStrings.add("(A == 'all' && B == 'all')");
                } else {
                    expectedQueryStrings.add("(A == 'all' && B == 'all') || (C == 'all' && D == 'unequal_start')");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A && B) || (C && D)
    @Test
    public void testUnion_ofNestedIntersections_allShards_unequalStop() throws Exception {
        String query = "(A == 'all' && B == 'all') || (C == 'all' && D == 'unequal_stop')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii == 5) {
                    expectedQueryStrings.add("(A == 'all' && B == 'all')");
                } else {
                    expectedQueryStrings.add("(A == 'all' && B == 'all') || (C == 'all' && D == 'unequal_stop')");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A && B) || (C && D)
    @Test
    public void testUnion_ofNestedIntersections_allShards_unevenStart() throws Exception {
        String query = "(A == 'all' && B == 'all') || (C == 'all' && D == 'uneven_start')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (jj == 0) {
                    expectedQueryStrings.add("(A == 'all' && B == 'all')");
                } else {
                    expectedQueryStrings.add("(A == 'all' && B == 'all') || (C == 'all' && D == 'uneven_start')");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A && B) || (C && D)
    @Test
    public void testUnion_ofNestedIntersections_allShards_unevenStop() throws Exception {
        String query = "(A == 'all' && B == 'all') || (C == 'all' && D == 'uneven_stop')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (jj == 9) {
                    expectedQueryStrings.add("(A == 'all' && B == 'all')");
                } else {
                    expectedQueryStrings.add("(A == 'all' && B == 'all') || (C == 'all' && D == 'uneven_stop')");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A && B) || (C && D)
    @Test
    public void testUnion_ofNestedIntersections_allShards_missingShards() throws Exception {
        String query = "(A == 'all' && B == 'all') || (C == 'all' && D == 'missing_shards')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii == 3) {
                    expectedQueryStrings.add("(A == 'all' && B == 'all')");
                } else {
                    expectedQueryStrings.add("(A == 'all' && B == 'all') || (C == 'all' && D == 'missing_shards')");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A && B) || (C && D)
    @Test
    public void testUnion_ofNestedIntersections_allShards_tickTockShards() throws Exception {
        String query = "(A == 'all' && B == 'all') || (C == 'all' && D == 'tick_tock')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii != 3 || jj == 3 || jj == 7) {
                    expectedQueryStrings.add("(A == 'all' && B == 'all') || (C == 'all' && D == 'tick_tock')");
                } else {
                    expectedQueryStrings.add("(A == 'all' && B == 'all')");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A && B) || (C && D)
    @Test
    public void testUnion_ofNestedIntersections_allDays() throws Exception {
        String query = "(A == 'all_day' && B == 'all_day') || (C == 'all_day' && D == 'all_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            expectedQueryStrings
                            .add("((_Delayed_ = true) && (A == 'all_day') && (_Delayed_ = true) && (B == 'all_day')) || ((_Delayed_ = true) && (C == 'all_day') && (_Delayed_ = true) && (D == 'all_day'))");
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A && B) || (C && D)
    @Test
    public void testUnion_ofNestedIntersections_allDays_unequalStart() throws Exception {
        String query = "(A == 'all_day' && B == 'all_day') || (C == 'all_day' && D == 'unequal_start')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            if (ii == 1) {
                expectedQueryStrings.add("((_Delayed_ = true) && (A == 'all_day') && (_Delayed_ = true) && (B == 'all_day'))");
            } else {
                expectedQueryStrings
                                .add("((_Delayed_ = true) && (A == 'all_day') && (_Delayed_ = true) && (B == 'all_day')) || ((_Delayed_ = true) && (C == 'all_day') && D == 'unequal_start')");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A && B) || (C && D)
    @Test
    public void testUnion_ofNestedIntersections_allDays_unequalStop() throws Exception {
        String query = "(A == 'all_day' && B == 'all_day') || (C == 'all_day' && D == 'unequal_stop')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            if (ii == 5) {
                expectedQueryStrings.add("((_Delayed_ = true) && (A == 'all_day') && (_Delayed_ = true) && (B == 'all_day'))");
            } else {
                expectedQueryStrings
                                .add("((_Delayed_ = true) && (A == 'all_day') && (_Delayed_ = true) && (B == 'all_day')) || ((_Delayed_ = true) && (C == 'all_day') && D == 'unequal_stop')");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A && B) || (C && D)
    @Test
    public void testUnion_ofNestedIntersections_allDays_unevenStart() throws Exception {
        String query = "(A == 'all_day' && B == 'all_day') || (C == 'all_day' && D == 'uneven_start')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            expectedQueryStrings
                            .add("((_Delayed_ = true) && (A == 'all_day') && (_Delayed_ = true) && (B == 'all_day')) || ((_Delayed_ = true) && (C == 'all_day') && D == 'uneven_start')");
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A && B) || (C && D)
    @Test
    public void testUnion_ofNestedIntersections_allDays_unevenStop() throws Exception {
        String query = "(A == 'all_day' && B == 'all_day') || (C == 'all_day' && D == 'uneven_stop')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            expectedQueryStrings
                            .add("((_Delayed_ = true) && (A == 'all_day') && (_Delayed_ = true) && (B == 'all_day')) || ((_Delayed_ = true) && (C == 'all_day') && D == 'uneven_stop')");
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A && B) || (C && D)
    @Test
    public void testUnion_ofNestedIntersections_allDays_missingShards() throws Exception {
        String query = "(A == 'all_day' && B == 'all_day') || (C == 'all_day' && D == 'missing_shards_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            if (ii == 3) {
                expectedQueryStrings.add("((_Delayed_ = true) && (A == 'all_day') && (_Delayed_ = true) && (B == 'all_day'))");
            } else {
                expectedQueryStrings
                                .add("((_Delayed_ = true) && (A == 'all_day') && (_Delayed_ = true) && (B == 'all_day')) || ((_Delayed_ = true) && (C == 'all_day') && ((_Delayed_ = true) && (D == 'missing_shards_day')))");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A && B) || (C && D)
    @Test
    public void testUnion_ofNestedIntersections_allDays_tickTockShards() throws Exception {
        String query = "(A == 'all_day' && B == 'all_day') || (C == 'all_day' && D == 'tick_tock')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            expectedQueryStrings
                            .add("((_Delayed_ = true) && (A == 'all_day') && (_Delayed_ = true) && (B == 'all_day')) || ((_Delayed_ = true) && (C == 'all_day') && D == 'tick_tock')");
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A && B) || (C && D)
    @Test
    public void testUnion_ofNestedIntersections_distributedDays() throws Exception {
        String query = "(A == 'all' && B == 'all_day') || (C == 'all' && D == 'all_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                expectedQueryStrings
                                .add("(A == 'all' && ((_Delayed_ = true) && (B == 'all_day'))) || (C == 'all' && ((_Delayed_ = true) && (D == 'all_day')))");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A && B) || (C && D)
    @Test
    public void testUnion_ofNestedIntersections_distributedDays_unequalStart() throws Exception {
        String query = "(A == 'all' && B == 'all_day') || (C == 'all' && D == 'unequal_start_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii == 1) {
                    expectedQueryStrings.add("(A == 'all' && ((_Delayed_ = true) && (B == 'all_day')))");
                } else {
                    expectedQueryStrings
                                    .add("(A == 'all' && ((_Delayed_ = true) && (B == 'all_day'))) || (C == 'all' && ((_Delayed_ = true) && (D == 'unequal_start_day')))");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A && B) || (C && D)
    @Test
    public void testUnion_ofNestedIntersections_distributedDays_unequalStop() throws Exception {
        String query = "(A == 'all' && B == 'all_day') || (C == 'all' && D == 'unequal_stop_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii == 5) {
                    expectedQueryStrings.add("(A == 'all' && ((_Delayed_ = true) && (B == 'all_day')))");
                } else {
                    expectedQueryStrings
                                    .add("(A == 'all' && ((_Delayed_ = true) && (B == 'all_day'))) || (C == 'all' && ((_Delayed_ = true) && (D == 'unequal_stop_day')))");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A && B) || (C && D)
    @Test
    public void testUnion_ofNestedIntersections_distributedDays_unevenStart() throws Exception {
        String query = "(A == 'all' && B == 'all_day') || (C == 'all' && D == 'uneven_start_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii == 0) {
                    expectedQueryStrings.add("(A == 'all' && ((_Delayed_ = true) && (B == 'all_day')))");
                } else {
                    expectedQueryStrings
                                    .add("(A == 'all' && ((_Delayed_ = true) && (B == 'all_day'))) || (C == 'all' && ((_Delayed_ = true) && (D == 'uneven_start_day')))");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A && B) || (C && D)
    @Test
    public void testUnion_ofNestedIntersections_distributedDays_unevenStop() throws Exception {
        String query = "(A == 'all' && B == 'all_day') || (C == 'all' && D == 'uneven_stop_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii == 0) {
                    expectedQueryStrings.add("(A == 'all' && ((_Delayed_ = true) && (B == 'all_day')))");
                } else {
                    expectedQueryStrings
                                    .add("(A == 'all' && ((_Delayed_ = true) && (B == 'all_day'))) || (C == 'all' && ((_Delayed_ = true) && (D == 'uneven_stop_day')))");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A && B) || (C && D)
    @Test
    public void testUnion_ofNestedIntersections_distributedDays_missingShards() throws Exception {
        String query = "(A == 'all' && B == 'all_day') || (C == 'all' && D == 'missing_shards_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii == 3) {
                    expectedQueryStrings.add("(A == 'all' && ((_Delayed_ = true) && (B == 'all_day')))");
                } else {
                    expectedQueryStrings
                                    .add("(A == 'all' && ((_Delayed_ = true) && (B == 'all_day'))) || (C == 'all' && ((_Delayed_ = true) && (D == 'missing_shards_day')))");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A && B) || (C && D)
    @Test
    public void testUnion_ofNestedIntersections_distributedDays_tickTockShards() throws Exception {
        String query = "(A == 'all' && B == 'all_day') || (C == 'all' && D == 'tick_tock')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii != 3 || jj == 3 || jj == 7) {
                    expectedQueryStrings.add("(A == 'all' && ((_Delayed_ = true) && (B == 'all_day'))) || (C == 'all' && D == 'tick_tock')");
                } else {
                    expectedQueryStrings.add("(A == 'all' && ((_Delayed_ = true) && (B == 'all_day')))");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A || B) && (C || D)
    @Test
    public void testIntersection_ofNestedUnions_allShards() throws Exception {
        String query = "(A == 'all' || B == 'all') && (C == 'all' || D == 'all')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                expectedQueryStrings.add("(A == 'all' || B == 'all') && (C == 'all' || D == 'all')");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A || B) && (C || D)
    @Test
    public void testIntersection_ofNestedUnions_allShards_unequalStart() throws Exception {
        String query = "(A == 'all' || B == 'all') && (C == 'all' || D == 'unequal_start')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii == 1) {
                    expectedQueryStrings.add("(A == 'all' || B == 'all') && C == 'all'");
                } else {
                    expectedQueryStrings.add("(A == 'all' || B == 'all') && (C == 'all' || D == 'unequal_start')");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A || B) && (C || D)
    @Test
    public void testIntersection_ofNestedUnions_allShards_unequalStop() throws Exception {
        String query = "(A == 'all' || B == 'all') && (C == 'all' || D == 'unequal_stop')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii == 5) {
                    expectedQueryStrings.add("(A == 'all' || B == 'all') && C == 'all'");
                } else {
                    expectedQueryStrings.add("(A == 'all' || B == 'all') && (C == 'all' || D == 'unequal_stop')");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A || B) && (C || D)
    @Test
    public void testIntersection_ofNestedUnions_allShards_unevenStart() throws Exception {
        String query = "(A == 'all' || B == 'all') && (C == 'all' || D == 'uneven_start')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (jj == 0) {
                    expectedQueryStrings.add("(A == 'all' || B == 'all') && C == 'all'");
                } else {
                    expectedQueryStrings.add("(A == 'all' || B == 'all') && (C == 'all' || D == 'uneven_start')");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A || B) && (C || D)
    @Test
    public void testIntersection_ofNestedUnions_allShards_unevenStop() throws Exception {
        String query = "(A == 'all' || B == 'all') && (C == 'all' || D == 'uneven_stop')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (jj == 9) {
                    expectedQueryStrings.add("(A == 'all' || B == 'all') && C == 'all'");
                } else {
                    expectedQueryStrings.add("(A == 'all' || B == 'all') && (C == 'all' || D == 'uneven_stop')");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A || B) && (C || D)
    @Test
    public void testIntersection_ofNestedUnions_allShards_missingShards() throws Exception {
        String query = "(A == 'all' || B == 'all') && (C == 'all' || D == 'missing_shards')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii == 3) {
                    expectedQueryStrings.add("(A == 'all' || B == 'all') && C == 'all'");
                } else {
                    expectedQueryStrings.add("(A == 'all' || B == 'all') && (C == 'all' || D == 'missing_shards')");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A || B) && (C || D)
    @Test
    public void testIntersection_ofNestedUnions_allShards_tickTockShards() throws Exception {
        String query = "(A == 'all' || B == 'all') && (C == 'all' || D == 'tick_tock')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii != 3 || jj == 3 || jj == 7) {
                    expectedQueryStrings.add("(A == 'all' || B == 'all') && (C == 'all' || D == 'tick_tock')");
                } else {
                    expectedQueryStrings.add("(A == 'all' || B == 'all') && C == 'all'");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A || B) && (C || D)
    @Test
    public void testIntersection_ofNestedUnions_allDays() throws Exception {
        String query = "(A == 'all_day' || B == 'all_day') && (C == 'all_day' || D == 'all_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            expectedQueryStrings
                            .add("(((_Delayed_ = true) && (A == 'all_day')) || ((_Delayed_ = true) && (B == 'all_day'))) && (((_Delayed_ = true) && (C == 'all_day')) || ((_Delayed_ = true) && (D == 'all_day')))");
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A || B) && (C || D)
    @Test
    public void testIntersection_ofNestedUnions_allDays_unequalStart() throws Exception {
        String query = "(A == 'all_day' || B == 'all_day') && (C == 'all_day' || D == 'unequal_start_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            if (ii == 1) {
                expectedQueryStrings
                                .add("(((_Delayed_ = true) && (A == 'all_day')) || ((_Delayed_ = true) && (B == 'all_day'))) && (((_Delayed_ = true) && (C == 'all_day')))");
            } else {
                expectedQueryStrings
                                .add("(((_Delayed_ = true) && (A == 'all_day')) || ((_Delayed_ = true) && (B == 'all_day'))) && (((_Delayed_ = true) && (C == 'all_day')) || ((_Delayed_ = true) && (D == 'unequal_start_day')))");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A || B) && (C || D)
    @Test
    public void testIntersection_ofNestedUnions_allDays_unequalStop() throws Exception {
        String query = "(A == 'all_day' || B == 'all_day') && (C == 'all_day' || D == 'unequal_stop_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            if (ii == 5) {
                expectedQueryStrings
                                .add("(((_Delayed_ = true) && (A == 'all_day')) || ((_Delayed_ = true) && (B == 'all_day'))) && (((_Delayed_ = true) && (C == 'all_day')))");
            } else {
                expectedQueryStrings
                                .add("(((_Delayed_ = true) && (A == 'all_day')) || ((_Delayed_ = true) && (B == 'all_day'))) && (((_Delayed_ = true) && (C == 'all_day')) || ((_Delayed_ = true) && (D == 'unequal_stop_day')))");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A || B) && (C || D)
    @Test
    public void testIntersection_ofNestedUnions_allDays_unevenStart() throws Exception {
        String query = "(A == 'all_day' || B == 'all_day') && (C == 'all_day' || D == 'uneven_start_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            expectedQueryStrings
                            .add("(((_Delayed_ = true) && (A == 'all_day')) || ((_Delayed_ = true) && (B == 'all_day'))) && (((_Delayed_ = true) && (C == 'all_day')) || ((_Delayed_ = true) && (D == 'uneven_start_day')))");
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A || B) && (C || D)
    @Test
    public void testIntersection_ofNestedUnions_allDays_unevenStop() throws Exception {
        String query = "(A == 'all_day' || B == 'all_day') && (C == 'all_day' || D == 'uneven_stop_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            // if(ii == 5){
            // expectedQueryStrings.add("(((_Delayed_ = true) && (A == 'all_day')) || ((_Delayed_ = true) && (B == 'all_day'))) && (((_Delayed_ = true) && (C == 'all_day')))");
            // } else {
            expectedQueryStrings
                            .add("(((_Delayed_ = true) && (A == 'all_day')) || ((_Delayed_ = true) && (B == 'all_day'))) && (((_Delayed_ = true) && (C == 'all_day')) || ((_Delayed_ = true) && (D == 'uneven_stop_day')))");
            // }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A || B) && (C || D)
    @Test
    public void testIntersection_ofNestedUnions_allDays_missingShards() throws Exception {
        String query = "(A == 'all_day' || B == 'all_day') && (C == 'all_day' || D == 'missing_shards_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            if (ii == 3) {
                expectedQueryStrings
                                .add("(((_Delayed_ = true) && (A == 'all_day')) || ((_Delayed_ = true) && (B == 'all_day'))) && (((_Delayed_ = true) && (C == 'all_day')))");
            } else {
                expectedQueryStrings
                                .add("(((_Delayed_ = true) && (A == 'all_day')) || ((_Delayed_ = true) && (B == 'all_day'))) && (((_Delayed_ = true) && (C == 'all_day')) || ((_Delayed_ = true) && (D == 'missing_shards_day')))");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A || B) && (C || D)
    @Test
    public void testIntersection_ofNestedUnions_allDays_tickTockShards() throws Exception {
        String query = "(A == 'all_day' || B == 'all_day') && (C == 'all_day' || D == 'tick_tock')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            expectedQueryStrings
                            .add("(((_Delayed_ = true) && (A == 'all_day')) || ((_Delayed_ = true) && (B == 'all_day'))) && (((_Delayed_ = true) && (C == 'all_day')) || D == 'tick_tock')");
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A || B) && (C || D)
    @Test
    public void testIntersection_ofNestedUnions_distributedDays() throws Exception {
        String query = "(A == 'all' || B == 'all_day') && (C == 'all' || D == 'all_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            expectedQueryStrings.add("((A == 'all') || ((_Delayed_ = true) && (B == 'all_day'))) && ((C == 'all') || (_Delayed_ = true) && (D == 'all_day'))");
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A || B) && (C || D)
    @Test
    public void testIntersection_ofNestedUnions_distributedDays_unequalStart() throws Exception {
        String query = "(A == 'all' || B == 'all_day') && (C == 'all' || D == 'unequal_start_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            if (ii == 1) {
                for (int jj = 0; jj < 10; jj++) {
                    expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                    expectedQueryStrings.add("((A == 'all') || ((_Delayed_ = true) && (B == 'all_day'))) && ((C == 'all'))");
                }
            } else {
                expectedRanges.add(makeDayRange("2020010" + ii));
                expectedQueryStrings
                                .add("((A == 'all') || ((_Delayed_ = true) && (B == 'all_day'))) && ((C == 'all') || (_Delayed_ = true) && (D == 'unequal_start_day'))");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A || B) && (C || D)
    @Test
    public void testIntersection_ofNestedUnions_distributedDays_unequalStop() throws Exception {
        String query = "(A == 'all' || B == 'all_day') && (C == 'all' || D == 'unequal_stop_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            if (ii == 5) {
                for (int jj = 0; jj < 10; jj++) {
                    expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                    expectedQueryStrings.add("((A == 'all') || ((_Delayed_ = true) && (B == 'all_day'))) && ((C == 'all'))");
                }
            } else {
                expectedRanges.add(makeDayRange("2020010" + ii));
                expectedQueryStrings
                                .add("((A == 'all') || ((_Delayed_ = true) && (B == 'all_day'))) && ((C == 'all') || (_Delayed_ = true) && (D == 'unequal_stop_day'))");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A || B) && (C || D)
    @Test
    public void testIntersection_ofNestedUnions_distributedDays_unevenStart() throws Exception {
        String query = "(A == 'all' || B == 'all_day') && (C == 'all' || D == 'uneven_start_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            expectedQueryStrings
                            .add("((A == 'all') || ((_Delayed_ = true) && (B == 'all_day'))) && ((C == 'all') || (_Delayed_ = true) && (D == 'uneven_start_day'))");
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A || B) && (C || D)
    @Test
    public void testIntersection_ofNestedUnions_distributedDays_unevenStop() throws Exception {
        String query = "(A == 'all' || B == 'all_day') && (C == 'all' || D == 'uneven_stop_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            expectedRanges.add(makeDayRange("2020010" + ii));
            expectedQueryStrings
                            .add("((A == 'all') || ((_Delayed_ = true) && (B == 'all_day'))) && ((C == 'all') || (_Delayed_ = true) && (D == 'uneven_stop_day'))");
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A || B) && (C || D)
    @Test
    public void testIntersection_ofNestedUnions_distributedDays_missingShards() throws Exception {
        String query = "(A == 'all' || B == 'all_day') && (C == 'all' || D == 'missing_shard_day')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                expectedQueryStrings.add("((A == 'all') || ((_Delayed_ = true) && (B == 'all_day'))) && ((C == 'all'))");
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    // (A || B) && (C || D)
    @Test
    public void testIntersection_ofNestedUnions_distributedDays_tickTockShards() throws Exception {
        String query = "(A == 'all' || B == 'all_day') && (C == 'all' || D == 'tick_tock')";
        
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii != 3 || jj == 3 || jj == 7) {
                    expectedQueryStrings.add("(A == 'all' || ((_Delayed_ = true) && (B == 'all_day'))) && (C == 'all' || D == 'tick_tock')");
                } else {
                    expectedQueryStrings.add("(A == 'all' || ((_Delayed_ = true) && (B == 'all_day'))) && C == 'all'");
                }
            }
        }
        
        runTest(query, expectedRanges, expectedQueryStrings);
    }
    
    private void runTest(String query, List<Range> expectedRanges, List<String> expectedQueries) throws Exception {
        
        assertEquals("Expected ranges and queries do not match, ranges: " + expectedRanges.size() + " queries: " + expectedQueries.size(),
                        expectedRanges.size(), expectedQueries.size());
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        // config.setBeginDate(new Date(0));
        config.setBeginDate(sdf.parse("20200101"));
        config.setEndDate(sdf.parse("20200105"));
        
        config.setDatatypeFilter(Sets.newHashSet("datatype1"));
        
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("A", Sets.newHashSet(new LcNoDiacriticsType()));
        dataTypes.putAll("B", Sets.newHashSet(new LcNoDiacriticsType()));
        dataTypes.putAll("C", Sets.newHashSet(new LcNoDiacriticsType()));
        dataTypes.putAll("D", Sets.newHashSet(new LcNoDiacriticsType()));
        
        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);
        config.setShardsPerDayThreshold(2);
        
        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());
        
        // Run a standard limited-scanner range stream.
        RangeStream rangeStream = new RangeStream(config, new ScannerFactory(config.getConnector(), 1), helper);
        rangeStream.setLimitScanners(true);
        runTest(rangeStream, script, expectedRanges, expectedQueries);
        
        // Run a default range stream.
        rangeStream = new RangeStream(config, new ScannerFactory(config.getConnector(), 1), helper);
        rangeStream.setLimitScanners(false);
        runTest(rangeStream, script, expectedRanges, expectedQueries);
    }
    
    private void runTest(RangeStream rangeStream, ASTJexlScript script, List<Range> expectedRanges, List<String> expectedQueries) throws Exception {
        CloseableIterable<QueryPlan> queryPlans = rangeStream.streamPlans(script);
        assertEquals(IndexStream.StreamContext.PRESENT, rangeStream.context());
        
        Iterator<Range> shardIter = expectedRanges.iterator();
        Iterator<String> queryIter = expectedQueries.iterator();
        
        // Should have one range per query plan
        int counter = 0;
        for (QueryPlan queryPlan : queryPlans) {
            
            // Assert proper range
            Iterator<Range> rangeIter = queryPlan.getRanges().iterator();
            Range planRange = rangeIter.next();
            Range expectedRange = shardIter.next();
            
            assertEquals("Query produced unexpected range: " + planRange.toString(), expectedRange, planRange);
            assertFalse("Query plan had more than one range!", rangeIter.hasNext());
            
            // Assert proper query string for this range.
            
            ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(queryIter.next());
            ASTJexlScript planScript = JexlNodeFactory.createScript(queryPlan.getQueryTree());
            
            String expectedString = JexlStringBuildingVisitor.buildQuery(expectedScript);
            String plannedString = JexlStringBuildingVisitor.buildQuery(planScript);
            
            // Re-parse to avoid weird cases of DelayedPredicates
            expectedScript = JexlASTHelper.parseJexlQuery(expectedString);
            planScript = JexlASTHelper.parseJexlQuery(plannedString);
            
            assertTrue("Queries did not match for counter: " + counter + " on shard: " + planRange.toString() + "\nExpected: " + expectedString
                            + "\nActual  : " + plannedString, TreeEqualityVisitor.isEqual(expectedScript, planScript));
            counter++;
        }
        
        // Ensure we didn't miss any expected ranges or queries
        if (shardIter.hasNext())
            fail("Expected ranges still exist after test: " + shardIter.next());
        if (queryIter.hasNext())
            fail("Expected queries still exist after test: " + queryIter.next());
    }
}
