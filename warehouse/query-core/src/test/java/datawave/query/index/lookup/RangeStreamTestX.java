package datawave.query.index.lookup;

import static datawave.common.test.utils.query.RangeFactoryForTests.makeShardedRange;
import static datawave.common.test.utils.query.RangeFactoryForTests.makeTestRange;
import static datawave.util.TableName.SHARD_INDEX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
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

/**
 * Cover some basic tests involving streams of shards for a basic set of query structures. Only tests for correctness of shard intersection, not that the
 * underlying IndexInfo objects correctly intersect.
 * <p>
 * 6 Basic Types of Query Structures
 * <p>
 * 3 Stream Type Combinations (All shards, shards and days, all days)
 * <p>
 * 2 Types of Unequal Stream Start/Stop (different start/end day)
 * <p>
 * 2 Types of Uneven Stream Start/Stop (same start/end day, different shard)
 * <p>
 * 1 Type of Tick-Tock Shards (alternating shards such that no hits are produced for a day)
 * <p>
 * 1 Type of Missing Shards (missing shards should drop terms from query)
 */
public class RangeStreamTestX {

    // A && B
    // A || B
    // A && ( B || C )
    // A || ( B && C )
    // (A && B) || (C && D)
    // (A || B) && (C || D)

    private static AccumuloClient client;
    private ShardQueryConfiguration config;

    @BeforeClass
    public static void setupAccumulo() throws Exception {
        client = new InMemoryAccumuloClient("", new InMemoryInstance());
        client.tableOperations().create(SHARD_INDEX);

        BatchWriter bw = client.createBatchWriter(SHARD_INDEX,
                        new BatchWriterConfig().setMaxLatency(10, TimeUnit.SECONDS).setMaxMemory(100000L).setMaxWriteThreads(1));

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

        // --------------- Data for nested pruning

        m = new Mutation("1"); // fully distributed
        m.put(new Text("F1"), new Text("20200101_10\0datatype1"), valueForShard);
        m.put(new Text("F1"), new Text("20200101_11\0datatype1"), valueForShard);
        m.put(new Text("F1"), new Text("20200101_12\0datatype1"), valueForShard);
        m.put(new Text("F1"), new Text("20200101_13\0datatype1"), valueForShard);
        bw.addMutation(m);

        m = new Mutation("2"); // skips shard _11
        m.put(new Text("F2"), new Text("20200101_10\0datatype1"), valueForShard);
        m.put(new Text("F2"), new Text("20200101_12\0datatype1"), valueForShard);
        m.put(new Text("F2"), new Text("20200101_13\0datatype1"), valueForShard);
        bw.addMutation(m);

        m = new Mutation("3"); // skips shard _12
        m.put(new Text("F3"), new Text("20200101_10\0datatype1"), valueForShard);
        m.put(new Text("F3"), new Text("20200101_11\0datatype1"), valueForShard);
        m.put(new Text("F3"), new Text("20200101_13\0datatype1"), valueForShard);
        bw.addMutation(m);

        m = new Mutation("4"); // fully distributed
        m.put(new Text("F4"), new Text("20200101_10\0datatype1"), valueForShard);
        m.put(new Text("F4"), new Text("20200101_11\0datatype1"), valueForShard);
        m.put(new Text("F4"), new Text("20200101_12\0datatype1"), valueForShard);
        m.put(new Text("F4"), new Text("20200101_13\0datatype1"), valueForShard);
        bw.addMutation(m);

        // --------------- some entries for post-index sorting via field or term counts

        m = new Mutation("23");
        m.put(new Text("FIELD_A"), new Text("20200101_10\0sort-type"), createValue(23L));
        m.put(new Text("FIELD_B"), new Text("20200101_10\0sort-type"), createValue(23L));
        m.put(new Text("FIELD_C"), new Text("20200101_10\0sort-type"), createValue(23L));
        bw.addMutation(m);

        m = new Mutation("34");
        m.put(new Text("FIELD_A"), new Text("20200101_10\0sort-type"), createValue(34L));
        m.put(new Text("FIELD_B"), new Text("20200101_10\0sort-type"), createValue(34L));
        m.put(new Text("FIELD_C"), new Text("20200101_10\0sort-type"), createValue(34L));
        bw.addMutation(m);

        m = new Mutation("45");
        m.put(new Text("FIELD_A"), new Text("20200101_10\0sort-type"), createValue(45L));
        m.put(new Text("FIELD_B"), new Text("20200101_10\0sort-type"), createValue(45L));
        m.put(new Text("FIELD_C"), new Text("20200101_10\0sort-type"), createValue(45L));
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

    /**
     * Create a value with a count
     *
     * @param count
     *            the count
     * @return a value
     */
    private static Value createValue(long count) {
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.setIGNORE(true);
        builder.setCOUNT(count);
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
        config.setClient(client);

        // disable all post-index sort options by default
        config.setSortQueryPostIndexWithFieldCounts(false);
        config.setSortQueryPostIndexWithTermCounts(false);
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
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                expectedQueryStrings.add("(B == 'all' && A == 'all_day')");
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
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                expectedQueryStrings.add("(A == 'unequal_start' && B == 'all_day')");
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
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                expectedQueryStrings.add("(A == 'unequal_stop' && B == 'all_day')");
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
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                expectedQueryStrings.add("(A == 'uneven_start' && B == 'all_day')");
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
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                expectedQueryStrings.add("(A == 'uneven_stop' && B == 'all_day')");
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
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                expectedQueryStrings.add("A == 'all_day' && B == 'missing_shards'");
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
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings.add("(A == 'tick_tock' && B == 'tick_tock_day')");
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                expectedQueryStrings.add("A == 'all_day' && B == 'all_day'");
            }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                expectedQueryStrings.add("A == 'all_day' && B == 'unequal_start_day'");
            }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                expectedQueryStrings.add("A == 'all_day' && B == 'unequal_stop_day'");
            }
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
            for (int jj = 1; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                expectedQueryStrings.add("A == 'all_day' && B == 'uneven_start_day'");
            }
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
            for (int jj = 0; jj < 9; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                expectedQueryStrings.add("A == 'all_day' && B == 'uneven_stop_day'");
            }
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
            if (ii == 3) {
                continue;
            }

            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                expectedQueryStrings.add("A == 'all_day' && B == 'missing_shards_day'");
            }
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
                for (int jj = 0; jj < 10; jj++) {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings.add("(A == 'tick_tock_day' && B == 'tick_tock_day')");
                }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                expectedQueryStrings.add("(A == 'all' || B == 'all_day')");
            }
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
            for (int jj = 0; jj < 10; jj++) {
                if (ii == 1) {
                    expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                    expectedQueryStrings.add("A == 'all'");
                } else {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings.add("A == 'all' || B == 'unequal_start_day'");
                }
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
            for (int jj = 0; jj < 10; jj++) {
                if (ii == 5) {
                    expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                    expectedQueryStrings.add("A == 'all'");
                } else {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings.add("A == 'all' || B == 'unequal_stop_day'");
                }
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
            for (int jj = 0; jj < 10; jj++) {
                if (jj == 0) {
                    expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\u0000a.b.c"));
                    expectedQueryStrings.add("A == 'all'");
                } else {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings.add("A == 'all' || B == 'uneven_start_day'");
                }
            }
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
            for (int jj = 0; jj < 10; jj++) {
                if (jj == 9) {
                    expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\u0000a.b.c"));
                    expectedQueryStrings.add("A == 'all'");
                } else {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings.add("A == 'all' || B == 'uneven_stop_day'");
                }
            }
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
            for (int jj = 0; jj < 10; jj++) {
                if (ii == 3) {
                    expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                    expectedQueryStrings.add("A == 'all'");
                } else {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings.add("A == 'all' || B == 'missing_shards_day'");
                }
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
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                if (ii == 3 && jj % 4 != 1) {
                    expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\u0000a.b.c"));
                    expectedQueryStrings.add("A == 'all'");
                } else {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings.add("A == 'all' || B == 'tick_tock_day'");
                }
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings);
    }

    // A || B
    @Test
    public void testUnion_ofDays() throws Exception {
        String query = "A == 'all_day' || B == 'all_day'";

        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                expectedQueryStrings.add("A == 'all_day' || B == 'all_day'");
            }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 1) {
                    expectedQueryStrings.add("A == 'all_day'");
                } else {
                    expectedQueryStrings.add("A == 'all_day' || B == 'unequal_start_day'");
                }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 5) {
                    expectedQueryStrings.add("A == 'all_day'");
                } else {
                    expectedQueryStrings.add("A == 'all_day' || B == 'unequal_stop_day'");
                }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (jj == 0) {
                    expectedQueryStrings.add("A == 'all_day'");
                } else {
                    expectedQueryStrings.add("A == 'all_day' || B == 'uneven_start_day'");
                }
            }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (jj == 9) {
                    expectedQueryStrings.add("A == 'all_day'");
                } else {
                    expectedQueryStrings.add("A == 'all_day' || B == 'uneven_stop_day'");
                }
            }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 3) {
                    expectedQueryStrings.add("A == 'all_day'");
                } else {
                    expectedQueryStrings.add("A == 'all_day' || B == 'missing_shards_day'");
                }
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
            for (int jj = 0; jj < 10; jj++) {
                if (ii != 3) {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings.add("(A == 'tick_tock_day' || B == 'tick_tock_day')");
                } else {
                    if (jj % 4 == 0) {
                        expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                        expectedQueryStrings.add("A == 'tick_tock_day'");
                    } else if (jj % 4 == 1) {
                        expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                        expectedQueryStrings.add("B == 'tick_tock_day'");
                    }
                }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                expectedQueryStrings.add("A == 'all_day' && (B == 'all_day' || C == 'all_day')");
            }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 1) {
                    expectedQueryStrings.add("A == 'all_day' && B == 'all_day'");
                } else {
                    expectedQueryStrings.add("A == 'all_day' && (B == 'all_day' || C == 'unequal_start_day')");
                }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 5) {
                    expectedQueryStrings.add("A == 'all_day' && B == 'all_day'");
                } else {
                    expectedQueryStrings.add("A == 'all_day' && (B == 'all_day' || C == 'unequal_stop_day')");
                }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (jj == 0) {
                    expectedQueryStrings.add("A == 'all_day' && B == 'all_day'");
                } else {
                    expectedQueryStrings.add("A == 'all_day' && (B == 'all_day' || C == 'uneven_start_day')");
                }
            }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (jj == 9) {
                    expectedQueryStrings.add("A == 'all_day' && B == 'all_day'");
                } else {
                    expectedQueryStrings.add("A == 'all_day' && (B == 'all_day' || C == 'uneven_stop_day')");
                }
            }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 3) {
                    expectedQueryStrings.add("(A == 'all_day' && B == 'all_day')");
                } else {
                    expectedQueryStrings.add("A == 'all_day' && (B == 'all_day' || C == 'missing_shards_day')");
                }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 3 && jj % 4 != 2) {
                    expectedQueryStrings.add("A == 'all_day' && B == 'all_day'");
                } else {
                    expectedQueryStrings.add("A == 'all_day' && (B == 'all_day' || C == 'tick_tock')");
                }
            }
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
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                expectedQueryStrings.add("(A == 'all_day') && (B == 'all' || C == 'all')");
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
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 1) {
                    expectedQueryStrings.add("A == 'all_day' && B == 'all'");
                } else {
                    expectedQueryStrings.add("A == 'all_day' && (B == 'all' || C == 'unequal_start')");
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
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 5) {
                    expectedQueryStrings.add("A == 'all_day' && B == 'all'");
                } else {
                    expectedQueryStrings.add("A == 'all_day' && (B == 'all' || C == 'unequal_stop')");
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
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (jj == 0) {
                    expectedQueryStrings.add("A == 'all_day' && B == 'all'");
                } else {
                    expectedQueryStrings.add("A == 'all_day' && (B == 'all' || C == 'uneven_start')");
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
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (jj == 9) {
                    expectedQueryStrings.add("A == 'all_day' && B == 'all'");
                } else {
                    expectedQueryStrings.add("A == 'all_day' && (B == 'all' || C == 'uneven_stop')");
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
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 3) {
                    expectedQueryStrings.add("A == 'all_day' && B == 'all'");
                } else {
                    expectedQueryStrings.add("A == 'all_day' && (B == 'all' || C == 'missing_shards')");
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
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii != 3 || jj == 2 || jj == 6) {
                    expectedQueryStrings.add("A == 'all_day' && (B == 'all' || C == 'tick_tock')");
                } else {
                    expectedQueryStrings.add("A == 'all_day' && B == 'all'");
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
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                expectedQueryStrings.add("A == 'all' && (B == 'all_day' || C == 'all_day')");
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
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 1) {
                    expectedQueryStrings.add("A == 'all' && B == 'all_day'");
                } else {
                    expectedQueryStrings.add("A == 'all' && (B == 'all_day' || C == 'unequal_start_day')");
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
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 5) {
                    expectedQueryStrings.add("A == 'all' && B == 'all_day'");
                } else {
                    expectedQueryStrings.add("A == 'all' && (B == 'all_day' || C == 'unequal_stop_day')");
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
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (jj == 0) {
                    expectedQueryStrings.add("A == 'all' && B == 'all_day'");
                } else {
                    expectedQueryStrings.add("A == 'all' && (B == 'all_day' || C == 'uneven_start_day')");
                }
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
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (jj == 9) {
                    expectedQueryStrings.add("A == 'all' && B == 'all_day'");
                } else {
                    expectedQueryStrings.add("A == 'all' && (B == 'all_day' || C == 'uneven_stop_day')");
                }
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
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 3) {
                    expectedQueryStrings.add("A == 'all' && B == 'all_day'");
                } else {
                    expectedQueryStrings.add("A == 'all' && (B == 'all_day' || C == 'missing_shards_day')");
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
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 3 && jj % 4 != 2) {
                    expectedQueryStrings.add("A == 'all' && B == 'all_day'");
                } else {
                    expectedQueryStrings.add("A == 'all' && (B == 'all_day' || C == 'tick_tock_day')");
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                expectedQueryStrings.add("A == 'all_day' || (B == 'all_day' && C == 'all_day')");
            }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 1) {
                    expectedQueryStrings.add("A == 'all_day'");
                } else {
                    expectedQueryStrings.add("A == 'all_day' || (B == 'all_day' && C == 'unequal_start_day')");
                }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 5) {
                    expectedQueryStrings.add("A == 'all_day'");
                } else {
                    expectedQueryStrings.add("A == 'all_day' || (B == 'all_day' && C == 'unequal_stop_day')");
                }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (jj == 0) {
                    expectedQueryStrings.add("A == 'all_day'");
                } else {
                    expectedQueryStrings.add("A == 'all_day' || (B == 'all_day' && C == 'uneven_start_day')");
                }
            }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (jj == 9) {
                    expectedQueryStrings.add("A == 'all_day'");
                } else {
                    expectedQueryStrings.add("A == 'all_day' || (B == 'all_day' && C == 'uneven_stop_day')");
                }
            }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 3) {
                    expectedQueryStrings.add("A == 'all_day'");
                } else {
                    expectedQueryStrings.add("A == 'all_day' || (B == 'all_day' && C == 'missing_shards_day')");
                }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 3 && jj % 4 != 2) {
                    expectedQueryStrings.add("A == 'all_day'");
                } else {
                    expectedQueryStrings.add("A == 'all_day' || (B == 'all_day' && C == 'tick_tock_day')");
                }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                expectedQueryStrings.add("A == 'all_day' || (B == 'all' && C == 'all')");
            }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 1) {
                    expectedQueryStrings.add("A == 'all_day'");
                } else {
                    expectedQueryStrings.add("A == 'all_day' || (B == 'all' && C == 'unequal_start')");
                }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 5) {
                    expectedQueryStrings.add("A == 'all_day'");
                } else {
                    expectedQueryStrings.add("A == 'all_day' || (B == 'all' && C == 'unequal_stop')");
                }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (jj == 0) {
                    expectedQueryStrings.add("A == 'all_day'");
                } else {
                    expectedQueryStrings.add("A == 'all_day' || (B == 'all' && C == 'uneven_start')");
                }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (jj == 9) {
                    expectedQueryStrings.add("A == 'all_day'");
                } else {
                    expectedQueryStrings.add("A == 'all_day' || (B == 'all' && C == 'uneven_stop')");
                }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 3) {
                    expectedQueryStrings.add("A == 'all_day'");
                } else {
                    expectedQueryStrings.add("A == 'all_day' || (B == 'all' && C == 'missing_shards')");
                }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 3 && jj % 4 != 2) {
                    expectedQueryStrings.add("A == 'all_day'");
                } else {
                    expectedQueryStrings.add("A == 'all_day' || (B == 'all' && C == 'tick_tock')");
                }
            }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                expectedQueryStrings.add("A == 'all' || (B == 'all_day' && C == 'all_day')");
            }
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
            for (int jj = 0; jj < 10; jj++) {
                if (ii != 1) {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings.add("A == 'all' || (B == 'all_day' && C == 'unequal_start_day')");
                } else {
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
            for (int jj = 0; jj < 10; jj++) {
                if (ii != 5) {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings.add("A == 'all' || (B == 'all_day' && C == 'unequal_stop_day')");
                } else {

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
            for (int jj = 0; jj < 10; jj++) {
                if (jj == 0) {
                    expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\u0000a.b.c"));
                    expectedQueryStrings.add("A == 'all'");
                } else {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings.add("A == 'all' || (B == 'all_day' && C == 'uneven_start_day')");
                }
            }
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
            for (int jj = 0; jj < 10; jj++) {
                if (jj == 9) {
                    expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\u0000a.b.c"));
                    expectedQueryStrings.add("A == 'all'");
                } else {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings.add("A == 'all' || (B == 'all_day' && C == 'uneven_stop_day')");
                }
            }
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
            for (int jj = 0; jj < 10; jj++) {
                if (ii == 3) {
                    expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                    expectedQueryStrings.add("A == 'all'");
                } else {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings.add("A == 'all' || (B == 'all_day' && C == 'missing_shards_day')");
                }
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
            for (int jj = 0; jj < 10; jj++) {
                if (ii != 3) {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings.add("A == 'all' || (B == 'all_day' && C == 'tick_tock_day')");
                } else {
                    if (jj == 2 || jj == 6) {
                        expectedRanges.add(makeShardedRange("20200103_" + jj));
                        expectedQueryStrings.add("A == 'all' || (B == 'all_day' && (C == 'tick_tock_day'))");
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                expectedQueryStrings.add("(A == 'all_day' && B == 'all_day') || (C == 'all_day' && D == 'all_day')");
            }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 1) {
                    expectedQueryStrings.add("A == 'all_day' && B == 'all_day'");
                } else {
                    expectedQueryStrings.add("(A == 'all_day' && B == 'all_day') || (C == 'all_day' && D == 'unequal_start')");
                }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 5) {
                    expectedQueryStrings.add("A == 'all_day' && B == 'all_day'");
                } else {
                    expectedQueryStrings.add("(A == 'all_day' && B == 'all_day') || (C == 'all_day' && D == 'unequal_stop')");
                }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (jj == 0) {
                    expectedQueryStrings.add("(A == 'all_day' && B == 'all_day')");
                } else {
                    expectedQueryStrings.add("(A == 'all_day' && B == 'all_day') || (C == 'all_day' && D == 'uneven_start')");
                }
            }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (jj == 9) {
                    expectedQueryStrings.add("(A == 'all_day' && B == 'all_day')");
                } else {
                    expectedQueryStrings.add("(A == 'all_day' && B == 'all_day') || (C == 'all_day' && D == 'uneven_stop')");
                }
            }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 3) {
                    expectedQueryStrings.add("A == 'all_day' && B == 'all_day'");
                } else {
                    expectedQueryStrings.add("(A == 'all_day' && B == 'all_day') || (C == 'all_day' && D == 'missing_shards_day')");
                }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 3 && jj % 4 != 3) {
                    expectedQueryStrings.add("(A == 'all_day' && B == 'all_day')");
                } else {
                    expectedQueryStrings.add("(A == 'all_day' && B == 'all_day') || (C == 'all_day' && D == 'tick_tock')");
                }
            }
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
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                expectedQueryStrings.add("(A == 'all' && B == 'all_day') || (C == 'all' && D == 'all_day')");
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
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 1) {
                    expectedQueryStrings.add("(A == 'all' && B == 'all_day')");
                } else {
                    expectedQueryStrings.add("(A == 'all' && B == 'all_day') || (C == 'all' && D == 'unequal_start_day')");
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
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 5) {
                    expectedQueryStrings.add("(A == 'all' && B == 'all_day')");
                } else {
                    expectedQueryStrings.add("(A == 'all' && B == 'all_day') || (C == 'all' && D == 'unequal_stop_day')");
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
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (jj == 0) {
                    expectedQueryStrings.add("(A == 'all' && B == 'all_day')");
                } else {
                    expectedQueryStrings.add("(A == 'all' && B == 'all_day') || (C == 'all' && D == 'uneven_start_day')");
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
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (jj == 9) {
                    expectedQueryStrings.add("(A == 'all' && B == 'all_day')");
                } else {
                    expectedQueryStrings.add("(A == 'all' && B == 'all_day') || (C == 'all' && D == 'uneven_stop_day')");
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
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 3) {
                    expectedQueryStrings.add("(A == 'all' && B == 'all_day')");
                } else {
                    expectedQueryStrings.add("(A == 'all' && B == 'all_day') || (C == 'all' && D == 'missing_shards_day')");
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
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii != 3 || jj == 3 || jj == 7) {
                    expectedQueryStrings.add("(A == 'all' && B == 'all_day') || (C == 'all' && D == 'tick_tock')");
                } else {
                    expectedQueryStrings.add("(A == 'all' && B == 'all_day')");
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                expectedQueryStrings.add("(A == 'all_day' || B == 'all_day') && (C == 'all_day' || D == 'all_day')");
            }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 1) {
                    expectedQueryStrings.add("(A == 'all_day' || B == 'all_day') && (C == 'all_day')");
                } else {
                    expectedQueryStrings.add("(A == 'all_day' || B == 'all_day') && (C == 'all_day' || D == 'unequal_start_day')");
                }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 5) {
                    expectedQueryStrings.add("(A == 'all_day' || B == 'all_day') && (C == 'all_day')");
                } else {
                    expectedQueryStrings.add("(A == 'all_day' || B == 'all_day') && (C == 'all_day' || D == 'unequal_stop_day')");
                }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (jj == 0) {
                    expectedQueryStrings.add("(A == 'all_day' || B == 'all_day') && C == 'all_day'");
                } else {
                    expectedQueryStrings.add("(A == 'all_day' || B == 'all_day') && (C == 'all_day' || D == 'uneven_start_day')");
                }
            }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (jj == 9) {
                    expectedQueryStrings.add("(A == 'all_day' || B == 'all_day') && C == 'all_day'");
                } else {
                    expectedQueryStrings.add("(A == 'all_day' || B == 'all_day') && (C == 'all_day' || D == 'uneven_stop_day')");
                }
            }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 3) {
                    expectedQueryStrings.add("(A == 'all_day' || B == 'all_day') && (C == 'all_day')");
                } else {
                    expectedQueryStrings.add("(A == 'all_day' || B == 'all_day') && (C == 'all_day' || D == 'missing_shards_day')");
                }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 3 && jj % 4 != 3) {
                    expectedQueryStrings.add("(A == 'all_day' || B == 'all_day') && C == 'all_day'");
                } else {
                    expectedQueryStrings.add("(A == 'all_day' || B == 'all_day') && (C == 'all_day' || D == 'tick_tock')");
                }
            }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                expectedQueryStrings.add("(A == 'all' || B == 'all_day') && (C == 'all' || D == 'all_day')");
            }
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
            for (int jj = 0; jj < 10; jj++) {
                if (ii == 1) {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings.add("((A == 'all') || B == 'all_day') && ((C == 'all'))");
                } else {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings.add("(A == 'all' || B == 'all_day') && (C == 'all' || D == 'unequal_start_day')");
                }
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
            for (int jj = 0; jj < 10; jj++) {
                if (ii == 5) {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings.add("(A == 'all' || B == 'all_day') && (C == 'all')");
                } else {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings.add("(A == 'all' || B == 'all_day') && (C == 'all' || D == 'unequal_stop_day')");
                }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (jj == 0) {
                    expectedQueryStrings.add("(A == 'all' || B == 'all_day') && C == 'all'");
                } else {
                    expectedQueryStrings.add("(A == 'all' || B == 'all_day') && (C == 'all' || D == 'uneven_start_day')");
                }
            }
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
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (jj == 9) {
                    expectedQueryStrings.add("(A == 'all' || B == 'all_day') && C == 'all'");
                } else {
                    expectedQueryStrings.add("(A == 'all' || B == 'all_day') && (C == 'all' || D == 'uneven_stop_day')");
                }
            }
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
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                expectedQueryStrings.add("((A == 'all') || B == 'all_day') && ((C == 'all'))");
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
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii != 3 || jj == 3 || jj == 7) {
                    expectedQueryStrings.add("(A == 'all' || B == 'all_day') && (C == 'all' || D == 'tick_tock')");
                } else {
                    expectedQueryStrings.add("(A == 'all' || B == 'all_day') && C == 'all'");
                }
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings);
    }

    @Test
    public void testNestedPruningWithTopLevelIntersection() throws Exception {
        String query = "F1 == '1' && (F3 == '3' || F4 == '4')";

        List<Range> expectedRanges = new ArrayList<>();
        expectedRanges.add(makeTestRange("20200101_10", "datatype1\0a.b.c"));
        expectedRanges.add(makeTestRange("20200101_11", "datatype1\0a.b.c"));
        expectedRanges.add(makeTestRange("20200101_12", "datatype1\0a.b.c"));
        expectedRanges.add(makeTestRange("20200101_13", "datatype1\0a.b.c"));

        List<String> expectedQueries = new ArrayList<>();
        expectedQueries.add("F1 == '1' && (F3 == '3' || F4 == '4')");
        expectedQueries.add("F1 == '1' && (F3 == '3' || F4 == '4')");
        expectedQueries.add("F1 == '1' && F4 == '4'"); // F3 skips shard _12
        expectedQueries.add("F1 == '1' && (F3 == '3' || F4 == '4')");

        runTest(query, expectedRanges, expectedQueries);
    }

    @Test
    public void testDelayedNestedPruningWithTopLevelIntersection() throws Exception {
        String query = "F1 == '1' && (F3 == '3' || F4 == '4')";

        List<Range> expectedRanges = new ArrayList<>();
        expectedRanges.add(makeTestRange("20200101_10", "datatype1\0a.b.c"));
        expectedRanges.add(makeTestRange("20200101_11", "datatype1\0a.b.c"));
        // F3 skips shard _12, this forces the intersection into a shard range
        expectedRanges.add(makeTestRange("20200101_12", "datatype1\0a.b.c"));
        expectedRanges.add(makeTestRange("20200101_13", "datatype1\0a.b.c"));

        List<String> expectedQueries = new ArrayList<>();
        expectedQueries.add("F1 == '1' && (F3 == '3' || F4 == '4')");
        expectedQueries.add("F1 == '1' && (F3 == '3' || F4 == '4')");
        expectedQueries.add("F1 == '1' && F4 == '4'"); // F3 skips shard _12
        expectedQueries.add("F1 == '1' && (F3 == '3' || F4 == '4')");

        runTest(query, expectedRanges, expectedQueries);
    }

    @Test
    public void testIntersectionOfNestedUnions() throws Exception {
        String query = "(F1 == '1' || F2 == '2') && (F3 == '3' || F4 == '4')";

        List<Range> expectedRanges = new ArrayList<>();
        expectedRanges.add(makeTestRange("20200101_10", "datatype1\0a.b.c"));
        expectedRanges.add(makeTestRange("20200101_11", "datatype1\0a.b.c"));
        expectedRanges.add(makeTestRange("20200101_12", "datatype1\0a.b.c"));
        expectedRanges.add(makeTestRange("20200101_13", "datatype1\0a.b.c"));

        List<String> expectedQueries = new ArrayList<>();
        expectedQueries.add("(F1 == '1' || F2 == '2') && (F3 == '3' || F4 == '4')");
        expectedQueries.add("F1 == '1' && (F3 == '3' || F4 == '4')"); // F2 skips shard _11
        expectedQueries.add("(F1 == '1' || F2 == '2') && F4 == '4'"); // F3 skips shard _12
        expectedQueries.add("(F1 == '1' || F2 == '2') && (F3 == '3' || F4 == '4')");

        runTest(query, expectedRanges, expectedQueries);
    }

    @Test
    public void testIntersectionOfNestedUnionsOnHasDelayedTerm() throws Exception {
        String query = "(F1 == '1' || F2 == '2') && (F3 == '3' || F4 == '4')";

        List<Range> expectedRanges = new ArrayList<>();
        expectedRanges.add(makeTestRange("20200101_10", "datatype1\0a.b.c"));
        expectedRanges.add(makeTestRange("20200101_11", "datatype1\0a.b.c"));
        expectedRanges.add(makeTestRange("20200101_12", "datatype1\0a.b.c"));
        expectedRanges.add(makeTestRange("20200101_13", "datatype1\0a.b.c"));

        List<String> expectedQueries = new ArrayList<>();
        expectedQueries.add("(F1 == '1' || F2 == '2') && (F3 == '3' || F4 == '4')");
        expectedQueries.add("F1 == '1' && (F3 == '3' || F4 == '4')"); // F2 skips shard_11
        expectedQueries.add("(F1 == '1' || F2 == '2') && F4 == '4'"); // F3 skips shard _12
        expectedQueries.add("(F1 == '1' || F2 == '2') && (F3 == '3' || F4 == '4')");

        runTest(query, expectedRanges, expectedQueries);
    }

    @Test
    public void testIntersectionOfNestedUnionsOnHasDelayedTerm_flipped() throws Exception {
        String query = "(F3 == '3' || F4 == '4') && (F1 == '1' || F2 == '2')";

        List<Range> expectedRanges = new ArrayList<>();
        expectedRanges.add(makeTestRange("20200101_10", "datatype1\0a.b.c"));
        expectedRanges.add(makeTestRange("20200101_11", "datatype1\0a.b.c"));
        expectedRanges.add(makeTestRange("20200101_12", "datatype1\0a.b.c"));
        expectedRanges.add(makeTestRange("20200101_13", "datatype1\0a.b.c"));

        List<String> expectedQueries = new ArrayList<>();
        expectedQueries.add("(F1 == '1' || F2 == '2') && (F3 == '3' || F4 == '4')");
        expectedQueries.add("F1 == '1' && (F3 == '3' || F4 == '4')"); // F2 skips shard_11
        expectedQueries.add("(F1 == '1' || F2 == '2') && F4 == '4'"); // F3 skips shard _12
        expectedQueries.add("(F1 == '1' || F2 == '2') && (F3 == '3' || F4 == '4')");

        runTest(query, expectedRanges, expectedQueries);
    }

    @Test
    public void testOrAndOr() throws Exception {
        String query = "F2 == '2' || (F1 == '1' && (F3 == '3' || F4 == '4'))";

        List<Range> expectedRanges = new ArrayList<>();
        expectedRanges.add(makeTestRange("20200101_10", "datatype1\0a.b.c"));
        expectedRanges.add(makeTestRange("20200101_11", "datatype1\0a.b.c"));
        expectedRanges.add(makeTestRange("20200101_12", "datatype1\0a.b.c"));
        expectedRanges.add(makeTestRange("20200101_13", "datatype1\0a.b.c"));

        List<String> expectedQueries = new ArrayList<>();
        expectedQueries.add("F2 == '2' || (F1 == '1' && (F3 == '3' || F4 == '4'))");
        expectedQueries.add("(F1 == '1' && (F3 == '3' || F4 == '4'))"); // F2 skips shard _11
        expectedQueries.add("F2 == '2' || (F1 == '1' && F4 == '4')"); // F3 skips shard _12
        expectedQueries.add("F2 == '2' || (F1 == '1' && (F3 == '3' || F4 == '4'))");

        runTest(query, expectedRanges, expectedQueries);
    }

    @Test
    public void testOrAndOrWithDeeplyNestedDelayedTerm() throws Exception {
        String query = "F2 == '2' || (F1 == '1' && (F3 == '3' || F4 == '4'))";

        List<Range> expectedRanges = new ArrayList<>();
        expectedRanges.add(makeTestRange("20200101_10", "datatype1\0a.b.c"));
        expectedRanges.add(makeTestRange("20200101_11", "datatype1\0a.b.c"));
        expectedRanges.add(makeTestRange("20200101_12", "datatype1\0a.b.c"));
        expectedRanges.add(makeTestRange("20200101_13", "datatype1\0a.b.c"));

        List<String> expectedQueries = new ArrayList<>();
        expectedQueries.add("F2 == '2' || (F1 == '1' && (F3 == '3' || F4 == '4'))");
        expectedQueries.add("(F1 == '1' && (F3 == '3' || F4 == '4'))"); // F2 skips shard _11
        expectedQueries.add("F2 == '2' || (F1 == '1' && F4 == '4')"); // F3 skips shard _12
        expectedQueries.add("F2 == '2' || (F1 == '1' && (F3 == '3' || F4 == '4'))");

        runTest(query, expectedRanges, expectedQueries);
    }

    @Test
    public void testSortingByFieldCardinality() {
        String query = "FIELD_A == '45' || FIELD_B == '34' || FIELD_C == '23'";
        String expected = "(FIELD_C == '23' || FIELD_B == '34' || FIELD_A == '45')";

        config.setSortQueryPostIndexWithFieldCounts(true);
        drive(query, expected);
    }

    @Test
    public void testSortingByTermCardinality() {
        String query = "FIELD_A == '45' || FIELD_B == '34' || FIELD_C == '23'";
        String expected = "(FIELD_C == '23' || FIELD_B == '34' || FIELD_A == '45')";

        config.setSortQueryPostIndexWithTermCounts(true);
        drive(query, expected);
    }

    private void runTest(String query, List<Range> expectedRanges, List<String> expectedQueries) throws Exception {

        assertEquals("Expected ranges and queries do not match, ranges: " + expectedRanges.size() + " queries: " + expectedQueries.size(),
                        expectedRanges.size(), expectedQueries.size());

        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        config.setBeginDate(sdf.parse("20200101"));
        config.setEndDate(sdf.parse("20200105"));

        config.setDatatypeFilter(Sets.newHashSet("datatype1"));

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("A", Sets.newHashSet(new LcNoDiacriticsType()));
        dataTypes.putAll("B", Sets.newHashSet(new LcNoDiacriticsType()));
        dataTypes.putAll("C", Sets.newHashSet(new LcNoDiacriticsType()));
        dataTypes.putAll("D", Sets.newHashSet(new LcNoDiacriticsType()));
        dataTypes.putAll("F1", Sets.newHashSet(new LcNoDiacriticsType()));
        dataTypes.putAll("F2", Sets.newHashSet(new LcNoDiacriticsType()));
        dataTypes.putAll("F3", Sets.newHashSet(new LcNoDiacriticsType()));
        dataTypes.putAll("F4", Sets.newHashSet(new LcNoDiacriticsType()));

        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());

        // Run a standard limited-scanner range stream.
        ScannerFactory scannerFactory = new ScannerFactory(config);
        RangeStream rangeStream = new RangeStream(config, scannerFactory, helper);
        rangeStream.setLimitScanners(true);
        runTest(rangeStream, script, expectedRanges, expectedQueries);

        // Run a default range stream.
        rangeStream = new RangeStream(config, scannerFactory, helper);
        rangeStream.setLimitScanners(false);
        runTest(rangeStream, script, expectedRanges, expectedQueries);

        rangeStream.close();
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

            assertTrue("Queries did not match for counter: " + counter + " on shard: " + planRange.toString() + "\nExpected: " + expectedString + "\nActual  : "
                            + plannedString, TreeEqualityVisitor.isEqual(expectedScript, planScript));
            counter++;
        }

        // Ensure we didn't miss any expected ranges or queries
        if (shardIter.hasNext())
            fail("Expected ranges still exist after test: " + shardIter.next());
        if (queryIter.hasNext())
            fail("Expected queries still exist after test: " + queryIter.next());
    }

    /**
     * Drives a query against a subset of the index data to verify post-index sorting options
     *
     * @param query
     *            the input query
     * @param expected
     *            the expected query
     */
    private void drive(String query, String expected) {
        try {
            ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);

            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            config.setBeginDate(sdf.parse("20200101"));
            config.setEndDate(sdf.parse("20200105"));

            config.setDatatypeFilter(Sets.newHashSet("sort-type"));

            Multimap<String,Type<?>> dataTypes = HashMultimap.create();
            dataTypes.putAll("FIELD_A", Sets.newHashSet(new LcNoDiacriticsType()));
            dataTypes.putAll("FIELD_B", Sets.newHashSet(new LcNoDiacriticsType()));
            dataTypes.putAll("FIELD_C", Sets.newHashSet(new LcNoDiacriticsType()));

            config.setQueryFieldsDatatypes(dataTypes);
            config.setIndexedFields(dataTypes);

            MockMetadataHelper helper = new MockMetadataHelper();
            helper.setIndexedFields(dataTypes.keySet());

            // Run a standard limited-scanner range stream.
            ScannerFactory scannerFactory = new ScannerFactory(config);
            try (RangeStream rangeStream = new RangeStream(config, scannerFactory, helper)) {
                rangeStream.setLimitScanners(true);

                Iterator<QueryPlan> plans = rangeStream.streamPlans(script).iterator();

                assertTrue(plans.hasNext());
                QueryPlan plan = plans.next();

                String plannedQuery = plan.getQueryString();
                assertEquals(expected, plannedQuery);

                assertFalse(plans.hasNext());
            }
        } catch (Exception e) {
            fail("test failed: " + e.getMessage());
        }
    }
}
