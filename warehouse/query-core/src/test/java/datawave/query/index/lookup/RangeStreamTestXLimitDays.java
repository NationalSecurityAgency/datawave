package datawave.query.index.lookup;

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
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
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
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import static datawave.common.test.utils.query.RangeFactoryForTests.*;
import static datawave.util.TableName.SHARD_INDEX;
import static org.junit.Assert.*;

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
public class RangeStreamTestXLimitDays extends RangeStreamTestX{

    // A && B
    // A || B
    // A && ( B || C )
    // A || ( B && C )
    // (A && B) || (C && D)
    // (A || B) && (C || D)

    @BeforeClass
    public static void setupAccumulo() throws Exception {
        RangeStreamTestX.setupAccumulo();
    }

    @Before
    public void setupTest() {
        config = new ShardQueryConfiguration();
        config.getServiceConfiguration().getIndexingConfiguration().setEnableRangeScannerLimitDays(true);
        config.setShardsPerDayThreshold(20);
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
    public void testIntersection_ofShardAndDayDisableBypass() throws Exception {
        String query = "A == 'all_day' && B == 'all'";

        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                expectedQueryStrings.add("(B == 'all' && A == 'all_day')");
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings, false);
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
    public void testIntersection_ofShardAndDay_unequalStartDisableBypass() throws Exception {
        String query = "A == 'unequal_start' && B == 'all_day'";

        // Day 1 is skipped in the unequal start and Day 5 is skipped in the unequal end
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 2; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                expectedQueryStrings.add("(A == 'unequal_start' && B == 'all_day')");
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings,false);
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
    public void testIntersection_ofShardAndDay_unequalStopDisableBypass() throws Exception {
        String query = "A == 'unequal_stop' && B == 'all_day'";

        // Day 1 is skipped in the unequal start and Day 5 is skipped in the unequal end
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 4; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                expectedQueryStrings.add("(A == 'unequal_stop' && B == 'all_day')");
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings, false);
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
    public void testIntersection_ofShardAndDay_unevenStartDisableBypass() throws Exception {
        String query = "A == 'uneven_start' && B == 'all_day'";

        // Shard 1 is skipped for every day
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 1; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                expectedQueryStrings.add("(A == 'uneven_start' && B == 'all_day')");
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings,false);
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
                expectedQueryStrings.add("(B == 'all_day' && A == 'uneven_stop')");
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings);
    }

    // A && B
    @Test
    public void testIntersection_ofShardAndDay_unevenStopDisableBypass() throws Exception {
        String query = "A == 'uneven_stop' && B == 'all_day'";

        // Shard 9 is skipped for every day
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 9; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                expectedQueryStrings.add("(B == 'all_day' && A == 'uneven_stop')");
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings,false);
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
                expectedQueryStrings.add("(A == 'all_day' && B == 'missing_shards')");
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings);
    }

    // A && B
    @Test
    public void testIntersection_ofShardAndDay_missingShardsDisableBypass() throws Exception {
        String query = "A == 'all_day' && B == 'missing_shards'";

        // Shards are missing for day 3
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            if (ii == 3)
                continue;
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                expectedQueryStrings.add("(A == 'all_day' && B == 'missing_shards')");
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings,false);
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
    public void testIntersection_ofShardAndDay_tickTockShardsDisableBypass() throws Exception {
        String query = "A == 'tick_tock' && B == 'tick_tock_day'";

        // No intersection exists between A & B for tick-tock shards for day 3
        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();

        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                if (ii != 3) {
                    expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                    expectedQueryStrings.add("(A == 'tick_tock' && B == 'tick_tock_day')");
                }
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings, false);
    }

    // A && B
    // previously this test showed delayed ranges, but because RangeStream no longer pushes down ranges that
    // work across all shards as days, this test will intersect the terms for all shards.
    @Test
    public void testPreviousIntersection_ofDays() throws Exception {
        String query = "A == 'all_day' && B == 'all_day'";

        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                expectedQueryStrings.add("(A == 'all_day' && B == 'all_day')");
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
            for(int jj = 0; jj < 10 ; jj++ ) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                expectedQueryStrings.add("(A == 'all_day' && B == 'unequal_start_day')");
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
            for(int jj = 0; jj < 10 ; jj++ ) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                expectedQueryStrings.add("(A == 'all_day' && B == 'unequal_stop_day')");
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
            for(int jj = 1; jj < 10 ; jj++ ) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                expectedQueryStrings.add("(A == 'all_day' && B == 'uneven_start_day')");
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
            for(int jj = 0; jj < 10 ; jj++ ) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                expectedQueryStrings.add("((A == 'all_day')) && (B == 'all_day')");
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
            for(int jj = 0; jj < 9 ; jj++ ) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                expectedQueryStrings.add("(A == 'all_day' && B == 'uneven_stop_day')");
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
            if (ii == 3)
                continue;
            for(int jj = 0; jj < 10 ; jj++ ) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                expectedQueryStrings.add("(A == 'all_day' && B == 'missing_shards_day')");
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
                for(int jj = 0; jj < 10 ; jj++ ) {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings.add("(A == 'tick_tock_day' && B == 'tick_tock_day')");
                }
            }
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
            for(int jj = 0; jj < 10 ; jj++ ) {
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
            if (ii == 1) {
                for (int jj = 0; jj < 10; jj++) {
                    expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                    expectedQueryStrings.add("A == 'all'");
                }
            } else {
                for(int jj = 0; jj < 10 ; jj++ ) {
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
            if (ii == 5) {
                for (int jj = 0; jj < 10; jj++) {
                    expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                    expectedQueryStrings.add("A == 'all'");
                }
            } else {
                for(int jj = 0; jj < 10 ; jj++ ) {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings.add("(A == 'all' || B == 'unequal_stop_day')");
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
            for(int jj = 0; jj < 10 ; jj++ ) {
                if (jj==0){
                    expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                    expectedQueryStrings.add("A == 'all'");
                }
                else {
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
            for(int jj = 0; jj < 10 ; jj++ ) {
                if (jj==9){
                    expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                    expectedQueryStrings.add("A == 'all'");
                }
                else {
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
            if (ii == 3) {
                for (int jj = 0; jj < 10; jj++) {
                    expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                    expectedQueryStrings.add("A == 'all'");
                }
            } else {
                for (int jj = 0; jj < 10; jj++) {
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
        for (int jj = 0; jj < 10; jj++) {
            expectedRanges.add(makeShardedRange("20200101_" + jj));
        }
        for (int jj = 0; jj < 10; jj++) {
            expectedRanges.add(makeShardedRange("20200102_" + jj));
        }
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
        for (int jj = 0; jj < 10; jj++) {
            expectedRanges.add(makeShardedRange("20200104_" + jj));
        }
        for (int jj = 0; jj < 10; jj++) {
            expectedRanges.add(makeShardedRange("20200105_" + jj));
        }

        for (int jj = 0; jj < 10; jj++) {
            expectedQueryStrings.add("A == 'all' || B == 'tick_tock_day'");
            expectedQueryStrings.add("A == 'all' || B == 'tick_tock_day'");
        }
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
        for (int jj = 0; jj < 10; jj++) {
            expectedQueryStrings.add("A == 'all' || B == 'tick_tock_day'");
            expectedQueryStrings.add("A == 'all' || B == 'tick_tock_day'");
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
            for(int jj = 0; jj < 10 ; jj++ ) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                expectedQueryStrings.add("(A == 'all_day' || B == 'all_day')");
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
            for(int jj = 0; jj < 10 ; jj++ ) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 1) {
                    expectedQueryStrings.add("A == 'all_day'");
                } else {
                    expectedQueryStrings.add("(A == 'all_day' || (B == 'unequal_start_day'))");
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
            for(int jj = 0; jj < 10 ; jj++ ) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 5) {
                    expectedQueryStrings.add("A == 'all_day'");
                } else {
                    expectedQueryStrings.add("(A == 'all_day' || B == 'unequal_stop_day')");
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
            for(int jj = 0; jj < 10 ; jj++ ) {
                if (ii==0 || jj>=1) {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings.add("(A == 'all_day' || B == 'uneven_start_day')");
                }else if (jj==1) {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings.add("(A == 'all_day')");
                }else{
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings.add("(A == 'all_day')");
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
            for(int jj = 0; jj < 10 ; jj++ ) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (jj == 9){
                    expectedQueryStrings.add("A == 'all_day'");
                }
                else
                    expectedQueryStrings.add("(A == 'all_day' || B == 'uneven_stop_day')");
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
            for(int jj = 0; jj < 10 ; jj++ ) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 3) {
                    expectedQueryStrings.add("A == 'all_day'");
                } else {
                    expectedQueryStrings.add("(A == 'all_day' || B == 'missing_shards_day')");
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
            if (ii != 3) {
                for(int jj = 0; jj < 10 ; jj++ ) {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings.add("(A == 'tick_tock_day' || B == 'tick_tock_day')");
                }
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
    public void testIntersection_withNestedUnion_allDays() throws Exception {
        String query = "A == 'all_day' && (B == 'all_day' || C == 'all_day')";

        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for(int jj = 0; jj < 10 ; jj++ ) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                expectedQueryStrings
                        .add("(A == 'all_day' && (B == 'all_day' || C == 'all_day'))");
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
            for(int jj = 0; jj < 10 ; jj++ ) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 1) {
                    expectedQueryStrings.add("(A == 'all_day' && B == 'all_day')");
                } else {
                    expectedQueryStrings
                            .add("(A == 'all_day' && (B == 'all_day' || C == 'unequal_start_day'))");
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
            for(int jj = 0; jj < 10 ; jj++ ) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 5) {
                    expectedQueryStrings.add("A == 'all_day' && (B == 'all_day')");
                } else {
                    expectedQueryStrings
                            .add("(A == 'all_day' && (B == 'all_day' || C == 'unequal_stop_day'))");
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
                    expectedQueryStrings
                            .add("(A == 'all_day' && B == 'all_day')");
                }
                else{
                    expectedQueryStrings
                            .add("(A == 'all_day' && (B == 'all_day' || C == 'uneven_start_day'))");
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
                if (jj==9){
                    expectedQueryStrings
                            .add("A == 'all_day' && B == 'all_day'");
                }
                else
                    expectedQueryStrings
                            .add("A == 'all_day' && (B == 'all_day' || C == 'uneven_stop_day')");
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
            for(int jj = 0; jj < 10 ; jj++ ) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 3) {
                    expectedQueryStrings.add("(A == 'all_day' && B == 'all_day')");
                } else {
                    expectedQueryStrings
                            .add("(A == 'all_day' && (B == 'all_day' || C == 'missing_shards_day'))");
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
                if (ii == 3 && (jj != 2 && jj != 6)){
                    expectedQueryStrings.add("(A == 'all_day' && (B == 'all_day' ))");
                }
                else {
                    expectedQueryStrings.add("(A == 'all_day' && (B == 'all_day' || C == 'tick_tock'))");
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
                expectedQueryStrings.add("(A == 'all_day' && (B == 'all' || C == 'all'))");
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings);
    }

    // A && ( B || C )
    @Test
    public void testIntersection_dayWithNestedUnionOfShardsDisableBypass() throws Exception {
        String query = "A == 'all_day' && (B == 'all' || C == 'all')";

        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                expectedQueryStrings.add("(A == 'all_day' && (B == 'all' || C == 'all'))");
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings, false);
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
                    expectedQueryStrings.add("(A == 'all_day' && B == 'all')");
                } else {
                    expectedQueryStrings.add("(A == 'all_day' && (B == 'all' || C == 'unequal_start'))");
                }
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings);
    }

    // A && ( B || C )
    @Test
    public void testIntersection_dayWithNestedUnionOfShards_unequalStartDisableBypass() throws Exception {
        String query = "A == 'all_day' && (B == 'all' || C == 'unequal_start')";

        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii == 1) {
                    expectedQueryStrings.add("(A == 'all_day' && B == 'all')");
                } else {
                    expectedQueryStrings.add("(A == 'all_day' && (B == 'all' || C == 'unequal_start'))");
                }
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings, false);
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
                    expectedQueryStrings.add("(A == 'all_day' && B == 'all')");
                } else {
                    //(A == 'all_day' && (B == 'all' || C == 'unequal_stop'))
                    expectedQueryStrings.add("((B == 'all' || C == 'unequal_stop') && A == 'all_day')");
                }
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings);
    }

    // A && ( B || C )
    @Test
    public void testIntersection_dayWithNestedUnionOfShards_unequalStopDisableBypass() throws Exception {
        String query = "A == 'all_day' && (B == 'all' || C == 'unequal_stop')";

        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii == 5) {
                    expectedQueryStrings.add("(A == 'all_day' && B == 'all')");
                } else {
                    //(A == 'all_day' && (B == 'all' || C == 'unequal_stop'))
                    expectedQueryStrings.add("((B == 'all' || C == 'unequal_stop') && A == 'all_day')");
                }
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings, false);
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
                    expectedQueryStrings.add("(A == 'all_day' && B == 'all')");
                } else {
                    expectedQueryStrings.add("(A == 'all_day' && (B == 'all' || C == 'uneven_start'))");
                }
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings);
    }

    // A && ( B || C )
    @Test
    public void testIntersection_dayWithNestedUnionOfShards_unevenStartDisableBypass() throws Exception {
        String query = "A == 'all_day' && (B == 'all' || C == 'uneven_start')";

        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (jj == 0) {
                    expectedQueryStrings.add("(A == 'all_day' && B == 'all')");
                } else {
                    expectedQueryStrings.add("(A == 'all_day' && (B == 'all' || C == 'uneven_start'))");
                }
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings, false);
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
                    expectedQueryStrings.add("(A == 'all_day' && B == 'all')");
                } else {
                    expectedQueryStrings.add("(A == 'all_day' && (B == 'all' || C == 'uneven_stop'))");
                }
            }
        }
        runTest(query, expectedRanges, expectedQueryStrings);
    }

    // A && ( B || C )
    @Test
    public void testIntersection_dayWithNestedUnionOfShards_unevenStopDisableBypass() throws Exception {
        String query = "A == 'all_day' && (B == 'all' || C == 'uneven_stop')";

        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (jj == 9) {
                    expectedQueryStrings.add("(A == 'all_day' && B == 'all')");
                } else {
                    expectedQueryStrings.add("(A == 'all_day' && (B == 'all' || C == 'uneven_stop'))");
                }
            }
        }
        runTest(query, expectedRanges, expectedQueryStrings, false);
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
                    expectedQueryStrings.add("(A == 'all_day' && B == 'all')");
                } else {
                    expectedQueryStrings.add("(A == 'all_day' && (B == 'all' || C == 'missing_shards'))");
                }
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings);
    }

    // A && ( B || C )
    @Test
    public void testIntersection_dayWithNestedUnionOfShards_missingShardsDisableBypass() throws Exception {
        String query = "A == 'all_day' && (B == 'all' || C == 'missing_shards')";

        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii == 3) {
                    expectedQueryStrings.add("(A == 'all_day' && B == 'all')");
                } else {
                    expectedQueryStrings.add("(A == 'all_day' && (B == 'all' || C == 'missing_shards'))");
                }
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings, false);
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
                    expectedQueryStrings.add("(A == 'all_day' && (B == 'all' || C == 'tick_tock'))");
                } else {
                    expectedQueryStrings.add("(A == 'all_day' && B == 'all')");
                }
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings);
    }

    // A && ( B || C )
    @Test
    public void testIntersection_dayWithNestedUnionOfShards_tickTockShardsDisableBypass() throws Exception {
        String query = "A == 'all_day' && (B == 'all' || C == 'tick_tock')";

        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii != 3 || jj == 2 || jj == 6) {
                    expectedQueryStrings.add("(A == 'all_day' && (B == 'all' || C == 'tick_tock'))");
                } else {
                    expectedQueryStrings.add("(A == 'all_day' && B == 'all')");
                }
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings, false);
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
                expectedQueryStrings.add("A == 'all' && (B == 'all_day' || (C == 'all_day'))");
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
                    expectedQueryStrings.add("A == 'all' && (B == 'all_day')");
                } else {
                    expectedQueryStrings.add("A == 'all' && (B == 'all_day' || (C == 'unequal_start_day'))");
                }
            }
        }
        runTest(query, expectedRanges, expectedQueryStrings);
    }

    // A && ( B || C )
    @Test
    public void testIntersection_shardWithNestedUnionOfDays_unequalStartDisableBypass() throws Exception {
        String query = "A == 'all' && (B == 'all_day' || C == 'unequal_start_day')";

        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii == 1) {
                    expectedQueryStrings.add("A == 'all' && (B == 'all_day')");
                } else {
                    expectedQueryStrings.add("A == 'all' && (B == 'all_day' || (C == 'unequal_start_day'))");
                }
            }
        }
        runTest(query, expectedRanges, expectedQueryStrings, false);
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
                    expectedQueryStrings.add("A == 'all' && (B == 'all_day')");
                } else {
                    expectedQueryStrings.add("A == 'all' && (B == 'all_day' || C == 'unequal_stop_day')");
                }
            }
        }
        runTest(query, expectedRanges, expectedQueryStrings);
    }

    // A && ( B || C )
    @Test
    public void testIntersection_shardWithNestedUnionOfDays_unequalStopDisableBypass() throws Exception {
        String query = "A == 'all' && (B == 'all_day' || C == 'unequal_stop_day')";

        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii == 5) {
                    expectedQueryStrings.add("A == 'all' && (B == 'all_day')");
                } else {
                    expectedQueryStrings.add("A == 'all' && (B == 'all_day' || C == 'unequal_stop_day')");
                }
            }
        }
        runTest(query, expectedRanges, expectedQueryStrings, false);
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
                if (jj==0) {
                    expectedQueryStrings.add("A == 'all' && B == 'all_day'");
                }
                else{
                    expectedQueryStrings.add("A == 'all' && (B == 'all_day' || C == 'uneven_start_day')");
                }
            }
        }
        runTest(query, expectedRanges, expectedQueryStrings);
    }

    // A && ( B || C )
    @Test
    public void testIntersection_shardWithNestedUnionOfDays_unevenStartDisableBypass() throws Exception {
        String query = "A == 'all' && (B == 'all_day' || C == 'uneven_start_day')";

        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (jj==0) {
                    expectedQueryStrings.add("A == 'all' && B == 'all_day'");
                }
                else{
                    expectedQueryStrings.add("A == 'all' && (B == 'all_day' || C == 'uneven_start_day')");
                }
            }
        }
        runTest(query, expectedRanges, expectedQueryStrings, false);
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
                if (jj==9){
                    expectedQueryStrings.add("A == 'all' && B == 'all_day'");
                }
                else {
                    expectedQueryStrings.add("A == 'all' && (B == 'all_day' || C == 'uneven_stop_day')");
                }
            }
        }
        runTest(query, expectedRanges, expectedQueryStrings);
    }

    // A && ( B || C )
    @Test
    public void testIntersection_shardWithNestedUnionOfDays_unevenStopDisableBypass() throws Exception {
        String query = "A == 'all' && (B == 'all_day' || C == 'uneven_stop_day')";

        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (jj==9){
                    expectedQueryStrings.add("A == 'all' && B == 'all_day'");
                }
                else {
                    expectedQueryStrings.add("A == 'all' && (B == 'all_day' || C == 'uneven_stop_day')");
                }
            }
        }
        runTest(query, expectedRanges, expectedQueryStrings, false);
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
                    expectedQueryStrings.add("A == 'all' && (B == 'all_day')");
                } else {
                    expectedQueryStrings.add("A == 'all' && (B == 'all_day' || C == 'missing_shards_day')");
                }
            }
        }
        runTest(query, expectedRanges, expectedQueryStrings);
    }

    // A && ( B || C )
    @Test
    public void testIntersection_shardWithNestedUnionOfDays_missingShardsDisableBypass() throws Exception {
        String query = "A == 'all' && (B == 'all_day' || C == 'missing_shards_day')";

        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii == 3) {
                    expectedQueryStrings.add("A == 'all' && (B == 'all_day')");
                } else {
                    expectedQueryStrings.add("A == 'all' && (B == 'all_day' || C == 'missing_shards_day')");
                }
            }
        }
        runTest(query, expectedRanges, expectedQueryStrings, false);
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
                if ((ii == 3 && (jj==2 || jj == 6)) || (ii >0 && ii != 3)) {
                    // Not enough shards to force rolling the C-term to a day range, thus no delay.
                    expectedQueryStrings.add("A == 'all' && (B == 'all_day' || (C == 'tick_tock_day'))");
                } else {
                    expectedQueryStrings.add("A == 'all' && (B == 'all_day')");
                }
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings);
    }

    // A && ( B || C )
    @Test
    public void testIntersection_shardWithNestedUnionOfDays_tickTockShardsDisableBypass() throws Exception {
        String query = "A == 'all' && (B == 'all_day' || C == 'tick_tock_day')";

        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();

        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if ((ii == 3 && (jj==2 || jj == 6)) || (ii >0 && ii != 3)) {
                    // Not enough shards to force rolling the C-term to a day range, thus no delay.
                    expectedQueryStrings.add("A == 'all' && (B == 'all_day' || (C == 'tick_tock_day'))");
                } else {
                    expectedQueryStrings.add("A == 'all' && (B == 'all_day')");
                }
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings,false);
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
                expectedQueryStrings
                        .add("(A == 'all_day' || (B == 'all_day' && C == 'all_day'))");
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
            for(int jj = 0; jj < 10 ; jj++ ) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 1) {
                    expectedQueryStrings.add("A == 'all_day'");
                } else {
                    expectedQueryStrings
                            .add("(A == 'all_day' || (B == 'all_day' && (C == 'unequal_start_day')))");
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
            for(int jj = 0; jj < 10 ; jj++ ) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 5) {
                    expectedQueryStrings.add("A == 'all_day'");
                } else {
                    expectedQueryStrings
                            .add("(A == 'all_day' || (B == 'all_day' && C == 'unequal_stop_day'))");
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
                    expectedQueryStrings
                            .add("(A == 'all_day' )");
                }
                else {
                    expectedQueryStrings
                            .add("(A == 'all_day' || (B == 'all_day' && C == 'uneven_start_day'))");
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
            for(int jj = 0; jj < 10 ; jj++ ) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (jj==9){
                    expectedQueryStrings
                            .add("(A == 'all_day')");
                }
                else {
                    expectedQueryStrings
                            .add("(A == 'all_day' || (B == 'all_day' && C == 'uneven_stop_day'))");
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
            for(int jj = 0; jj < 10 ; jj++ ) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 3) {
                    expectedQueryStrings.add("A == 'all_day'");
                } else {
                    expectedQueryStrings
                            .add("(A == 'all_day' || (B == 'all_day' && C == 'missing_shards_day'))");
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
            for(int jj = 0; jj < 10 ; jj++ ) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii==3 && (jj != 2 && jj != 6)){
                    expectedQueryStrings.add("A == 'all_day'");
                }else if (ii == 3) {
                    expectedQueryStrings.add("(A == 'all_day' || (B == 'all_day' && C == 'tick_tock_day'))");
                } else {
                    expectedQueryStrings
                            .add("(A == 'all_day' || (B == 'all_day' && C == 'tick_tock_day'))");
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
            for(int jj = 0; jj < 10 ; jj++ ) {
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
            for(int jj = 0; jj < 10 ; jj++ ) {
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
            for(int jj = 0; jj < 10 ; jj++ ) {
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
            for(int jj = 0; jj < 10 ; jj++ ) {
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
            for(int jj = 0; jj < 10 ; jj++ ) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 0 || (jj==9)) {
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
            for(int jj = 0; jj < 10 ; jj++ ) {
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
                if ((jj <= 9 && ii < 3) || (ii == 3 && (jj ==2 || jj == 6)) || (ii == 4 ) || ii==5){
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings.add("A == 'all_day' || (B == 'all' && C == 'tick_tock')");
                }
                else {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings.add("A == 'all_day'");
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
                expectedQueryStrings.add("A == 'all' || (B == 'all_day' && (C == 'all_day'))");
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
            if (ii != 1) {
                for(int jj = 0; jj < 10 ; jj++ ) {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings.add("A == 'all' || (B == 'all_day' && C == 'unequal_start_day')");
                }
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
                for(int jj = 0; jj < 10 ; jj++ ) {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings.add("A == 'all' || (B == 'all_day' && C == 'unequal_stop_day')");
                }
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
            for(int jj = 0; jj < 10 ; jj++ ) {
                if (jj==0) {
                    expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                    expectedQueryStrings.add("A == 'all'"); //  || (B == 'all_day' && C == 'uneven_start_day'
                }
                else{
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
            for(int jj = 0; jj < 10 ; jj++ ) {
                if (jj < 9) {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings.add("A == 'all' || (B == 'all_day' && C == 'uneven_stop_day')");
                }else {
                    expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                    expectedQueryStrings.add("A == 'all'");
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
            if (ii == 3) {
                for (int jj = 0; jj < 10; jj++) {
                    expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                    expectedQueryStrings.add("A == 'all'");
                }
            } else {
                for(int jj = 0; jj < 10 ; jj++ ) {
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
            if (ii != 3) {
                for(int jj = 0; jj < 10 ; jj++ ) {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings.add("A == 'all' || (B == 'all_day' && C == 'tick_tock_day')");
                }
            } else {
                for (int jj = 0; jj < 10; jj++) {
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
    public void testUnion_ofNestedIntersections_allDays() throws Exception {
        String query = "(A == 'all_day' && B == 'all_day') || (C == 'all_day' && D == 'all_day')";

        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                expectedQueryStrings
                        .add("(A == 'all_day' && B == 'all_day') || (C == 'all_day' && D == 'all_day')");
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
            for(int jj = 0; jj < 10 ; jj++ ) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 1) {
                    expectedQueryStrings.add("(A == 'all_day' && B == 'all_day')");
                } else {
                    expectedQueryStrings
                            .add("(A == 'all_day' && B == 'all_day') || (C == 'all_day' && D == 'unequal_start')");
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
            for(int jj = 0; jj < 10 ; jj++ ) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 5) {
                    expectedQueryStrings.add("(A == 'all_day' && B == 'all_day')");
                } else {
                    expectedQueryStrings
                            .add("(A == 'all_day' && B == 'all_day') || (C == 'all_day' && D == 'unequal_stop')");
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
                if (jj==0){
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings
                            .add("(A == 'all_day' && B == 'all_day')");
                }
                else {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings
                            .add("(A == 'all_day' && B == 'all_day') || (C == 'all_day' && D == 'uneven_start')");
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
                if (jj==9){
                    expectedQueryStrings
                            .add("A == 'all_day' && B == 'all_day'");
                }
                else
                    expectedQueryStrings
                            .add("(A == 'all_day' && B == 'all_day') || (C == 'all_day' && D == 'uneven_stop')");
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
            for(int jj = 0; jj < 10 ; jj++ ) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 3) {
                    expectedQueryStrings.add("(A == 'all_day' && B == 'all_day')");
                } else {
                    expectedQueryStrings
                            .add("(A == 'all_day' && B == 'all_day') || (C == 'all_day' && (D == 'missing_shards_day'))");
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
                if (ii == 3 && jj != 3 && jj != 7) {
                    expectedQueryStrings
                            .add("(A == 'all_day' && B == 'all_day') ");
                }else {
                    expectedQueryStrings
                            .add("(A == 'all_day' && B == 'all_day') || (C == 'all_day' && D == 'tick_tock')");
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
                expectedQueryStrings
                        .add("(A == 'all' && B == 'all_day') || (C == 'all' && (D == 'all_day'))");
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings);
    }

    // (A && B) || (C && D)
    @Test
    public void testUnion_ofNestedIntersections_distributedDaysDisableBypass() throws Exception {
        String query = "(A == 'all' && B == 'all_day') || (C == 'all' && D == 'all_day')";

        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                expectedQueryStrings
                        .add("(A == 'all' && B == 'all_day') || (C == 'all' && (D == 'all_day'))");
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings,false);
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
                    expectedQueryStrings
                            .add("(A == 'all' && B == 'all_day') || (C == 'all' && (D == 'unequal_start_day'))");
                }
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings);
    }

    // (A && B) || (C && D)
    @Test
    public void testUnion_ofNestedIntersections_distributedDays_unequalStartDisableBypass() throws Exception {
        String query = "(A == 'all' && B == 'all_day') || (C == 'all' && D == 'unequal_start_day')";

        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii == 1) {
                    expectedQueryStrings.add("(A == 'all' && B == 'all_day')");
                } else {
                    expectedQueryStrings
                            .add("(A == 'all' && B == 'all_day') || (C == 'all' && (D == 'unequal_start_day'))");
                }
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings, false);
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
                    expectedQueryStrings
                            .add("(A == 'all' && B == 'all_day') || (C == 'all' && (D == 'unequal_stop_day'))");
                }
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings);
    }

    // (A && B) || (C && D)
    @Test
    public void testUnion_ofNestedIntersections_distributedDays_unequalStopDisableBypass() throws Exception {
        String query = "(A == 'all' && B == 'all_day') || (C == 'all' && D == 'unequal_stop_day')";

        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii == 5) {
                    expectedQueryStrings.add("(A == 'all' && B == 'all_day')");
                } else {
                    expectedQueryStrings
                            .add("(A == 'all' && B == 'all_day') || (C == 'all' && (D == 'unequal_stop_day'))");
                }
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings, false);
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
                    expectedQueryStrings
                            .add("(A == 'all' && B == 'all_day') || (C == 'all' && (D == 'uneven_start_day'))");
                }
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings);
    }

    // (A && B) || (C && D)
    @Test
    public void testUnion_ofNestedIntersections_distributedDays_unevenStartDisableBypass() throws Exception {
        String query = "(A == 'all' && B == 'all_day') || (C == 'all' && D == 'uneven_start_day')";

        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (jj == 0) {
                    expectedQueryStrings.add("(A == 'all' && B == 'all_day')");
                } else {
                    expectedQueryStrings
                            .add("(A == 'all' && B == 'all_day') || (C == 'all' && (D == 'uneven_start_day'))");
                }
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings, false);
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
                if (ii == 0 || jj == 9) {
                    expectedQueryStrings.add("(A == 'all' && B == 'all_day')");
                } else {
                    expectedQueryStrings
                            .add("(A == 'all' && B == 'all_day') || (C == 'all' && (D == 'uneven_stop_day'))");
                }
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings);
    }

    // (A && B) || (C && D)
    @Test
    public void testUnion_ofNestedIntersections_distributedDays_unevenStopDisableBypass() throws Exception {
        String query = "(A == 'all' && B == 'all_day') || (C == 'all' && D == 'uneven_stop_day')";

        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii == 0 || jj == 9) {
                    expectedQueryStrings.add("(A == 'all' && B == 'all_day')");
                } else {
                    expectedQueryStrings
                            .add("(A == 'all' && B == 'all_day') || (C == 'all' && (D == 'uneven_stop_day'))");
                }
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings, false);
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
                    expectedQueryStrings
                            .add("(A == 'all' && B == 'all_day') || (C == 'all' && (D == 'missing_shards_day'))");
                }
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings);
    }

    // (A && B) || (C && D)
    @Test
    public void testUnion_ofNestedIntersections_distributedDays_missingShardsDisableBypass() throws Exception {
        String query = "(A == 'all' && B == 'all_day') || (C == 'all' && D == 'missing_shards_day')";

        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii == 3) {
                    expectedQueryStrings.add("(A == 'all' && B == 'all_day')");
                } else {
                    expectedQueryStrings
                            .add("(A == 'all' && B == 'all_day') || (C == 'all' && (D == 'missing_shards_day'))");
                }
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings,false);
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

    // (A && B) || (C && D)
    @Test
    public void testUnion_ofNestedIntersections_distributedDays_tickTockShardsDisableBypass() throws Exception {
        String query = "(A == 'all' && B == 'all_day') || (C == 'all' && D == 'tick_tock')";

        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii != 3 || jj == 3 || jj == 7) {
                    expectedQueryStrings.add("(A == 'all' && B == 'all_day') || (C == 'all' && D == 'tick_tock')");
                } else {
                    expectedQueryStrings.add("(A == 'all' && B == 'all_day')");
                }
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings,false);
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
                expectedQueryStrings
                        .add("(A == 'all_day' || B == 'all_day') && ((C == 'all_day') || (D == 'all_day'))");
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
            for(int jj = 0; jj < 10 ; jj++ ) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 1) {
                    expectedQueryStrings
                            .add("(A == 'all_day' || B == 'all_day') && ((C == 'all_day'))");
                } else {
                    expectedQueryStrings
                            .add("(A == 'all_day' || B == 'all_day') && ((C == 'all_day') || (D == 'unequal_start_day'))");
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
                    expectedQueryStrings
                            .add("(A == 'all_day' || B == 'all_day') && ((C == 'all_day'))");
                } else {
                    expectedQueryStrings
                            .add("(A == 'all_day' || B == 'all_day') && ((C == 'all_day') || (D == 'unequal_stop_day'))");
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
                if (  jj == 0) {
                    expectedQueryStrings
                            .add("(A == 'all_day' || B == 'all_day') && ((C == 'all_day'))");
                }else{
                    expectedQueryStrings
                            .add("(A == 'all_day' || B == 'all_day') && ((C == 'all_day') || (D == 'uneven_start_day'))");
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
            for(int jj = 0; jj < 10 ; jj++ ) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if(jj == 9){
                    expectedQueryStrings.add("(A == 'all_day' || B == 'all_day') && ((C == 'all_day'))");
                } else {
                    expectedQueryStrings
                            .add("(A == 'all_day' || B == 'all_day') && ((C == 'all_day') || (D == 'uneven_stop_day'))");
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
            for(int jj = 0; jj < 10 ; jj++ ) {
                expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                if (ii == 3) {
                    expectedQueryStrings
                            .add("(A == 'all_day' || B == 'all_day') && ((C == 'all_day'))");
                } else {
                    expectedQueryStrings
                            .add("(A == 'all_day' || B == 'all_day') && ((C == 'all_day') || (D == 'missing_shards_day'))");
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
                if (ii==1 || ii==2 || (ii==3&&(jj==3||jj==7)) || ii==4 || ii==5) {
                    expectedQueryStrings
                            .add("((D == 'tick_tock' || C == 'all_day') && (A == 'all_day' || B == 'all_day'))");
                }
                else{
                    expectedQueryStrings
                            .add("(A == 'all_day' || B == 'all_day') && (C == 'all_day')");
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
                expectedQueryStrings.add("((A == 'all') || B == 'all_day') && ((C == 'all') || D == 'all_day')");
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
            if (ii == 1) {
                for (int jj = 0; jj < 10; jj++) {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings.add("((A == 'all') || B == 'all_day') && ((C == 'all'))");
                }
            } else {
                for(int jj = 0; jj < 10 ; jj++ ) {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings
                            .add("((A == 'all') || B == 'all_day') && ((C == 'all') || D == 'unequal_start_day')");
                }
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings);
    }

    // (A || B) && (C || D)
    @Test
    public void testIntersection_ofNestedUnions_distributedDays_unequalStartDisableBypass() throws Exception {
        String query = "(A == 'all' || B == 'all_day') && (C == 'all' || D == 'unequal_start_day')";

        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            if (ii == 1) {
                for (int jj = 0; jj < 10; jj++) {
                    expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                    expectedQueryStrings.add("((A == 'all') || B == 'all_day') && ((C == 'all'))");
                }
            } else {
                for(int jj = 0; jj < 10 ; jj++ ) {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings
                            .add("((A == 'all') || B == 'all_day') && ((C == 'all') || D == 'unequal_start_day')");
                }
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings,false);
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
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings.add("((A == 'all') || B == 'all_day') && ((C == 'all'))");
                }
            } else {
                for(int jj = 0; jj < 10 ; jj++ ) {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings
                            .add("((A == 'all') || B == 'all_day') && ((C == 'all') || D == 'unequal_stop_day')");
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
                    expectedQueryStrings
                            .add(" (C == 'all' && (A == 'all' || B == 'all_day'))");
                }
                else {


                    expectedQueryStrings
                            .add("((A == 'all') || B == 'all_day') && ((C == 'all') || D == 'uneven_start_day')");
                }

            }


        }

        runTest(query, expectedRanges, expectedQueryStrings);
    }


    @Test
    public void testIntersection_ofNestedUnions_distributedDays_unevenStartDisableBypass() throws Exception {
        String query = "(A == 'all' || B == 'all_day') && (C == 'all' || D == 'uneven_start_day')";

        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                if (jj == 0) {
                    expectedRanges.add(makeTestRange("2020010" + ii + "_" + 0, "datatype1\0a.b.c"));
                    expectedQueryStrings
                            .add(" (C == 'all' && (A == 'all' || B == 'all_day'))");
                }
                else {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));

                    expectedQueryStrings
                            .add("((A == 'all') || B == 'all_day') && ((C == 'all') || D == 'uneven_start_day')");
                }

            }


        }

        runTest(query, expectedRanges, expectedQueryStrings,false);
    }

    // (A || B) && (C || D)
    @Test
    public void testIntersection_ofNestedUnions_distributedDays_unevenStop() throws Exception {
        String query = "(A == 'all' || B == 'all_day') && (C == 'all' || D == 'uneven_stop_day')";

        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for(int jj = 0; jj < 10 ; jj++ ) {
                if (jj < 9 ) {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings
                            .add("((A == 'all') || B == 'all_day') && ((C == 'all') || D == 'uneven_stop_day')");
                }
                else {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings
                            .add("((A == 'all') || B == 'all_day') && ((C == 'all'))"); // || D == 'uneven_stop_day'
                }

            }
        }

        runTest(query, expectedRanges, expectedQueryStrings);
    }

    // (A || B) && (C || D)
    @Test
    public void testIntersection_ofNestedUnions_distributedDays_unevenStopEnableUids() throws Exception {
        String query = "(A == 'all' || B == 'all_day') && (C == 'all' || D == 'uneven_stop_day')";

        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for(int jj = 0; jj < 10 ; jj++ ) {
                if (jj < 9 ) {
                    expectedRanges.add(makeShardedRange("2020010" + ii + "_" + jj));
                    expectedQueryStrings
                            .add("((A == 'all') || B == 'all_day') && ((C == 'all') || D == 'uneven_stop_day')");
                }
                else {
                    expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                    expectedQueryStrings
                            .add("((A == 'all') || B == 'all_day') && ((C == 'all'))"); // || D == 'uneven_stop_day'
                }

            }
        }

        runTest(query, expectedRanges, expectedQueryStrings,false);
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
    public void testIntersection_ofNestedUnions_distributedDays_missingShardsDisableBypass() throws Exception {
        String query = "(A == 'all' || B == 'all_day') && (C == 'all' || D == 'missing_shard_day')";

        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                expectedQueryStrings.add("((A == 'all') || B == 'all_day') && ((C == 'all'))");
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings,false);
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

    // (A || B) && (C || D)
    @Test
    public void testIntersection_ofNestedUnions_distributedDays_tickTockShardsDisableBypass() throws Exception {
        String query = "(A == 'all' || B == 'all_day') && (C == 'all' || D == 'tick_tock')";

        List<Range> expectedRanges = new ArrayList<>();
        List<String> expectedQueryStrings = new ArrayList<>();
        for (int ii = 1; ii <= 5; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                expectedRanges.add(makeTestRange("2020010" + ii + "_" + jj, "datatype1\0a.b.c"));
                if (ii != 3 || jj == 3 || jj == 7) {
                    expectedQueryStrings.add("(A == 'all' || B == 'all_day') && (C == 'all' || D == 'tick_tock')");
                } else {
                    expectedQueryStrings.add("(A == 'all' || B == 'all_day') && C == 'all'");
                }
            }
        }

        runTest(query, expectedRanges, expectedQueryStrings,false);
    }

    @Test
    public void testIntersectionOfNestedUnionsOnHasDelayedTermDisableBypass() throws Exception {
        String query = "(F1 == '1' || F2 == '2') && (F3 == '3' || ((_Delayed_ = true) && (F4 == '4')))";

        List<Range> expectedRanges = new ArrayList<>();
        expectedRanges.add(makeTestRange("20200101_10", "datatype1\0a.b.c"));
        expectedRanges.add(makeTestRange("20200101_11", "datatype1\0a.b.c"));
        expectedRanges.add(makeTestRange("20200101_12", "datatype1\0a.b.c"));
        expectedRanges.add(makeTestRange("20200101_13", "datatype1\0a.b.c"));

        List<String> expectedQueries = new ArrayList<>();
        expectedQueries.add("(F1 == '1' || F2 == '2') && (F3 == '3' || ((_Delayed_ = true) && (F4 == '4')))");
        expectedQueries.add("F1 == '1' && (F3 == '3' || ((_Delayed_ = true) && (F4 == '4')))"); // F2 skips shard_11
        expectedQueries.add("(F1 == '1' || F2 == '2') && ((_Delayed_ = true) && (F4 == '4'))"); // F3 skips shard _12
        expectedQueries.add("(F1 == '1' || F2 == '2') && (F3 == '3' || ((_Delayed_ = true) && (F4 == '4')))");

        runTest(query, expectedRanges, expectedQueries,false);
    }

    @Test
    public void


    testIntersectionOfNestedUnionsOnHasDelayedTerm_flipped() throws Exception {
        String query = "(F3 == '3' || ((_Delayed_ = true) && (F4 == '4'))) && (F1 == '1' || F2 == '2')";

        List<Range> expectedRanges = new ArrayList<>();
        expectedRanges.add(makeTestRange("20200101_10", "datatype1\0a.b.c"));
        expectedRanges.add(makeTestRange("20200101_11", "datatype1\0a.b.c"));
        expectedRanges.add(makeShardedRange("20200101_12"));
        expectedRanges.add(makeTestRange("20200101_13", "datatype1\0a.b.c"));

        List<String> expectedQueries = new ArrayList<>();
        expectedQueries.add("(F1 == '1' || F2 == '2') && (F3 == '3' || ((_Delayed_ = true) && (F4 == '4')))");
        expectedQueries.add("F1 == '1' && (F3 == '3' || ((_Delayed_ = true) && (F4 == '4')))"); // F2 skips shard_11
        expectedQueries.add("(F1 == '1' || F2 == '2') && ((_Delayed_ = true) && (F4 == '4'))"); // F3 skips shard _12
        expectedQueries.add("(F1 == '1' || F2 == '2') && (F3 == '3' || ((_Delayed_ = true) && (F4 == '4')))");

        runTest(query, expectedRanges, expectedQueries);
    }

    @Test
    public void


    testIntersectionOfNestedUnionsOnHasDelayedTerm_flippedDisableBypass() throws Exception {
        String query = "(F3 == '3' || ((_Delayed_ = true) && (F4 == '4'))) && (F1 == '1' || F2 == '2')";

        List<Range> expectedRanges = new ArrayList<>();
        expectedRanges.add(makeTestRange("20200101_10", "datatype1\0a.b.c"));
        expectedRanges.add(makeTestRange("20200101_11", "datatype1\0a.b.c"));
        expectedRanges.add(makeTestRange("20200101_12", "datatype1\0a.b.c"));
        expectedRanges.add(makeTestRange("20200101_13", "datatype1\0a.b.c"));

        List<String> expectedQueries = new ArrayList<>();
        expectedQueries.add("(F1 == '1' || F2 == '2') && (F3 == '3' || ((_Delayed_ = true) && (F4 == '4')))");
        expectedQueries.add("F1 == '1' && (F3 == '3' || ((_Delayed_ = true) && (F4 == '4')))"); // F2 skips shard_11
        expectedQueries.add("(F1 == '1' || F2 == '2') && ((_Delayed_ = true) && (F4 == '4'))"); // F3 skips shard _12
        expectedQueries.add("(F1 == '1' || F2 == '2') && (F3 == '3' || ((_Delayed_ = true) && (F4 == '4')))");

        runTest(query, expectedRanges, expectedQueries, false);
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
        String query = "F2 == '2' || (F1 == '1' && (F3 == '3' || ((_Delayed_ = true) && (F4 == '4'))))";

        List<Range> expectedRanges = new ArrayList<>();
        expectedRanges.add(makeTestRange("20200101_10", "datatype1\0a.b.c"));
        expectedRanges.add(makeTestRange("20200101_11", "datatype1\0a.b.c"));
        expectedRanges.add(makeShardedRange("20200101_12"));
        expectedRanges.add(makeTestRange("20200101_13", "datatype1\0a.b.c"));

        List<String> expectedQueries = new ArrayList<>();
        expectedQueries.add("F2 == '2' || (F1 == '1' && (F3 == '3' || ((_Delayed_ = true) && (F4 == '4'))))");
        expectedQueries.add("(F1 == '1' && (F3 == '3' || ((_Delayed_ = true) && (F4 == '4'))))"); // F2 skips shard _11
        expectedQueries.add("F2 == '2' || (F1 == '1' && ((_Delayed_ = true) && (F4 == '4')))"); // F3 skips shard _12
        expectedQueries.add("F2 == '2' || (F1 == '1' && (F3 == '3' || ((_Delayed_ = true) && (F4 == '4'))))");

        runTest(query, expectedRanges, expectedQueries);
    }
    @Test
    public void testOrAndOrWithDeeplyNestedDelayedTermDisableBypass() throws Exception {
        String query = "F2 == '2' || (F1 == '1' && (F3 == '3' || ((_Delayed_ = true) && (F4 == '4'))))";

        List<Range> expectedRanges = new ArrayList<>();
        expectedRanges.add(makeTestRange("20200101_10", "datatype1\0a.b.c"));
        expectedRanges.add(makeTestRange("20200101_11", "datatype1\0a.b.c"));
        expectedRanges.add(makeTestRange("20200101_12", "datatype1\0a.b.c"));
        expectedRanges.add(makeTestRange("20200101_13", "datatype1\0a.b.c"));

        List<String> expectedQueries = new ArrayList<>();
        expectedQueries.add("F2 == '2' || (F1 == '1' && (F3 == '3' || ((_Delayed_ = true) && (F4 == '4'))))");
        expectedQueries.add("(F1 == '1' && (F3 == '3' || ((_Delayed_ = true) && (F4 == '4'))))"); // F2 skips shard _11
        expectedQueries.add("F2 == '2' || (F1 == '1' && ((_Delayed_ = true) && (F4 == '4')))"); // F3 skips shard _12
        expectedQueries.add("F2 == '2' || (F1 == '1' && (F3 == '3' || ((_Delayed_ = true) && (F4 == '4'))))");

        runTest(query, expectedRanges, expectedQueries, false);
    }

    private void runTest(String query, List<Range> expectedRanges, List<String> expectedQueries) throws Exception {
        runTest(query,expectedRanges,expectedQueries,true);
    }

    private void runTest(String query, List<Range> expectedRanges, List<String> expectedQueries, boolean enableUidsOnDayRangesBypass) throws Exception {

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
        config.setShardsPerDayThreshold(2);

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(dataTypes.keySet());
        config.getServiceConfiguration().getIndexingConfiguration().setEnableIndexInfoUidToDayIntersectionBypass(enableUidsOnDayRangesBypass);

        // Run a standard limited-scanner range stream.
        RangeStream rangeStream = new RangeStream(config, new ScannerFactory(client, 1), helper);
        rangeStream.setLimitScanners(true);
        runTest(rangeStream, script, expectedRanges, expectedQueries);

        // Run a default range stream.
        rangeStream = new RangeStream(config, new ScannerFactory(client, 1), helper);
        rangeStream.setLimitScanners(false);

        rangeStream.close();
    }

    private int runTest(RangeStream rangeStream, ASTJexlScript script, List<Range> expectedRanges, List<String> expectedQueries) throws Exception {
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
            Range expectedRange = null;
            try {
                expectedRange = shardIter.next();
            }catch(NoSuchElementException e){
                throw e;
            }

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
        return counter;
    }
}
