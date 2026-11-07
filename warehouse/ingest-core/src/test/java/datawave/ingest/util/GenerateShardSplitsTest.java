package datawave.ingest.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import datawave.util.cli.PasswordConverter;
import datawave.util.time.DateHelper;

class GenerateShardSplitsTest {

    @Nested
    class OptionParsingTests {

        @Test
        void minArgsRequired() {
            assertUsageWouldBePrinted();
            assertUsageWouldBePrinted("1");
            assertUsageWouldBePrinted("1", "2");
        }

        @Test
        void testParsingInvalidStartDate() {
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> parseOptions("2022-10-15", "", ""));
            assertTrue(exception.getMessage().startsWith("Start Date does not match format"));
        }

        @Test
        void testParsingInvalidDaysToGenerate() {
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> parseOptions("20221015", "2.0", ""));
            assertTrue(exception.getMessage().startsWith("Days to Generate argument is not an integer: "));
        }

        @Test
        void testParsingInvalidNumShardsPerDay() {
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> parseOptions("20221015", "2", "2.0"));
            assertTrue(exception.getMessage().startsWith("Num Shards Per Day argument is not an integer: "));
        }

        @Test
        void testParsingValidMinimumArgs() {
            GenerateShardSplits.Options opts = parseOptions("20221015", "2", "10");
            assertEquals(DateHelper.parse("20221015"), opts.startDate);
            assertEquals(2, opts.daysToGenerate);
            assertEquals(10, opts.numShardsPerDay);

            // Verify defaults:
            assertEquals(1, opts.numShardsPerSplit);
            assertEquals(5_000, opts.balancerDelay);
            assertEquals(90_000, opts.maxBalancerDelay);
            assertEquals(0.5, opts.pctBatchBalanceRequired);
            assertTrue(opts.addSplits);
            assertFalse(opts.addShardMarkers);
            assertEquals(100_000, opts.splitsPerBatch);
            assertNull(opts.shardMarkerTypes);
            assertNull(opts.username);
            assertNull(opts.password);
            assertNull(opts.tableName);
            assertNull(opts.instanceName);
            assertNull(opts.zookeepers);
        }

        @Test
        void testParsingInvalidNumShardsSplit() {
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> parseOptions("20221015", "5", "100", "10.0"));
            assertTrue(exception.getMessage().startsWith("Num Shards Per Split argument is not an integer: "));

            // Test a valid argument.
            GenerateShardSplits.Options opts = parseOptions("20221015", "2", "100", "1000", "-splitsPerBatch", "25");
            assertEquals(25, opts.splitsPerBatch);
        }

        @Test
        void testParsingSplitsPerBatch() {
            // Test no argument.
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                            () -> parseOptions("20221015", "5", "100", "1000", "-splitsPerBatch"));
            assertEquals("-splitsPerBatch must be followed by a number of splits to create per batch", exception.getMessage());

            // Test a non-integer argument.
            exception = assertThrows(IllegalArgumentException.class, () -> parseOptions("20221015", "5", "100", "1000", "-splitsPerBatch", "5.0"));
            assertTrue(exception.getMessage().startsWith("-splitsPerBatch argument is not an integer: "));

            // Test a value less than 10.
            exception = assertThrows(IllegalArgumentException.class, () -> parseOptions("20221015", "5", "100", "1000", "-splitsPerBatch", "9"));
            assertTrue(exception.getMessage().startsWith("-splitsPerBatch cannot be less than 10"));

            // Test a valid argument.
            GenerateShardSplits.Options opts = parseOptions("20221015", "2", "100", "1000", "-splitsPerBatch", "25");
            assertEquals(25, opts.splitsPerBatch);
        }

        @Test
        void testParsingBalancerDelay() {
            // Test no argument.
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                            () -> parseOptions("20221015", "5", "100", "1000", "-balancerDelay"));
            assertEquals("-balancerDelay must be followed by a number of milliseconds to wait for balance between batches", exception.getMessage());

            // Test a non-integer argument.
            exception = assertThrows(IllegalArgumentException.class, () -> parseOptions("20221015", "5", "100", "1000", "-balancerDelay", "5.0"));
            assertTrue(exception.getMessage().startsWith("-balancerDelay argument is not an integer: "));

            // Test a valid argument.
            GenerateShardSplits.Options opts = parseOptions("20221015", "2", "100", "1000", "-balancerDelay", "2500");
            assertEquals(2_500, opts.balancerDelay);
        }

        @Test
        void testParsingMaxBalancerDelay() {
            // Test no argument.
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                            () -> parseOptions("20221015", "5", "100", "1000", "-maxBalancerDelay"));
            assertEquals("-maxBalancerDelay must be followed a maximum number of milliseconds to wait for balance", exception.getMessage());

            // Test a non-integer argument.
            exception = assertThrows(IllegalArgumentException.class, () -> parseOptions("20221015", "5", "100", "1000", "-maxBalancerDelay", "5.0"));
            assertTrue(exception.getMessage().startsWith("-maxBalancerDelay argument is not an integer: "));

            // Test a valid argument.
            GenerateShardSplits.Options opts = parseOptions("20221015", "2", "100", "1000", "-maxBalancerDelay", "2500");
            assertEquals(2_500, opts.maxBalancerDelay);
        }

        @Test
        void testParsingPctBatchBalanceRequired() {
            // Test no argument.
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                            () -> parseOptions("20221015", "5", "100", "1000", "-pctBatchBalanceRequired"));
            assertEquals("-pctBatchBalanceRequired must be followed a double of the percentage of batch size that must be on a different server to continue",
                            exception.getMessage());

            // Test a non-double argument.
            exception = assertThrows(IllegalArgumentException.class, () -> parseOptions("20221015", "5", "100", "1000", "-pctBatchBalanceRequired", "5.0.14"));
            assertTrue(exception.getMessage().startsWith("-pctBatchBalanceRequired argument is not a double: "));

            // Test a number less than 0.
            exception = assertThrows(IllegalArgumentException.class, () -> parseOptions("20221015", "5", "100", "1000", "-pctBatchBalanceRequired", "-0.0001"));
            assertEquals("-pctBatchBalanceRequired must be between 0 and 1", exception.getMessage());

            // Test a number greater than 1.
            exception = assertThrows(IllegalArgumentException.class, () -> parseOptions("20221015", "5", "100", "1000", "-pctBatchBalanceRequired", "1.00001"));
            assertEquals("-pctBatchBalanceRequired must be between 0 and 1", exception.getMessage());

            // Test a valid argument.
            GenerateShardSplits.Options opts = parseOptions("20221015", "2", "100", "1000", "-pctBatchBalanceRequired", "0.5");
            assertEquals(0.5, opts.pctBatchBalanceRequired);
        }

        @Test
        void testParsingMarkersOnly() {
            // Test the default.
            GenerateShardSplits.Options opts = parseOptions("20221015", "2", "100", "1000");
            assertTrue(opts.addSplits);

            // Test the flag.
            opts = parseOptions("20221015", "2", "100", "1000", "-markersOnly");
            assertFalse(opts.addSplits);
        }

        @Test
        void testParsingAddShardMarkers() {
            // Test the default.
            GenerateShardSplits.Options opts = parseOptions("20221015", "2", "100", "1000");
            assertFalse(opts.addShardMarkers);

            // Test the flag.
            opts = parseOptions("20221015", "2", "100", "1000", "-addShardMarkers");
            assertTrue(opts.addShardMarkers);
        }

        @Test
        void testParsingAddDataTypeMarkers() {
            // Test no argument.
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                            () -> parseOptions("20221015", "5", "100", "1000", "-addDataTypeMarkers"));
            assertTrue(exception.getMessage().startsWith("-addDataTypeMarkers must be followed a comma-delimited list of data types"));

            // Test a single data type.
            GenerateShardSplits.Options opts = parseOptions("20221015", "2", "100", "1000", "-addDataTypeMarkers", "data1");
            assertArrayEquals(new String[] {"data1"}, opts.shardMarkerTypes);

            // Test multiple data type.
            opts = parseOptions("20221015", "2", "100", "1000", "-addDataTypeMarkers", "data1,data2,data3");
            assertArrayEquals(new String[] {"data1", "data2", "data3"}, opts.shardMarkerTypes);
        }

        @Test
        void testParsingAccumuloConnectionInfo() {
            // Verify that if a username is given, the password and table name must be too.
            assertUsageWouldBePrinted("20221015", "2", "100", "1000", "username");
            assertUsageWouldBePrinted("20221015", "2", "100", "1000", "username", "password");

            // Test a valid number of arguments for an Accumulo connection.
            GenerateShardSplits.Options opts = parseOptions("20221015", "2", "100", "1000", "username", "password", "table");
            assertEquals("username", opts.username);
            assertArrayEquals(PasswordConverter.parseArg("password").getBytes(), opts.password);
            assertEquals("table", opts.tableName);

            // Verify that if an instance name is given, the zookeepers must be too.
            assertUsageWouldBePrinted("20221015", "2", "100", "1000", "username", "password", "table", "instance-01");

            // Test when the zookeepers are given.
            opts = parseOptions("20221015", "2", "100", "1000", "username", "password", "table", "instance-01", "localhost:2181");
            assertEquals("instance-01", opts.instanceName);
            assertEquals("localhost:2181", opts.zookeepers);
        }

        private void assertUsageWouldBePrinted(String... args) {
            GenerateShardSplits.Options options = parseOptions(args);
            assertTrue(options.printUsageAndExit);
        }

        private GenerateShardSplits.Options parseOptions(String... args) {
            return GenerateShardSplits.parseOptions(args);
        }
    }

    @Test
    void sortSplitsByMidpoints() {
        List<Text> files = List.of(new Text("20230101_1"), new Text("20230101_2"), new Text("20230101_3"), new Text("20230102_1"), new Text("20230102_2"),
                        new Text("20230102_3"), new Text("20230103_1"), new Text("20230103_2"), new Text("20230103_3"), new Text("20230103_10"));

        List<Text> expected = List.of(new Text("20230102_2"), new Text("20230102_3"), new Text("20230101_2"), new Text("20230101_3"), new Text("20230101_1"),
                        new Text("20230102_1"), new Text("20230103_2"), new Text("20230103_3"), new Text("20230103_1"), new Text("20230103_10"));
        List<Text> results = new ArrayList<>(files.size());
        GenerateShardSplits.calculateMidpoints(files, results);

        assertEquals(expected, results, "Sort order was incorrect");

        for (Text element : files) {
            assertTrue(expected.contains(element));
        }
    }

}
