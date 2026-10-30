package datawave.ingest.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

class GenerateShardSplitsTest {

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

    @Test
    void splitsPerBatchTakesTheNextArgument() throws Exception {
        List<String> plain = run("20260101", "1", "4", "2");
        assertEquals(List.of("20260101_0", "20260101_2"), plain);

        // the value must be consumed, not treated as the start of <username> <password> <tableName>
        assertEquals(plain, run("20260101", "1", "4", "2", "-splitsPerBatch", "5"));
    }

    @Test
    void pctBatchBalanceRequiredAcceptsAFraction() throws Exception {
        assertEquals(List.of("20260101_0", "20260101_2"), run("20260101", "1", "4", "2", "-pctBatchBalanceRequired", ".75"));
    }

    @Test
    void optionsAfterValuedOptionsAreStillParsed() throws Exception {
        List<String> output = run("20260101", "1", "4", "2", "-splitsPerBatch", "5", "-pctBatchBalanceRequired", "0.5", "-addShardMarkers");
        assertEquals("20260101_0", output.get(0));
        assertEquals("20260101_2", output.get(1));
        assertEquals(4, output.size(), "expected a shard marker for each split: " + output);
        assertTrue(output.get(2).startsWith("20260101_0 :"), output.get(2));
        assertTrue(output.get(3).startsWith("20260101_2 :"), output.get(3));
    }

    @Test
    void markersOnlyAfterValuedOptionSkipsSplits() throws Exception {
        List<String> output = run("20260101", "1", "4", "2", "-splitsPerBatch", "5", "-markersOnly", "-addShardMarkers");
        assertEquals(2, output.size(), "expected only shard markers: " + output);
        assertTrue(output.get(0).startsWith("20260101_0 :"), output.get(0));
    }

    /**
     * Runs GenerateShardSplits without Accumulo connection args, so it prints the splits and markers instead of adding them.
     */
    private static List<String> run(String... args) throws Exception {
        PrintStream original = System.out;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        try (PrintStream out = new PrintStream(captured, true, StandardCharsets.UTF_8.name())) {
            System.setOut(out);
            GenerateShardSplits.main(args);
        } finally {
            System.setOut(original);
        }
        return Arrays.stream(captured.toString(StandardCharsets.UTF_8.name()).split("\\R")).filter(line -> !line.isEmpty()).collect(Collectors.toList());
    }
}
