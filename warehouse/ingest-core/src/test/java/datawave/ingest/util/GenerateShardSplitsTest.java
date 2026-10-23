package datawave.ingest.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.admin.TabletMergeability;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
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
            Assert.assertTrue(expected.contains(element));
        }

    }

    @Test
    void parseMergeability() {
        assertEquals(TabletMergeability.never(), GenerateShardSplits.parseMergeability("never"));
        assertEquals(TabletMergeability.always(), GenerateShardSplits.parseMergeability("ALWAYS"));
        assertEquals(TabletMergeability.after(Duration.ofDays(30)), GenerateShardSplits.parseMergeability("P30D"));
        assertEquals(TabletMergeability.after(Duration.ofHours(12)), GenerateShardSplits.parseMergeability("PT12H"));
        assertEquals(TabletMergeability.after(Duration.ZERO), GenerateShardSplits.parseMergeability("PT0S"));
    }

    @Test
    void parseMergeabilityRejectsBadValues() {
        assertThrows(IllegalArgumentException.class, () -> GenerateShardSplits.parseMergeability("30d"));
        assertThrows(IllegalArgumentException.class, () -> GenerateShardSplits.parseMergeability("sometimes"));
        assertThrows(IllegalArgumentException.class, () -> GenerateShardSplits.parseMergeability("-P1D"));
    }
}
