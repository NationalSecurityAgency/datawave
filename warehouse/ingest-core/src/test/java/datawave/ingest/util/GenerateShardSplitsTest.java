package datawave.ingest.util;

import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class GenerateShardSplitsTest {

    @Test
    void sortSplitsByMidpoints() {
        List<Text> files = List.of(
                new Text("20230101_1"), new Text("20230101_2"), new Text("20230101_3"),
                new Text("20230102_1"), new Text("20230102_2"), new Text("20230102_3"),
                new Text("20230103_1"), new Text("20230103_2"), new Text("20230103_3"),
                new Text("20230103_10")
        );

        List<Text> expected = List.of(new Text("20230102_2"), new Text("20230102_3"), new Text("20230101_2"),
                new Text("20230101_3"),new Text("20230101_1"),new Text("20230102_1"),
        new Text("20230103_2"),new Text("20230103_3"),new Text("20230103_1"), new Text("20230103_10"));

        List<Text> sorted = GenerateShardSplits.sortSplitsByMidpoints(files);

        assertEquals(expected, sorted, "Sort order was incorrect");

        for(Text element : sorted)
        {
            Assert.assertTrue(expected.contains(element));
        }


    }
}