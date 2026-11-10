package datawave.util.accumulo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RFileUtilTest {
    private Configuration config;
    private File tmpFile;

    @BeforeEach
    public void setup() {
        Logger.getRootLogger().setLevel(Level.WARN);
        config = new Configuration();
    }

    @AfterEach
    public void cleanup() {
        if (tmpFile != null && tmpFile.exists()) {
            assertTrue(tmpFile.delete());
        }
    }

    @Test
    public void getRangeSplitsTest_emptyRfile() throws IOException {
        List<Map.Entry<Key,Value>> data = new ArrayList<>();

        tmpFile = createRFile(data);
        List<Range> splits = RFileUtil.getRangeSplits(config, tmpFile.getPath(), null, null, -1);
        assertEquals(0, splits.size());
    }

    @Test
    public void getRangeSplitsTest_emptyRfileConstrained() throws IOException {
        List<Map.Entry<Key,Value>> data = new ArrayList<>();

        tmpFile = createRFile(data);
        List<Range> splits = RFileUtil.getRangeSplits(config, tmpFile.getPath(), new Key("a"), new Key("z"), -1);
        assertEquals(0, splits.size());
    }

    @Test
    public void getRangeSplitsTest_emptyRfileConstrained_countingBlocks() throws IOException {
        List<Map.Entry<Key,Value>> data = new ArrayList<>();

        tmpFile = createRFile(data);
        List<Range> splits = RFileUtil.getRangeSplits(config, tmpFile.getPath(), new Key("a"), new Key("z"), 1);
        assertEquals(0, splits.size());
    }

    @Test
    public void getRangeSplitsTest_singleRow_outsideFence() throws IOException {
        List<Map.Entry<Key,Value>> data = new ArrayList<>();
        addData(data, 1, 1);
        tmpFile = createRFile(data);
        List<Range> splits = RFileUtil.getRangeSplits(config, tmpFile.getPath(), new Key("a"), new Key("z"), 1);
        assertEquals(0, splits.size());
    }

    @Test
    public void getRangeSplitsTest_singleRow_insideFenceEdge() throws IOException {
        List<Map.Entry<Key,Value>> data = new ArrayList<>();
        addData(data, 1, 1);
        tmpFile = createRFile(data);
        List<Range> splits = RFileUtil.getRangeSplits(config, tmpFile.getPath(), createKey(1), createKey(9), 1);
        assertEquals(1, splits.size());
        Range r = new Range(createKey(1), true, createKey(1).followingKey(PartialKey.ROW), false);
        assertEquals(r, splits.get(0));
    }

    @Test
    public void getRangeSplitsTest_singleRow_outsideFenceEdge() throws IOException {
        List<Map.Entry<Key,Value>> data = new ArrayList<>();
        addData(data, 1, 1);
        tmpFile = createRFile(data);
        List<Range> splits = RFileUtil.getRangeSplits(config, tmpFile.getPath(), createKey(2), createKey(9), 1);
        assertEquals(0, splits.size());
    }

    @Test
    public void getRangeSplitsTest_singleRow_insideFenceEdge2() throws IOException {
        List<Map.Entry<Key,Value>> data = new ArrayList<>();
        addData(data, 9, 1);
        tmpFile = createRFile(data);
        List<Range> splits = RFileUtil.getRangeSplits(config, tmpFile.getPath(), createKey(1), createKey(9), 1);
        assertEquals(1, splits.size());
        Range r = new Range(createKey(9), true, createKey(9), true);
        assertEquals(r, splits.get(0));
    }

    @Test
    public void getRangeSplitsTest_singleRow_outsideFenceEdge2() throws IOException {
        List<Map.Entry<Key,Value>> data = new ArrayList<>();
        addData(data, 9, 1);
        tmpFile = createRFile(data);
        List<Range> splits = RFileUtil.getRangeSplits(config, tmpFile.getPath(), createKey(2), createKey(8), 1);
        assertEquals(0, splits.size());
    }

    @Test
    public void getRangeSplitsTest_10000Keys() throws IOException {
        List<Map.Entry<Key,Value>> data = new ArrayList<>();
        Key first = null;
        Key last = null;
        for (int i = 0; i < 1000000; i++) {
            Key added = addData(data, i, i);
            if (first == null) {
                first = added;
            }
            last = added;
        }
        tmpFile = createRFile(data);
        List<Range> splits = RFileUtil.getRangeSplits(config, tmpFile.getPath(), null, null, 1);
        Range r = new Range(first, true, last.followingKey(PartialKey.ROW), false);

        // ensure no overlapping
        for (int i = 1; i < splits.size(); i++) {
            assertFalse(splits.get(i - 1).contains(splits.get(i).getStartKey()));
        }
        // adjacent should merge to 1
        List<Range> merged = Range.mergeOverlapping(splits);
        assertEquals(1, merged.size());

        // total merged should equal the initial fence
        assertEquals(r, merged.get(0));
    }

    @Test
    public void getRangeSplitsTest_10000Keys_moreIndexBlocksPerSplitThanExist() throws IOException {
        List<Map.Entry<Key,Value>> data = new ArrayList<>();
        Key first = null;
        Key last = null;
        for (int i = 0; i < 1000000; i++) {
            Key added = addData(data, i, i);
            if (first == null) {
                first = added;
            }
            last = added;
        }
        tmpFile = createRFile(data);
        List<Range> splits = RFileUtil.getRangeSplits(config, tmpFile.getPath(), null, null, 999999999);
        Range r = new Range(first, true, last.followingKey(PartialKey.ROW), false);
        assertEquals(1, splits.size());
        assertEquals(r, splits.get(0));
    }

    public static Key createKey(int key) {
        return new Key(String.format("%08x", key));
    }

    public static Value createValue(int value) {
        return new Value(String.format("%08d", value));
    }

    public static Key addData(List<Map.Entry<Key,Value>> data, int key, int value) {
        Key created = createKey(key);
        data.add(new AbstractMap.SimpleEntry<>(created, createValue(value)));
        return created;
    }

    public static File createRFile(List<Map.Entry<Key,Value>> data) throws IOException {
        File tmpFile = File.createTempFile("testSimpleSplits", ".rf");
        assertTrue(tmpFile.delete());

        try (RFileWriter writer = RFile.newWriter().to(tmpFile.getPath()).withFileSystem(FileSystem.getLocal(new Configuration())).build()) {
            for (Map.Entry<Key,Value> toWrite : data) {
                writer.append(toWrite.getKey(), toWrite.getValue());
            }
        }

        return tmpFile;
    }
}
