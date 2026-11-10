package datawave.util.accumulo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.spi.crypto.AESCryptoService;
import org.apache.accumulo.core.spi.crypto.GenericCryptoServiceFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RFileUtilTest {
    // 1K data blocks, so a few hundred keys span many blocks
    private static final Map<String,String> SMALL_BLOCKS = Map.of("table.file.compress.blocksize", "1K");

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

    @Test
    public void getRangeSplitsTest_fenceInsideOneBlock() throws IOException {
        // rows 1 to 3 fit in one block, so the block's index key, the last key of row 3, is past a fence on row 2
        List<Map.Entry<Key,Value>> data = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            addRow(data, i, 5);
        }
        tmpFile = createRFile(data);
        // the fence ShardReindexJob uses for a row
        Range fence = new Range(new Key(row(2)), true, new Key(row(2), "\uFFFF", "\uFFFF"), true);
        List<Range> splits = RFileUtil.getRangeSplits(config, tmpFile.getPath(), fence.getStartKey(), fence.getEndKey(), 1);
        assertEquals(List.of(fence), splits);
    }

    @Test
    public void getRangeSplitsTest_fenceBetweenRows() throws IOException {
        List<Map.Entry<Key,Value>> data = new ArrayList<>();
        addRow(data, 1, 5);
        addRow(data, 3, 5);
        tmpFile = createRFile(data);
        // the files have no keys in row 2, so there is no range for it
        List<Range> splits = RFileUtil.getRangeSplits(config, tmpFile.getPath(), new Key(row(2)), new Key(row(2), "\uFFFF", "\uFFFF"), 1);
        assertEquals(0, splits.size());
    }

    @Test
    public void getRangeSplitsTest_everyKeyInOneRange() throws IOException {
        // with small blocks a few hundred keys make many blocks. When the last block in the fence completes a split, the keys after that split, starting
        // with the split key itself, still need a range.
        int splitRuns = 0;
        for (int keys = 50; keys <= 500; keys += 50) {
            List<Map.Entry<Key,Value>> data = new ArrayList<>();
            for (int i = 0; i < keys; i++) {
                addData(data, i, i);
            }
            tmpFile = createRFile(data, SMALL_BLOCKS);
            for (int blocksPerSplit = 1; blocksPerSplit <= 3; blocksPerSplit++) {
                List<Range> splits = RFileUtil.getRangeSplits(config, tmpFile.getPath(), null, null, blocksPerSplit);
                assertEachKeyInOneRange(data, splits, keys + " keys, " + blocksPerSplit + " blocks per split");
                if (splits.size() > 1) {
                    splitRuns++;
                }
            }
            assertTrue(tmpFile.delete());
        }
        // the files were split
        assertTrue(splitRuns > 0);
    }

    @Test
    public void getRangeSplitsTest_multipleFiles() throws IOException {
        // two files with alternating rows, and an empty file
        List<Map.Entry<Key,Value>> even = new ArrayList<>();
        List<Map.Entry<Key,Value>> odd = new ArrayList<>();
        for (int i = 0; i < 400; i++) {
            addData(i % 2 == 0 ? even : odd, i, i);
        }
        List<File> files = List.of(createRFile(even, SMALL_BLOCKS), createRFile(odd, SMALL_BLOCKS), createRFile(new ArrayList<>()));
        try {
            List<String> paths = files.stream().map(File::getPath).toList();
            List<Range> splits = RFileUtil.getRangeSplits(config, paths, null, null, 1, Function.identity());
            assertTrue(splits.size() > 1);
            assertEachKeyInOneRange(even, splits, "even rows");
            assertEachKeyInOneRange(odd, splits, "odd rows");
            Range all = new Range(createKey(0), true, createKey(399).followingKey(PartialKey.ROW), false);
            assertEquals(List.of(all), Range.mergeOverlapping(splits));
        } finally {
            for (File file : files) {
                assertTrue(file.delete());
            }
        }
    }

    @Test
    public void getRangeSplitsTest_localityGroups() throws IOException {
        // the file's first row is in its second locality group
        tmpFile = File.createTempFile("testSimpleSplits", ".rf");
        assertTrue(tmpFile.delete());
        List<Map.Entry<Key,Value>> data = new ArrayList<>();
        try (RFileWriter writer = RFile.newWriter().to(tmpFile.getPath()).withFileSystem(FileSystem.getLocal(new Configuration())).build()) {
            writer.startNewLocalityGroup("lg", "a");
            for (int i = 5; i <= 9; i++) {
                Key key = new Key(row(i), "a", "cq");
                writer.append(key, createValue(i));
                data.add(new AbstractMap.SimpleEntry<>(key, createValue(i)));
            }
            writer.startDefaultLocalityGroup();
            for (int i = 1; i <= 3; i++) {
                Key key = new Key(row(i), "z", "cq");
                writer.append(key, createValue(i));
                data.add(new AbstractMap.SimpleEntry<>(key, createValue(i)));
            }
        }
        List<Range> splits = RFileUtil.getRangeSplits(config, tmpFile.getPath(), null, null, 1);
        assertEquals(List.of(new Range(createKey(1), true, createKey(9).followingKey(PartialKey.ROW), false)), Range.mergeOverlapping(splits));
        assertEachKeyInOneRange(data, splits, "locality groups");
    }

    @Test
    public void getRangeSplitsTest_encryptedFile() throws IOException {
        File kek = File.createTempFile("testSimpleSplits", ".kek");
        try {
            Files.write(kek.toPath(), new byte[32]);
            Map<String,String> crypto = Map.of(GenericCryptoServiceFactory.TABLE_SERVICE_NAME_PROP, AESCryptoService.class.getName(),
                            AESCryptoService.KEY_URI_PROPERTY, kek.toURI().toString());
            List<Map.Entry<Key,Value>> data = new ArrayList<>();
            addRow(data, 1, 5);
            tmpFile = createRFile(data, crypto);

            // the encrypted file can only be read with the crypto properties
            assertThrows(IOException.class, () -> RFileUtil.getRangeSplits(config, tmpFile.getPath(), null, null, 1));
            crypto.forEach(config::set);
            List<Range> splits = RFileUtil.getRangeSplits(config, tmpFile.getPath(), null, null, 1);
            assertEquals(List.of(new Range(createKey(1), true, createKey(1).followingKey(PartialKey.ROW), false)), splits);
        } finally {
            assertTrue(kek.delete());
        }
    }

    private static void assertEachKeyInOneRange(List<Map.Entry<Key,Value>> data, List<Range> ranges, String message) {
        for (Map.Entry<Key,Value> entry : data) {
            assertEquals(1, ranges.stream().filter(range -> range.contains(entry.getKey())).count(), message + ": " + entry.getKey());
        }
    }

    private static String row(int row) {
        return String.format("%08x", row);
    }

    private static void addRow(List<Map.Entry<Key,Value>> data, int row, int columns) {
        for (int i = 0; i < columns; i++) {
            data.add(new AbstractMap.SimpleEntry<>(new Key(row(row), "cf" + i, "cq"), createValue(i)));
        }
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
        return createRFile(data, Map.of());
    }

    public static File createRFile(List<Map.Entry<Key,Value>> data, Map<String,String> tableProperties) throws IOException {
        File tmpFile = File.createTempFile("testSimpleSplits", ".rf");
        assertTrue(tmpFile.delete());

        try (RFileWriter writer = RFile.newWriter().to(tmpFile.getPath()).withFileSystem(FileSystem.getLocal(new Configuration())).withTableProperties(tableProperties)
                        .build()) {
            for (Map.Entry<Key,Value> toWrite : data) {
                writer.append(toWrite.getKey(), toWrite.getValue());
            }
        }

        return tmpFile;
    }
}
