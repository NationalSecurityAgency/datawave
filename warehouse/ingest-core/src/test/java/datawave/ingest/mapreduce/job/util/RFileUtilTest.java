package datawave.ingest.mapreduce.job.util;

import static org.apache.accumulo.core.conf.Property.TABLE_CRYPTO_PREFIX;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.crypto.CryptoFactoryLoader;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.rfile.RFileOperations;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RFileUtilTest {
    private Configuration config;
    private File tmpFile;

    @Before
    public void setup() {
        Logger.getRootLogger().setLevel(Level.WARN);
        config = new Configuration();
    }

    @After
    public void cleanup() {
        if (tmpFile != null && tmpFile.exists()) {
            tmpFile.delete();
        }
    }

    @Test
    public void getRangeSplitsTest_emptyRfile() throws IOException {
        List<Map.Entry<Key,Value>> data = new ArrayList<>();

        tmpFile = createRFile(data);
        List<Range> splits = RFileUtil.getRangeSplits(config, tmpFile.getPath(), null, null, -1);
        assertTrue(splits.size() == 0);
    }

    @Test
    public void getRangeSplitsTest_emptyRfileConstrained() throws IOException {
        List<Map.Entry<Key,Value>> data = new ArrayList<>();

        tmpFile = createRFile(data);
        List<Range> splits = RFileUtil.getRangeSplits(config, tmpFile.getPath(), new Key("a"), new Key("z"), -1);
        assertTrue(splits.size() == 0);
    }

    @Test
    public void getRangeSplitsTest_emptyRfileConstrained_countingBlocks() throws IOException {
        List<Map.Entry<Key,Value>> data = new ArrayList<>();

        tmpFile = createRFile(data);
        List<Range> splits = RFileUtil.getRangeSplits(config, tmpFile.getPath(), new Key("a"), new Key("z"), 1);
        assertTrue(splits.size() == 0);
    }

    @Test
    public void getRangeSplitsTest_singleRow_outsideFence() throws IOException {
        List<Map.Entry<Key,Value>> data = new ArrayList<>();
        addData(data, 1, 1);
        tmpFile = createRFile(data);
        List<Range> splits = RFileUtil.getRangeSplits(config, tmpFile.getPath(), new Key("a"), new Key("z"), 1);
        assertTrue(splits.size() == 0);
    }

    @Test
    public void getRangeSplitsTest_singleRow_insideFenceEdge() throws IOException {
        List<Map.Entry<Key,Value>> data = new ArrayList<>();
        addData(data, 1, 1);
        tmpFile = createRFile(data);
        List<Range> splits = RFileUtil.getRangeSplits(config, tmpFile.getPath(), createKey(1), createKey(9), 1);
        assertTrue(splits.size() == 1);
        Range r = new Range(createKey(1), true, createKey(1), true);
        assertTrue(r.equals(splits.get(0)));
    }

    @Test
    public void getRangeSplitsTest_singleRow_outsideFenceEdge() throws IOException {
        List<Map.Entry<Key,Value>> data = new ArrayList<>();
        addData(data, 1, 1);
        tmpFile = createRFile(data);
        List<Range> splits = RFileUtil.getRangeSplits(config, tmpFile.getPath(), createKey(2), createKey(9), 1);
        assertTrue(splits.size() == 0);
    }

    @Test
    public void getRangeSplitsTest_singleRow_insideFenceEdge2() throws IOException {
        List<Map.Entry<Key,Value>> data = new ArrayList<>();
        addData(data, 9, 1);
        tmpFile = createRFile(data);
        List<Range> splits = RFileUtil.getRangeSplits(config, tmpFile.getPath(), createKey(1), createKey(9), 1);
        assertTrue(splits.size() == 1);
        Range r = new Range(createKey(9), true, createKey(9), true);
        assertTrue(r.equals(splits.get(0)));
    }

    @Test
    public void getRangeSplitsTest_singleRow_outsideFenceEdge2() throws IOException {
        List<Map.Entry<Key,Value>> data = new ArrayList<>();
        addData(data, 9, 1);
        tmpFile = createRFile(data);
        List<Range> splits = RFileUtil.getRangeSplits(config, tmpFile.getPath(), createKey(2), createKey(8), 1);
        assertTrue(splits.size() == 0);
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
        Range r = new Range(first, true, last, true);

        // ensure no overlapping
        for (int i = 1; i < splits.size(); i++) {
            assertFalse(splits.get(i - 1).contains(splits.get(i).getStartKey()));
        }
        // adjacent should merge to 1
        List<Range> merged = Range.mergeOverlapping(splits);
        assertTrue(merged.size() == 1);

        // total merged should equal the initial fence
        assertTrue(merged.get(0).equals(r));
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
        Range r = new Range(first, true, last, true);

        assertTrue(splits.size() == 1);
        assertTrue(splits.get(0).equals(r));
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
        FileSystem fs = FileSystem.getLocal(new Configuration());
        File tmpFile = File.createTempFile("testSimpleSplits", ".rf");
        tmpFile.delete();

        CryptoService cs = CryptoFactoryLoader.getServiceForClient(CryptoEnvironment.Scope.TABLE,
                        new Configuration().getPropsWithPrefix(TABLE_CRYPTO_PREFIX.name()));

        FileSKVWriter writer = RFileOperations.getInstance().newWriterBuilder().forFile(tmpFile.getCanonicalPath(), fs, new Configuration(), cs)
                        .withTableConfiguration(DefaultConfiguration.getInstance()).build();
        writer.startDefaultLocalityGroup();

        // write data
        for (Map.Entry<Key,Value> toWrite : data) {
            writer.append(toWrite.getKey(), toWrite.getValue());
        }
        writer.close();

        return tmpFile;
    }
}
