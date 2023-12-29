package datawave.ingest.mapreduce.job;

import static org.apache.accumulo.core.conf.Property.TABLE_CRYPTO_PREFIX;
import static org.junit.Assert.assertEquals;
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
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.rfile.RFileOperations;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SplittableRFileInputFormatTest {
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
    public void testSingleKeySplit() throws IOException, InterruptedException {
        List<Map.Entry<Key,Value>> data = new ArrayList<>();
        Key original = createKey(1);
        data.add(new AbstractMap.SimpleEntry<>(original, createValue(0)));

        // get the splits from the file
        tmpFile = createRFile(data);
        List<InputSplit> splits = getSplits(config, new Path(tmpFile.toString()));

        // 1 split
        assertEquals(1, splits.size());
        RFileSplit rfileSplit = (RFileSplit) splits.get(0);
        verifySplit(rfileSplit, 0, 2, original, original, 1);
    }

    @Test
    public void testSingleMultiKeySplit() throws IOException, InterruptedException {
        List<Map.Entry<Key,Value>> data = new ArrayList<>();
        Key start = createKey(0);
        Key end = createKey(1);
        for (int i = 0; i < 2; i++) {
            addData(data, i, 0);
        }

        tmpFile = createRFile(data);
        // get the splits from the file
        List<InputSplit> splits = getSplits(config, new Path(tmpFile.toString()));

        // 2 split
        assertEquals(2, splits.size());
        RFileSplit rfileSplit = (RFileSplit) splits.get(0);
        verifySplit(rfileSplit, 0, 1, start, start, 1);
        rfileSplit = (RFileSplit) splits.get(1);
        verifySplit(rfileSplit, 1, 1, end, end, 1);
    }

    @Test
    public void testBlockNotMatchEndSplit() throws IOException, InterruptedException {
        List<Map.Entry<Key,Value>> data = new ArrayList<>();
        Key start = createKey(1);
        Key end = createKey(2);
        for (int i = 0; i < 100000; i++) {
            addData(data, 1, i);
        }

        addData(data, 2, 0);

        tmpFile = createRFile(data);
        // get the splits from the file
        List<InputSplit> splits = getSplits(config, new Path(tmpFile.toString()));

        // 2 split
        assertEquals(2, splits.size());
        RFileSplit rfileSplit = (RFileSplit) splits.get(0);
        verifySplit(rfileSplit, 0, 13, start, start, 100000);
        rfileSplit = (RFileSplit) splits.get(1);
        verifySplit(rfileSplit, 13, 1, end, end, 1);
    }

    @Test
    public void testHugeSplits() throws IOException, InterruptedException {
        List<Map.Entry<Key,Value>> data = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            addData(data, 1, i);
        }
        for (int i = 0; i < 1000; i++) {
            // this data will be merged with the previous split, so start its value at 10000
            addData(data, 2, 10000 + i);
        }
        for (int i = 0; i < 1000; i++) {
            addData(data, 3, i);
        }
        tmpFile = createRFile(data);
        // get the splits from the file
        List<InputSplit> splits = getSplits(config, new Path(tmpFile.toString()));
        assertEquals(2, splits.size());
        verifySplit((RFileSplit) splits.get(0), 0, 2, createKey(1), createKey(2), 11000);
        verifySplit((RFileSplit) splits.get(1), 2, 1, createKey(3), createKey(3), 1000);
    }

    @Test
    public void testMegaSplit() throws IOException, InterruptedException {
        List<Map.Entry<Key,Value>> data = new ArrayList<>();
        for (int i = 0; i < 1000000; i++) {
            addData(data, 1, i);
        }
        for (int i = 0; i < 1000; i++) {
            addData(data, 2, i);
        }
        for (int i = 0; i < 1000; i++) {
            addData(data, 3, i);
        }
        tmpFile = createRFile(data);
        // get the splits from the file
        List<InputSplit> splits = getSplits(config, new Path(tmpFile.toString()));
        assertEquals(3, splits.size());
        verifySplit((RFileSplit) splits.get(0), 0, 127, createKey(1), createKey(1), 1000000);
        verifySplit((RFileSplit) splits.get(1), 127, 1, createKey(2), createKey(2), 1000);
        verifySplit((RFileSplit) splits.get(2), 128, 1, createKey(3), createKey(3), 1000);
    }

    private Key createKey(int key) {
        return new Key(String.format("%08x", key));
    }

    private Value createValue(int value) {
        return new Value(String.format("%08d", value));
    }

    private void addData(List<Map.Entry<Key,Value>> data, int key, int value) {
        data.add(new AbstractMap.SimpleEntry<>(createKey(key), createValue(value)));
    }

    /**
     * Verify a split matches expected data. Each value should be 0-index incremented in the split
     *
     * @param split
     *            the split to evaluate
     * @param startBlk
     *            the start block of the split
     * @param blkCount
     *            the number of blocks in the split
     * @param first
     *            the first key of the split
     * @param last
     *            the last key of the split
     * @param keyCount
     *            the number of keys in the split
     * @throws IOException
     * @throws InterruptedException
     */
    private void verifySplit(RFileSplit split, int startBlk, int blkCount, Key first, Key last, int keyCount) throws IOException {
        // beginning of the file
        assertEquals("unexpected start block", startBlk, split.getStartBlock());
        // 2 blocks (1 real block + 1 to the end of the file)
        assertEquals("unexpected num blocks", blkCount, split.getNumBlocks());

        // verify the key is the same key written in
        FileSKVIterator i = SplittableRFileRecordReader.getIterator(config, split);
        int count = 0;
        boolean verifiedFirst = false;
        Key top = null;
        while (count < keyCount) {
            assertTrue(i.hasTop());
            top = i.getTopKey();
            if (!verifiedFirst) {
                assertEquals("unexpected first key", first, top);
                verifiedFirst = true;
            }
            Value v = i.getTopValue();
            assertEquals(v, createValue(count));
            i.next();
            count++;
        }
        assertEquals("unexpected last key", last, top);
        assertFalse("unexpected key count", i.hasTop());
    }

    private File createRFile(List<Map.Entry<Key,Value>> data) throws IOException {
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

    // create a simple FileSplit to create an RFileSplit from
    private List<InputSplit> getSplits(Configuration config, Path rfile) throws IOException {
        return SplittableRFileInputFormat.getSplits(config, new FileSplit(rfile, 0, 0, new String[0]));
    }
}
