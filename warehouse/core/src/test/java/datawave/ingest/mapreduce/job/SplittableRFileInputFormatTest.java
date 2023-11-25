package datawave.ingest.mapreduce.job;

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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.accumulo.core.conf.Property.TABLE_CRYPTO_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SplittableRFileInputFormatTest {
    private Configuration config;

    @Before
    public void setup() {
        Logger.getRootLogger().setLevel(Level.WARN);
        config = new Configuration();
    }

    @After
    public void cleanup() {

    }

    @Test
    public void testSingleKeySplit() throws IOException, InterruptedException {
        List<Map.Entry<Key,Value>> data = new ArrayList<>();
        Key original = new Key(String.format("%08x", 1));
        data.add(new AbstractMap.SimpleEntry<>(original, new Value()));

        File tmpFile = createRFile(data);
        try {
            // get the splits from the file
            List<InputSplit> splits = SplittableRFileInputFormat.getSplits(config, new Path(tmpFile.toString()));

            // 1 split
            assertEquals(1, splits.size());
            SplittableRFileInputFormat.RFileSplit rfileSplit = (SplittableRFileInputFormat.RFileSplit) splits.get(0);
            verifySplit(rfileSplit, 0, 2, original, original, 1);
        } finally {
            if (tmpFile.exists()) {
                tmpFile.delete();
            }
        }
    }

    @Test
    public void testSingleMultiKeySplit() throws IOException, InterruptedException {
        List<Map.Entry<Key,Value>> data = new ArrayList<>();
        Key start = new Key(String.format("%08x", 0));
        Key end = new Key(String.format("%08x", 1));
        for (int i = 0; i < 2; i++) {
            Key key = new Key(String.format("%08x", i));
            data.add(new AbstractMap.SimpleEntry<>(key, new Value(String.format("%08x", i))));
        }

        File tmpFile = createRFile(data);
        try {
            // get the splits from the file
            List<InputSplit> splits = SplittableRFileInputFormat.getSplits(config, new Path(tmpFile.toString()));

            // 2 split
            assertEquals(2, splits.size());
            SplittableRFileInputFormat.RFileSplit rfileSplit = (SplittableRFileInputFormat.RFileSplit) splits.get(0);
            verifySplit(rfileSplit, 0, 1, start, start, 1);
            rfileSplit = (SplittableRFileInputFormat.RFileSplit) splits.get(1);
            verifySplit(rfileSplit, 1, 1, end, end, 1);
        } finally {
            if (tmpFile.exists()) {
                tmpFile.delete();
            }
        }
    }

    @Test
    public void testHugeSplits() throws IOException, InterruptedException {
        List<Map.Entry<Key,Value>> data = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            data.add(new AbstractMap.SimpleEntry<>(new Key(String.format("%08x", 1)), new Value(String.format("%08d", i))));
        }
        for (int i = 0; i < 1000; i++) {
            data.add(new AbstractMap.SimpleEntry<>(new Key(String.format("%08x", 2)), new Value(String.format("%08d", i))));
        }
        for (int i = 0; i < 1000; i++) {
            data.add(new AbstractMap.SimpleEntry<>(new Key(String.format("%08x", 3)), new Value(String.format("%08d", i))));
        }
        File tmpFile = createRFile(data);
        try {
            // get the splits from the file
            List<InputSplit> splits = SplittableRFileInputFormat.getSplits(config, new Path(tmpFile.toString()));
            assertEquals(2, splits.size());
            verifySplit((SplittableRFileInputFormat.RFileSplit) splits.get(0), 0, 2, new Key(String.format("%08x", 1)), new Key(String.format("%08x", 2)), 11000);
            verifySplit((SplittableRFileInputFormat.RFileSplit) splits.get(1), 2, 1, new Key(String.format("%08x", 3)), new Key(String.format("%08x", 3)), 1000);
        } finally {
            if (tmpFile.exists()) {
                tmpFile.delete();
            }
        }
    }

    @Test
    public void testMegaSplit() throws IOException, InterruptedException {
        List<Map.Entry<Key,Value>> data = new ArrayList<>();
        for (int i = 0; i < 1000000; i++) {
            data.add(new AbstractMap.SimpleEntry<>(new Key(String.format("%08x", 1)), new Value(String.format("%08d", i))));
        }
        for (int i = 0; i < 1000; i++) {
            data.add(new AbstractMap.SimpleEntry<>(new Key(String.format("%08x", 2)), new Value(String.format("%08d", i))));
        }
        for (int i = 0; i < 1000; i++) {
            data.add(new AbstractMap.SimpleEntry<>(new Key(String.format("%08x", 3)), new Value(String.format("%08d", i))));
        }
        File tmpFile = createRFile(data);
        try {
            // get the splits from the file
            List<InputSplit> splits = SplittableRFileInputFormat.getSplits(config, new Path(tmpFile.toString()));
            assertEquals(3, splits.size());
            verifySplit((SplittableRFileInputFormat.RFileSplit) splits.get(0), 0, 127, new Key(String.format("%08x", 1)), new Key(String.format("%08x", 1)), 1000000);
            verifySplit((SplittableRFileInputFormat.RFileSplit) splits.get(1), 127, 1, new Key(String.format("%08x", 2)), new Key(String.format("%08x", 2)), 1000);
            verifySplit((SplittableRFileInputFormat.RFileSplit) splits.get(2), 128, 1, new Key(String.format("%08x", 3)), new Key(String.format("%08x", 3)), 1000);
        } finally {
            if (tmpFile.exists()) {
                tmpFile.delete();
            }
        }
    }

    private void verifySplit(SplittableRFileInputFormat.RFileSplit split, int startBlk, int blkCount, Key first, Key last, int keyCount) throws IOException, InterruptedException {
        // beginning of the file
        assertEquals("unexpected start block", startBlk, split.getStartBlock());
        // 2 blocks (1 real block + 1 to the end of the file)
        assertEquals("unexpected split length", blkCount, split.getLength());

        // verify the key is the same key written in
        FileSKVIterator i = SplittableRFileInputFormat.getIterator(config, split);
        int count = 0;
        boolean verifiedFirst = false;
        Key top = null;
        while(count < keyCount) {
            assertTrue(i.hasTop());
            top = i.getTopKey();
            if (!verifiedFirst) {
                assertEquals("unexpected first key", first, top);
                verifiedFirst = true;
            }
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

        CryptoService cs = CryptoFactoryLoader.getServiceForClient(CryptoEnvironment.Scope.TABLE, new Configuration().getPropsWithPrefix(TABLE_CRYPTO_PREFIX.name()));
        FileSKVWriter writer = RFileOperations.getInstance().newWriterBuilder().forFile(tmpFile.getCanonicalPath(), fs, new Configuration(), cs).withTableConfiguration(DefaultConfiguration.getInstance()).build();
        writer.startDefaultLocalityGroup();

        // write data
        for (Map.Entry<Key,Value> toWrite : data) {
            writer.append(toWrite.getKey(), toWrite.getValue());
        }
        writer.close();

        return tmpFile;
    }
}
