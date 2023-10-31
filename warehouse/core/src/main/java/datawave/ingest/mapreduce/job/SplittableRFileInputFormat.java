package datawave.ingest.mapreduce.job;

import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.crypto.CryptoFactoryLoader;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.rfile.RFileOperations;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.accumulo.core.conf.Property.TABLE_CRYPTO_PREFIX;

public class SplittableRFileInputFormat extends RFileInputFormat {
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return true;
    }

    @Override
    public RecordReader<Key,Value> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new SplittableRFileRecordReader();
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        Configuration config = job.getConfiguration();

        List<InputSplit> rfileSplits = new ArrayList<>();


        // get the configured directories/files
        for (InputSplit inputSplit : super.getSplits(job)) {
            if (!(inputSplit instanceof FileSplit)) {
                throw new IllegalArgumentException("Must have file splits");
            }

            // for each file get the index blocks
            FileSplit fileSplit = (FileSplit) inputSplit;
            rfileSplits.addAll(getSplits(config, fileSplit));
        }

        return rfileSplits;
    }

    public static List<InputSplit> getSplits(Configuration config, FileSplit fileSplit) throws IOException {
        Path rfile = fileSplit.getPath();
        FileSystem fs = rfile.getFileSystem(config);

        if (!fs.exists(rfile)) {
            throw new IllegalArgumentException(rfile + " does not exist");
        }

        FileOperations ops = RFileOperations.getInstance();
        String file = rfile.toString();
        CryptoService cs = CryptoFactoryLoader.getServiceForClient(CryptoEnvironment.Scope.TABLE, config.getPropsWithPrefix(TABLE_CRYPTO_PREFIX.name()));
        FileSKVIterator iter = ops.newIndexReaderBuilder().forFile(file, fs, config, cs).withTableConfiguration(DefaultConfiguration.getInstance()).build();

        List<InputSplit> splits = new ArrayList<>();
        int blkCount = 0;
        while (iter.hasTop()) {
            splits.add(new RFileSplit(fileSplit, blkCount++, 1));
            iter.next();
        }

        // add the last split
        splits.add(new RFileSplit(fileSplit, blkCount, 1));

        return splits;
    }

    // get the splits for the rfile
    public static List<InputSplit> getSplits(Configuration config, Path rfile) throws IOException {
        return getSplits(config, new FileSplit(rfile, 0, 0, new String[0]));
    }

    public static FileSKVIterator getIterator(Configuration config, RFileSplit split) throws IOException, InterruptedException {
        return getKeys(config, split.getFileSplit().getPath(), split.getStartBlock(), split.getLength());
    }

    public static FileSKVIterator getKeys(Configuration config, Path rfile, int startIndexBlock, long numIndexBlocks) throws IOException {
        FileSystem fs = rfile.getFileSystem(config);

        if (!fs.exists(rfile)) {
            throw new IllegalArgumentException(rfile.toString() + " does not exist");
        }

        FileOperations ops = RFileOperations.getInstance();
        String file = rfile.toString();
        CryptoService cs = CryptoFactoryLoader.getServiceForClient(CryptoEnvironment.Scope.TABLE, config.getPropsWithPrefix(TABLE_CRYPTO_PREFIX.name()));
        FileSKVIterator iter = ops.newIndexReaderBuilder().forFile(file, fs, config, cs).withTableConfiguration(DefaultConfiguration.getInstance()).build();

        Key start = null;
        int blkCount = 1;
        if (startIndexBlock > 0) {
            while (iter.hasTop() && blkCount < startIndexBlock) {
                iter.next();
                blkCount++;
            }

            if (blkCount < startIndexBlock) {
                throw new EOFException("start index " + startIndexBlock + " beyond end of file");
            }

            start = iter.getTopKey();
            blkCount = 0;
        }

        // find the end key
        while (iter.hasTop() && blkCount < numIndexBlocks) {
            iter.next();
            blkCount++;
        }
        iter.close();

        if (blkCount < numIndexBlocks) {
            throw new EOFException("end index " + startIndexBlock + numIndexBlocks + " beyond end of file");
        }

        Key end = null;
        if (iter.hasTop()) {
            end = iter.getTopKey();
        }

        FileSKVIterator reader = ops.newReaderBuilder().forFile(file, fs, config, cs).withTableConfiguration(DefaultConfiguration.getInstance()).build();
        reader.seek(new Range(start, true, end, false), Collections.EMPTY_SET, false);

        return reader;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Logger.getRootLogger().setLevel(Level.WARN);
        FileSystem fs = FileSystem.getLocal(new Configuration());
        Path rfile = new Path("/tmp/test.rf");

        boolean buildFile = false;

        if (buildFile && fs.exists(rfile)) {
            fs.delete(rfile);
        }

        if (buildFile) {
            CryptoService cs = CryptoFactoryLoader.getServiceForClient(CryptoEnvironment.Scope.TABLE, new Configuration().getPropsWithPrefix(TABLE_CRYPTO_PREFIX.name()));
            FileSKVWriter writer = RFileOperations.getInstance().newWriterBuilder().forFile("/tmp/test.rf", FileSystem.getLocal(new Configuration()), new Configuration(), cs).withTableConfiguration(DefaultConfiguration.getInstance()).build();
            writer.startDefaultLocalityGroup();


            int count = 0;
            for (int i = 0; i < 10000000; i++) {
                // use hex formatting
                String key = String.format("%08x", i);
                for (int j = 0; j < 100; j++) {
                    writer.append(new Key(key), new Value("" + j));
                    count++;
                }
            }

            System.out.println("total keys: " + count);
            writer.close();
        }

        if (fs.exists(new Path("/tmp/keys"))) {
            fs.delete(new Path("/tmp/keys"));
        }

        FSDataOutputStream out = fs.create(new Path("/tmp/keys"));


        List<InputSplit> splits = SplittableRFileInputFormat.getSplits(new Configuration(), rfile);
        int writeBlocks = splits.size() + 1;
        out.write(("total splits: " + splits.size() + "\n").getBytes(StandardCharsets.UTF_8));
        int totalKeys = 0;
        for (int i = 0; i < splits.size(); i++) {
            RFileSplit split = (RFileSplit) splits.get(i);

            FileSKVIterator itr = SplittableRFileInputFormat.getKeys(new Configuration(), split.getFileSplit().getPath(), split.getStartBlock(), split.getLength());
            out.write((itr.getTopKey() + " " + itr.getTopValue() + "\n").getBytes(StandardCharsets.UTF_8));

            int count = 0;
            Key last = null;
            Value lastValue = null;
            while (itr.hasTop()) {
                count++;
                last = itr.getTopKey();
                lastValue = itr.getTopValue();
//                out.write((last + " " + lastValue + "\n").getBytes(StandardCharsets.UTF_8));
                itr.next();
            }
//            out.write((last + " " + lastValue + "\n").getBytes(StandardCharsets.UTF_8));
            out.write(("keys: " + count + "\n").getBytes(StandardCharsets.UTF_8));
            totalKeys += count;
            if (i + 1 == writeBlocks) {
                System.exit(0);
            }
        }

        out.write(("total keys: " + totalKeys).getBytes(StandardCharsets.UTF_8));

        out.close();
    }

    public static class RFileSplit extends InputSplit {
        private final FileSplit fileSplit;
        private final int startBlock;
        private final int numBlocks;

        public RFileSplit(FileSplit fileSplit, int startBlock, int numBlocks) {
            this.fileSplit = fileSplit;
            this.startBlock = startBlock;
            this.numBlocks = numBlocks;
        }

        public FileSplit getFileSplit() {
            return fileSplit;
        }

        public int getStartBlock() {
            return startBlock;
        }

        @Override
        public long getLength() throws IOException, InterruptedException {
            return numBlocks;
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            return fileSplit.getLocations();
        }
    }
}