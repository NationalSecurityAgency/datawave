package datawave.ingest.mapreduce.job;

import static org.apache.accumulo.core.conf.Property.TABLE_CRYPTO_PREFIX;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.crypto.CryptoFactoryLoader;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

public class SplittableRFileInputFormat extends RFileInputFormat {
    private static final Logger log = Logger.getLogger(SplittableRFileInputFormat.class);
    public static final String MIN_BLOCKS_PER_SPLIT = SplittableRFileInputFormat.class.getName() + ".minBlocksPerSplit";

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return true;
    }

    @Override
    public RecordReader<Key,Value> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new SplittableRFileRecordReader();
    }

    /**
     * Create rfile splits for the configured job. See <code>FileInputFormat</code>
     *
     * @param job
     * @return
     * @throws IOException
     */
    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        Configuration config = job.getConfiguration();

        List<InputSplit> rfileSplits = new ArrayList<>();
        log.info("getting splits for job");
        // get the configured directories/files
        for (InputSplit inputSplit : super.getSplits(job)) {
            if (!(inputSplit instanceof FileSplit)) {
                throw new IllegalArgumentException("Must have file splits");
            }

            // for each file get the index blocks
            FileSplit fileSplit = (FileSplit) inputSplit;
            log.info("creating splits for: " + fileSplit);
            rfileSplits.addAll(getSplits(config, fileSplit));
        }
        log.info("total splits: " + rfileSplits.size());

        return rfileSplits;
    }

    /**
     * Create rfile splits for a given FileSplit. Splits will honor the MIN_BLOCKS_PER_SPLIT config option. Splits may extend more than MIN_BLOCKS_PER_SPLIT if
     * a given Key spans more index blocks.
     *
     * @param config
     * @param fileSplit
     * @return
     * @throws IOException
     */
    public static List<InputSplit> getSplits(Configuration config, FileSplit fileSplit) throws IOException {
        log.info("getting splits for: " + fileSplit);
        Path rfile = fileSplit.getPath();
        FileSystem fs = rfile.getFileSystem(config);

        if (!fs.exists(rfile)) {
            throw new IllegalArgumentException(rfile + " does not exist");
        }

        CryptoService cs = CryptoFactoryLoader.getServiceForClient(CryptoEnvironment.Scope.TABLE, config.getPropsWithPrefix(TABLE_CRYPTO_PREFIX.name()));
        CachableBlockFile.CachableBuilder cb = new CachableBlockFile.CachableBuilder().fsPath(fs, rfile).conf(config).cryptoService(cs);
        RFile.Reader rfileReader = new RFile.Reader(cb);

        // get the first and last keys to bound the blocks while creating splits
        Key firstKey = rfileReader.getFirstKey();
        Key lastKey = rfileReader.getLastKey();

        // use the index blocks to create the splits
        FileSKVIterator iter = rfileReader.getIndex();

        // track the last split key to since multiple splits with the same split key MUST be in the same block
        Key lastSplit = firstKey;
        List<InputSplit> splits = new ArrayList<>();
        int blkCount = 0;
        int splitBlocks = 0;

        int minBlocksPerSplit = config.getInt(MIN_BLOCKS_PER_SPLIT, 1);
        log.info("Blocks per split: " + minBlocksPerSplit);

        Key top = null;
        while (iter.hasTop()) {
            splitBlocks++;
            top = iter.getTopKey();

            if (!top.equals(lastSplit) && splitBlocks >= minBlocksPerSplit) {
                splits.add(new RFileSplit(fileSplit.getPath(), fileSplit.getStart(), fileSplit.getLength(), fileSplit.getLocations(), blkCount, splitBlocks,
                                top));
                blkCount += splitBlocks;
                lastSplit = iter.getTopKey();
                splitBlocks = 0;
            }
            iter.next();
        }

        // if there is a gap between the last index key top key and the lastKey in the file or there hasn't been a split created yet
        if (top.equals(lastKey) || splits.isEmpty()) {
            // add the last split
            splits.add(new RFileSplit(fileSplit.getPath(), fileSplit.getStart(), fileSplit.getLength(), fileSplit.getLocations(), blkCount, splitBlocks + 1,
                            top));
        }

        return splits;
    }

    // get the splits for the rfile
    public static List<InputSplit> getSplits(Configuration config, Path rfile) throws IOException {
        return getSplits(config, new FileSplit(rfile, 0, 0, new String[0]));
    }
}
