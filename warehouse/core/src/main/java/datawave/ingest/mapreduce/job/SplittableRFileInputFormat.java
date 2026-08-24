package datawave.ingest.mapreduce.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import datawave.util.accumulo.RFileUtil;

public class SplittableRFileInputFormat extends RFileInputFormat {
    private static final Logger log = Logger.getLogger(SplittableRFileInputFormat.class);
    public static final String MIN_BLOCKS_PER_SPLIT = SplittableRFileInputFormat.class.getName() + ".minBlocksPerSplit";
    public static final String NUM_THREADS = SplittableRFileInputFormat.class.getName() + ".numThreads";

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        // prevent FileInputFormat from splitting the raw files, potentially creating duplicates with InputSplit.getStart() != 0 when super.getSplits() is
        // called
        return false;
    }

    @Override
    public RecordReader<Key,Value> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new SplittableRFileRecordReader();
    }

    /**
     * Create rfile splits for the configured job. See <code>FileInputFormat</code>. Set NUM_THREADS to read more than one input file at a time.
     *
     * @param job
     * @return
     * @throws IOException
     */
    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        Configuration config = job.getConfiguration();

        log.info("getting splits for job");
        int minBlocksPerSplit = config.getInt(MIN_BLOCKS_PER_SPLIT, 1);
        log.info("Blocks per split: " + minBlocksPerSplit);

        // get the configured directories/files
        List<FileSplit> fileSplits = new ArrayList<>();
        for (InputSplit inputSplit : super.getSplits(job)) {
            if (!(inputSplit instanceof FileSplit)) {
                throw new IllegalArgumentException("Must have file splits");
            }

            fileSplits.add((FileSplit) inputSplit);
        }

        int numThreads = config.getInt(NUM_THREADS, 1);
        List<InputSplit> rfileSplits;
        if (numThreads > 1) {
            rfileSplits = getSplits(config, fileSplits, numThreads);
        } else {
            rfileSplits = new ArrayList<>();
            for (FileSplit fileSplit : fileSplits) {
                // for each file get the index blocks
                rfileSplits.addAll(getSplits(config, fileSplit));
            }
        }
        log.info("total splits: " + rfileSplits.size());

        return rfileSplits;
    }

    /**
     * Create rfile splits for each FileSplit, reading numThreads files at a time. The splits come back in the order they would be created serially.
     *
     * @param config
     * @param fileSplits
     * @param numThreads
     * @return
     * @throws IOException
     */
    private static List<InputSplit> getSplits(Configuration config, List<FileSplit> fileSplits, int numThreads) throws IOException {
        ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);
        try {
            List<Future<List<InputSplit>>> pending = new ArrayList<>(fileSplits.size());
            for (FileSplit fileSplit : fileSplits) {
                // for each file get the index blocks
                Callable<List<InputSplit>> task = () -> getSplits(config, fileSplit);
                pending.add(threadPool.submit(task));
            }

            List<InputSplit> rfileSplits = new ArrayList<>();
            for (Future<List<InputSplit>> future : pending) {
                rfileSplits.addAll(future.get());
            }

            return rfileSplits;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while creating rfile splits", e);
        } catch (ExecutionException e) {
            throw new IOException("failed to create rfile splits", e.getCause());
        } finally {
            threadPool.shutdownNow();
        }
    }

    /**
     * Create rfile splits for a given FileSplit. Splits will honor the MIN_BLOCKS_PER_SPLIT config option. Splits may extend more than MIN_BLOCKS_PER_SPLIT if
     * a given Key spans more index blocks. Each split carries the range it covers so that reading it requires no further index traversal.
     *
     * @param config
     * @param fileSplit
     * @return
     * @throws IOException
     */
    public static List<InputSplit> getSplits(Configuration config, FileSplit fileSplit) throws IOException {
        log.info("getting splits for: " + fileSplit);
        List<InputSplit> splits = new ArrayList<>();

        try (RFile.Reader rfileReader = RFileUtil.getRFileReader(config, fileSplit.getPath())) {
            // get the first key to bound the blocks while creating splits
            Key firstKey = rfileReader.getFirstKey();

            // use the index blocks to create the splits
            FileSKVIterator iter = rfileReader.getIndex();

            // track the last split key to since multiple splits with the same split key MUST be in the same block
            Key lastSplit = firstKey;
            long blkCount = 0;
            long splitBlocks = 0;

            int minBlocksPerSplit = config.getInt(MIN_BLOCKS_PER_SPLIT, 1);

            // a split runs from the index entry that closed the previous split up to, but not including, the one that closes it
            Key splitStart = null;
            Key top = null;
            while (iter.hasTop()) {
                splitBlocks++;
                top = iter.getTopKey();

                if (!top.equals(lastSplit) && splitBlocks >= minBlocksPerSplit) {
                    splits.add(new RFileSplit(fileSplit.getPath(), fileSplit.getStart(), fileSplit.getLength(), fileSplit.getLocations(), blkCount, splitBlocks,
                                    top, splitStart, top));
                    blkCount += splitBlocks;
                    lastSplit = top;
                    splitStart = top;
                    splitBlocks = 0;
                }
                iter.next();
            }

            // add the last split, which runs to the end of the file
            splits.add(new RFileSplit(fileSplit.getPath(), fileSplit.getStart(), fileSplit.getLength(), fileSplit.getLocations(), blkCount, splitBlocks + 1,
                            top, splitStart, null));
        }

        return splits;
    }
}
