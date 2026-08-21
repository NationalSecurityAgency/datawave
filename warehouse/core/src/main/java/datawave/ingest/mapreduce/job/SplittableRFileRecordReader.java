package datawave.ingest.mapreduce.job;

import java.io.EOFException;
import java.io.IOException;
import java.util.Collections;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import datawave.util.accumulo.RFileUtil;

public class SplittableRFileRecordReader extends RFileRecordReader {

    /**
     * Initializes the fileIterator from the split. If split is an RFileSplit the record reader will create an iterator that only covers the content for the
     * split. If instead the split is a FileSplit it will be handled by {@link RFileRecordReader}.
     *
     * @param split
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        if (split instanceof RFileSplit) {
            fileIterator = getIterator(context.getConfiguration(), (RFileSplit) split);
        } else {
            super.initialize(split, context);
        }
    }

    /**
     * Open an rfile specified by the split and create an iterator over the region of the rfile the split covers. Splits created by
     * {@link SplittableRFileInputFormat} carry their range, so the index is only walked for a split created without one.
     *
     * @param config
     * @param split
     * @return
     * @throws IOException
     */
    public static FileSKVIterator getIterator(Configuration config, RFileSplit split) throws IOException {
        Path rfile = split.getPath();

        RFile.Reader rfileReader = RFileUtil.getRFileReader(config, rfile);
        Range seekRange = split.hasSeekRange() ? split.getSeekRange() : findSeekRange(rfileReader, split);

        rfileReader.seek(seekRange, Collections.emptySet(), false);

        return rfileReader;
    }

    /**
     * Locate the range covered by the split by reading the rfile index blocks.
     *
     * @param rfileReader
     * @param split
     * @return
     * @throws IOException
     */
    private static Range findSeekRange(RFile.Reader rfileReader, RFileSplit split) throws IOException {
        long startIndexBlock = split.getStartBlock();
        long numIndexBlocks = split.getNumBlocks();

        FileSKVIterator iter = rfileReader.getIndex();

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

        if (blkCount < numIndexBlocks) {
            throw new EOFException("end index " + startIndexBlock + numIndexBlocks + " beyond end of file");
        }

        Key end = null;
        if (iter.hasTop()) {
            end = iter.getTopKey();
        }

        return split.getSeekRange(start, end);
    }
}
