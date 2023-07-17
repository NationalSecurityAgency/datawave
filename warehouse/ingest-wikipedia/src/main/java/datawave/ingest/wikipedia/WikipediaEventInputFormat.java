package datawave.ingest.wikipedia;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.DataTypeHelperImpl;
import datawave.ingest.input.reader.AbstractEventRecordReader;

public class WikipediaEventInputFormat extends SequenceFileInputFormat<LongWritable,RawRecordContainer> {
    static final String NUM_INPUT_FILES = "mapreduce.input.num.files";

    private static final double SPLIT_SLOP = 1.1; // 10% slop

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        final Configuration conf = job.getConfiguration();

        long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
        long maxSize = getMaxSplitSize(job);

        // generate splits
        List<InputSplit> splits = new ArrayList<>();
        List<FileStatus> files = listStatus(job);
        for (FileStatus file : files) {
            Path path = file.getPath();
            FileSystem fs = path.getFileSystem(conf);
            long length = file.getLen();
            BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
            if ((length != 0) && isSplitable(job, path)) {
                long blockSize = file.getBlockSize();
                long splitSize = computeSplitSize(blockSize, minSize, maxSize);

                long bytesRemaining = length;
                while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
                    int blkIndex = getBlockIndex(blkLocations, length - bytesRemaining);
                    splits.add(new FileSplit(path, length - bytesRemaining, splitSize, blkLocations[blkIndex].getHosts()));
                    bytesRemaining -= splitSize;
                }

                if (bytesRemaining != 0) {
                    splits.add(new FileSplit(path, length - bytesRemaining, bytesRemaining, blkLocations[blkLocations.length - 1].getHosts()));
                }
            } else if (length != 0) {
                splits.add(new FileSplit(path, 0, length, blkLocations[0].getHosts()));
            } else {
                // Create empty hosts array for zero length files
                splits.add(new FileSplit(path, 0, length, new String[0]));
            }
        }

        // Save the number of input files in the job-conf
        conf.setLong(NUM_INPUT_FILES, files.size());

        return splits;
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        // Can't actually call this on the delegate...
        CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        return codec == null;
    }

    @Override
    public RecordReader<LongWritable,RawRecordContainer> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new RecordReader<LongWritable,RawRecordContainer>() {
            private AbstractEventRecordReader<RawRecordContainer> rrDelegate = null;

            {
                /*
                 * Reader will typically be datawave.ingest.wikipedia.WikipediaRecordReader, but the ingest api allows for other implementations so we'll defer
                 * to ingest config to tell us the concrete class
                 */
                DataTypeHelperImpl d = new DataTypeHelperImpl();
                d.setup(context.getConfiguration());
                rrDelegate = (AbstractEventRecordReader<RawRecordContainer>) d.getType().newRecordReader();
                if (rrDelegate == null) {
                    throw new IllegalArgumentException(d.getType().typeName() + " not handled in WikipediaEventInputFormat");
                }
            }

            @Override
            public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
                rrDelegate.initialize(split, context);
            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                return rrDelegate.nextKeyValue();
            }

            @Override
            public LongWritable getCurrentKey() throws IOException, InterruptedException {
                return rrDelegate.getCurrentKey();
            }

            @Override
            public RawRecordContainer getCurrentValue() throws IOException, InterruptedException {
                return rrDelegate.getEvent();
            }

            @Override
            public float getProgress() throws IOException, InterruptedException {
                return rrDelegate.getProgress();
            }

            @Override
            public void close() throws IOException {
                rrDelegate.close();
            }
        };
    }
}
