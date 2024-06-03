package datawave.ingest.mapreduce.job.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class SplittableRFileRangeInputFormat extends InputFormat {
    public static void setSplitFiles(Job j, String files) {
        j.getConfiguration().set(SplittableRFileRangeInputFormat.class.getName() + ".splitFiles", files);
    }

    public static void setStartKey(Job j, String startKey) {
        j.getConfiguration().set(SplittableRFileRangeInputFormat.class.getName() + ".startKey", startKey);
    }

    public static void setEndKey(Job j, String endKey) {
        j.getConfiguration().set(SplittableRFileRangeInputFormat.class.getName() + ".end", endKey);
    }

    public static void setIndexBlocksPerSplit(Job j, int indexBlocksPerSplit) {
        j.getConfiguration().setInt(SplittableRFileRangeInputFormat.class.getName() + ".indexBlocksPerSplit", indexBlocksPerSplit);
    }

    private static String getProperty(Configuration config, String property) {
        return config.get(SplittableRFileRangeInputFormat.class.getName() + "." + property);
    }

    public SplittableRFileRangeInputFormat() {
        // no-op
    }

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        Configuration config = jobContext.getConfiguration();

        String splitFiles = getProperty(config, "splitFiles");

        String startKey = getProperty(config, "startKey");
        String endKey = getProperty(config, "endKey");

        int indexBlocksPerSplit = Integer.parseInt(getProperty(config, "indexBlocksPerSplit"));

        List<Range> ranges = RFileUtil.getRangeSplits(config, splitFiles, new Key(startKey), new Key(endKey), indexBlocksPerSplit);
        List<InputSplit> inputSplits = new ArrayList<>();
        for (Range r : ranges) {
            RangeInputSplit ris = new RangeInputSplit(r);
            inputSplits.add(ris);
        }

        if (inputSplits.isEmpty()) {
            throw new IllegalStateException("No ranges to process");
        }

        return inputSplits;
    }

    @Override
    public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new RecordReader<Range,String>() {
            private Range nextRange = null;
            private boolean done = false;

            @Override
            public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
                if (inputSplit instanceof RangeInputSplit) {
                    RangeInputSplit ris = (RangeInputSplit) inputSplit;
                    nextRange = ris.range;
                }
            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                if (!done) {
                    done = true;
                    return true;
                }

                return false;
            }

            @Override
            public Range getCurrentKey() throws IOException, InterruptedException {
                return nextRange;
            }

            @Override
            public String getCurrentValue() throws IOException, InterruptedException {
                return null;
            }

            @Override
            public float getProgress() throws IOException, InterruptedException {
                return !done ? 0 : 1;
            }

            @Override
            public void close() throws IOException {
                // no-op
            }
        };
    }

    private static class RangeInputSplit extends InputSplit implements Writable {
        private Range range = new Range();

        public RangeInputSplit() {
            // no-op
        }

        public RangeInputSplit(Range r) {
            this.range = r;
        }

        @Override
        public long getLength() throws IOException, InterruptedException {
            return 1;
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            return new String[0];
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            range.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            range.readFields(dataInput);
        }

        @Override
        public String toString() {
            return range.toString();
        }
    }
}
