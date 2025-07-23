package datawave.ingest.mapreduce.job.validation;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;
import java.util.List;

public class FixedMapperInputFormat extends InputFormat<LongWritable, Text> {
    public static class DummyRecordReader extends RecordReader<LongWritable, Text> {
        private boolean read = false;
        private long key = 0;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) { }

        @Override
        public boolean nextKeyValue() {
            if (read) return false;
            read = true;
            return true;
        }

        @Override
        public LongWritable getCurrentKey() {
            return new LongWritable(key++);
        }

        @Override
        public Text getCurrentValue() {
            return new Text("dummy");
        }

        @Override
        public float getProgress() { return read ? 1.0f : 0.0f; }

        @Override
        public void close() {}
    }

    public static class DummyInputSplit extends InputSplit implements Writable {
        @Override
        public long getLength() { return 1; }

        @Override
        public String[] getLocations() { return new String[0]; }

        @Override
        public void write(DataOutput out) {}

        @Override
        public void readFields(DataInput in) {}
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) {
        List<InputSplit> splits = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            splits.add(new DummyInputSplit());
        }
        return splits;
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new DummyRecordReader();
    }
}