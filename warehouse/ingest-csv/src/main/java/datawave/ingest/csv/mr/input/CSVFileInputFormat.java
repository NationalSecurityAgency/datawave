package datawave.ingest.csv.mr.input;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import datawave.ingest.data.RawRecordContainer;

/**
 * Specialization of SequenceFileInputFormat that expects an {@link RawRecordContainer} object as the value.
 */
public class CSVFileInputFormat extends SequenceFileInputFormat<LongWritable,RawRecordContainer> {

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public RecordReader<LongWritable,RawRecordContainer> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new RecordReader<LongWritable,RawRecordContainer>() {

            private CSVRecordReader delegate = null;

            @Override
            public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
                delegate = new CSVRecordReader();
                delegate.initialize(split, context);
            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                return delegate.nextKeyValue();
            }

            @Override
            public LongWritable getCurrentKey() throws IOException, InterruptedException {
                return delegate.getCurrentKey();
            }

            @Override
            public RawRecordContainer getCurrentValue() throws IOException, InterruptedException {
                return delegate.getEvent();
            }

            @Override
            public float getProgress() throws IOException, InterruptedException {
                return delegate.getProgress();
            }

            @Override
            public void close() throws IOException {
                delegate.close();
                delegate = null;
            }

        };
    }

}
