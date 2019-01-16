package datawave.ingest.nyctlc;

import datawave.ingest.data.RawRecordContainer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class NYCTLCInputFormat extends FileInputFormat<LongWritable,RawRecordContainer> {
    
    @Override
    public RecordReader<LongWritable,RawRecordContainer> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new RecordReader<LongWritable,RawRecordContainer>() {
            private NYCTLCReader delegate;
            
            @Override
            public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
                delegate = new NYCTLCReader();
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
