package datawave.ingest.input.reader.raw;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 *
 */
public class FakeReader extends RecordReader<LongWritable,MapWritable> {
    
    private final LongWritable currentKey = new LongWritable();
    private MapWritable currentValue = new MapWritable();
    
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return true;
    }
    
    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return currentKey;
    }
    
    @Override
    public MapWritable getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
    }
    
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }
    
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub
        
    }
}
