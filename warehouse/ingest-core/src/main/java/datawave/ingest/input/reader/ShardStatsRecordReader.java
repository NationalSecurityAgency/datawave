package datawave.ingest.input.reader;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Empty class that is used to process shard index stats bulk ingest files. The class is only referenced in order to allow the bulk input operation to succeed
 * and to enable the shardStats to be recognized as a type.
 * 
 * @param <K>
 *            type of the key for the recordreader
 * @param <V>
 *            type of the value for the recordreader
 */
public class ShardStatsRecordReader<K,V> extends RecordReader<K,V> {
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        
    }
    
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return false;
    }
    
    @Override
    public K getCurrentKey() throws IOException, InterruptedException {
        return null;
    }
    
    @Override
    public V getCurrentValue() throws IOException, InterruptedException {
        return null;
    }
    
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }
    
    @Override
    public void close() throws IOException {
        
    }
}
