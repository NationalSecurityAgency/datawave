package nsa.datawave.poller.manager.io;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

public abstract class TranslatingRecordReader<K1,V1,K2,V2> extends RecordReader<K2,V2> {
    
    protected TaskInputOutputContext<?,?,?,?> context;
    protected RecordReader<K1,V1> source;
    
    protected K2 destKey;
    protected V2 destValue;
    
    protected long skipCount = 0;
    
    public void setSource(RecordReader<K1,V1> rr) {
        this.source = rr;
    }
    
    @Override
    public void close() throws IOException {
        if (source != null) {
            source.close();
        }
    }
    
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return source.getProgress();
    }
    
    @Override
    public K2 getCurrentKey() throws IOException, InterruptedException {
        return destKey;
    }
    
    abstract public K2 createKey();
    
    @Override
    public V2 getCurrentValue() throws IOException, InterruptedException {
        return destValue;
    }
    
    abstract public V2 createValue();
    
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        if (context instanceof TaskInputOutputContext<?,?,?,?>) {
            this.context = (TaskInputOutputContext<?,?,?,?>) context;
        }
        source.initialize(split, context);
    }
    
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        boolean ans = source.nextKeyValue();
        // Skip over the specified number of records, if any
        for (long count = 0; ans && count < skipCount; ++count)
            ans = source.nextKeyValue();
        if (!ans)
            return false;
        if (destKey == null)
            destKey = createKey();
        if (destValue == null)
            destValue = createValue();
        translate(source.getCurrentKey(), source.getCurrentValue(), destKey, destValue);
        return true;
    }
    
    public void setup(JobContext context) throws IOException {
        // Default implementation does nothing
    }
    
    /**
     * Sets the number of records to skip over (without translating) after reading each input record from the source record reader.
     */
    public void setSkipCount(long skipCount) {
        this.skipCount = skipCount;
    }
    
    abstract public void translate(K1 sourceKey, V1 sourceValue, K2 key, V2 value) throws IOException;
}
