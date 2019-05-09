package datawave.mapreduce.shardStats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.ReduceContextImpl;
import org.apache.hadoop.util.Progress;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Mock for reducer testing.
 * 
 * @param <KEYIN>
 * @param <VALUEIN>
 * @param <KEYOUT>
 * @param <VALUEOUT>
 */
class MockReduceContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> extends ReduceContextImpl<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
    
    final List<MRPair<KEYOUT,VALUEOUT>> output = new ArrayList<>();
    Iterator<MRPair<KEYIN,Iterable<VALUEIN>>> reduceInput;
    private MRPair<KEYIN,Iterable<VALUEIN>> current;
    
    MockReduceContext(Configuration conf, TaskAttemptID id, Class<KEYIN> keyClass, Class<VALUEIN> valueClass) throws IOException, InterruptedException {
        super(conf, id, new MockKVIterator(), null, null, null, null, null, null, keyClass, valueClass);
    }
    
    @Override
    public boolean nextKeyValue() {
        if (this.reduceInput.hasNext()) {
            this.current = this.reduceInput.next();
            return true;
        }
        return false;
    }
    
    @Override
    public KEYIN getCurrentKey() {
        return this.current.key;
    }
    
    @Override
    public Iterable<VALUEIN> getValues() throws IOException, InterruptedException {
        return this.current.value;
    }
    
    @Override
    public void write(Object key, Object value) throws IOException, InterruptedException {
        MRPair<KEYOUT,VALUEOUT> kvPair = new MRPair(key, value);
        this.output.add(kvPair);
    }
    
    @Override
    public void progress() {}
    
    /**
     * Mock for input iterator because it generates a call to next during the
     */
    static class MockKVIterator implements RawKeyValueIterator {
        @Override
        public DataInputBuffer getKey() throws IOException {
            return null;
        }
        
        @Override
        public DataInputBuffer getValue() throws IOException {
            return null;
        }
        
        @Override
        public boolean next() throws IOException {
            return true;
        }
        
        @Override
        public void close() throws IOException {
            
        }
        
        @Override
        public Progress getProgress() {
            return null;
        }
    }
}
