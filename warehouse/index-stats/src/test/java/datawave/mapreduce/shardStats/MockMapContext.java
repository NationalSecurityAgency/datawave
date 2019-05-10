package datawave.mapreduce.shardStats;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.junit.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class MockMapContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> extends MapContextImpl<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
    
    final List<MRPair<KEYOUT,VALUEOUT>> output = new ArrayList<>();
    Iterator<MRPair<KEYIN,VALUEIN>> mapInput;
    MRPair<KEYIN,VALUEIN> current;
    
    MockMapContext(Configuration conf, TaskAttemptID id) {
        super(conf, id, null, null, null, null, new MockInputSplit());
    }
    
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (this.mapInput.hasNext()) {
            this.current = this.mapInput.next();
            return true;
        }
        return false;
    }
    
    @Override
    public KEYIN getCurrentKey() throws IOException, InterruptedException {
        return this.current.key;
    }
    
    @Override
    public VALUEIN getCurrentValue() throws IOException, InterruptedException {
        return this.current.value;
    }
    
    @Override
    public void write(Object key, Object value) throws IOException, InterruptedException {
        // this is a hack, but is good enough for testing
        // we know it will be a Value object, would have to change if this is used for multiple mappers
        // because the Value object is reused by StatsHyperLogMapper, a new instance is required
        Value v = new Value((Value) value);
        MRPair kvPair = new MRPair((KEYOUT) key, v);
        this.output.add(kvPair);
    }
    
    @Override
    public void progress() {}
    
    static class MockInputSplit extends InputSplit {
        @Override
        public long getLength() throws IOException, InterruptedException {
            return 0;
        }
        
        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            return new String[0];
        }
    }
}
