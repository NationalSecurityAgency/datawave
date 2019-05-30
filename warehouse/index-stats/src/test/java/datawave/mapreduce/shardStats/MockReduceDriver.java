package datawave.mapreduce.shardStats;

import datawave.ingest.mapreduce.job.BulkIngestKey;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Mock driver for Reducer testing.
 * 
 * @param <KEYIN>
 * @param <VALUEIN>
 * @param <KEYOUT>
 * @param <VALUEOUT>
 */
class MockReduceDriver<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
    
    private final Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> reducer;
    private final List<MRPair<KEYIN,Iterable<VALUEIN>>> input = new ArrayList<>();
    private final Configuration conf = new Configuration();
    
    MockReduceDriver(Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> reducer) {
        this.reducer = reducer;
    }
    
    Configuration getConfiguration() {
        return conf;
    }
    
    void addInput(KEYIN key, Iterable<VALUEIN> value) {
        MRPair pair = new MRPair(key, value);
        this.input.add(pair);
    }
    
    List<MRPair<KEYOUT,VALUEOUT>> run() throws IOException, InterruptedException {
        TaskAttemptID id = new TaskAttemptID("testJob", 0, TaskType.REDUCE, 0, 0);
        final MockReduceContext context = new MockReduceContext(this.conf, id, BulkIngestKey.class, Value.class);
        context.reduceInput = this.input.iterator();
        
        Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>.Context con = new WrappedReducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>().getReducerContext(context);
        this.reducer.run(con);
        return context.output;
    }
}
