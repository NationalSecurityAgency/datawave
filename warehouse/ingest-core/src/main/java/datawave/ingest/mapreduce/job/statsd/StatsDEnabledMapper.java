package datawave.ingest.mapreduce.job.statsd;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Created on 4/25/16.
 */
public class StatsDEnabledMapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT> extends Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
    
    protected StatsDHelper helper = null;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        helper = createHelper();
        helper.setup(context);
    }
    
    protected StatsDHelper createHelper() {
        return new StatsDHelper();
    }
    
    public StatsDHelper getHelper() {
        return helper;
    }
    
    public TaskAttemptContext getContext(TaskAttemptContext context) {
        return helper.getContext(context);
    }
    
    /**
     * A helper method to get the appropriate counter dependent on whether we have a statsd client
     * 
     * @param context
     *            the context
     * @param counter
     *            the name of the counter
     * @param group
     *            the name of the group
     * @return a counter
     */
    public Counter getCounter(TaskAttemptContext context, String group, String counter) {
        return helper.getCounter(context, group, counter);
    }
    
    /**
     * A helper method to get the appropriate counter dependent on whether we have a statsd client
     * 
     * @param context
     *            the context
     * @param counterName
     *            the counter name
     * @return a counter
     */
    public Counter getCounter(TaskAttemptContext context, Enum<?> counterName) {
        return helper.getCounter(context, counterName);
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        helper.close();
        super.cleanup(context);
    }
}
