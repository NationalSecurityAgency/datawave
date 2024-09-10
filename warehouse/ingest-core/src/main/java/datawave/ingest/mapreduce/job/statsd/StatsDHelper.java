package datawave.ingest.mapreduce.job.statsd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Created on 4/25/16.
 */
public class StatsDHelper {
    protected CounterStatsDClient statsd = null;

    public void setup(TaskAttemptContext context) {
        setup(context.getConfiguration());
    }

    public void setup(Configuration conf) {
        statsd = new CounterToStatsDConfiguration(conf).getClient();
    }

    public CounterStatsDClient getClient() {
        return statsd;
    }

    public TaskAttemptContext getContext(TaskAttemptContext context) {
        if (statsd == null) {
            return context;
        } else {
            return statsd.getTaskAttemptContext(context);
        }
    }

    /**
     * A helper method to get the appropriate counter dependent on whether we have a statsd client
     *
     * @param context
     *            the context
     * @param group
     *            the group
     * @param counter
     *            string representation of the counter
     * @return a counter
     */
    public Counter getCounter(TaskAttemptContext context, String group, String counter) {
        return getContext(context).getCounter(group, counter);
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
        return getContext(context).getCounter(counterName);
    }

    protected void close() {
        if (statsd != null) {
            statsd.close();
            statsd = null;
        }
    }
}
