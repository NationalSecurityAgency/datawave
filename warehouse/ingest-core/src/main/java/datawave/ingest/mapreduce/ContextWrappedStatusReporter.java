package datawave.ingest.mapreduce;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 */
public class ContextWrappedStatusReporter extends StatusReporter {

    @SuppressWarnings("rawtypes")
    private TaskAttemptContext context = null;

    @SuppressWarnings({"rawtypes", "hiding"})
    public ContextWrappedStatusReporter(TaskAttemptContext context) {
        this.context = context;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.hadoop.mapreduce.StatusReporter#getCounter(java.lang.Enum)
     */
    @SuppressWarnings("unchecked")
    @Override
    public Counter getCounter(Enum<?> name) {
        return context.getCounter(name);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.hadoop.mapreduce.StatusReporter#getCounter(java.lang.String, java.lang.String)
     */
    @Override
    public Counter getCounter(String group, String name) {
        try {
            return context.getCounter(group, name);
        } catch (NullPointerException npe) {
            return null;
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.hadoop.mapreduce.StatusReporter#progress()
     */
    @Override
    public void progress() {
        context.progress();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.hadoop.mapreduce.StatusReporter#setStatus(java.lang.String)
     */
    @Override
    public void setStatus(String status) {
        context.setStatus(status);
    }

    @Override
    public float getProgress() {
        return context.getProgress();
    }

}
