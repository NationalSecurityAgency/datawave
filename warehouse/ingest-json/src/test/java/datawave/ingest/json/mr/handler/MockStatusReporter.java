package datawave.ingest.json.mr.handler;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.StatusReporter;

/**
 * 
 */
public class MockStatusReporter extends StatusReporter {
    
    private Counters counters = new Counters();
    private String status = null;
    private int progress = 0;
    
    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapreduce.StatusReporter#getCounter(java.lang.Enum)
     */
    @Override
    public Counter getCounter(Enum<?> name) {
        return getCounter("default", name.name());
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapreduce.StatusReporter#getCounter(java.lang.String, java.lang.String)
     */
    @Override
    public Counter getCounter(String group, String name) {
        return counters.getGroup(group).findCounter(name);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapreduce.StatusReporter#progress()
     */
    @Override
    public void progress() {
        progress++;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapreduce.StatusReporter#setStatus(java.lang.String)
     */
    @Override
    public void setStatus(String status) {
        this.status = status;
    }
    
    /**
     * Get the counters
     */
    public Counters getCounters() {
        return counters;
    }
    
    public String getStatus() {
        return status;
    }
    
    public float getProgress() {
        return progress;
    }
    
}
