package datawave.ingest.mapreduce.job;

import java.beans.PropertyChangeSupport;

import org.apache.hadoop.fs.FileSystem;

public class JobObservable {
    private final FileSystem fs;
    private String jobId;
    private PropertyChangeSupport support;

    public JobObservable(FileSystem fs) {
        this.fs = fs;
        this.support = new PropertyChangeSupport(this);
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        support.firePropertyChange("jobId", this.jobId, jobId);
        this.jobId = jobId;
    }

    public FileSystem getFs() {
        return fs;
    }
}
