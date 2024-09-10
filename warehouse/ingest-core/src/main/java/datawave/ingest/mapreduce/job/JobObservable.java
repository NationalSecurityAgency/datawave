package datawave.ingest.mapreduce.job;

import java.util.Observable;

import org.apache.hadoop.fs.FileSystem;

public class JobObservable extends Observable {
    private final FileSystem fs;
    private String jobId;

    public JobObservable(FileSystem fs) {
        this.fs = fs;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
        setChanged();
        notifyObservers();
    }

    public FileSystem getFs() {
        return fs;
    }
}
