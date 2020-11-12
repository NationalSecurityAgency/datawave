package datawave.ingest.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskInputOutputContextImpl;

public class StandaloneTaskAttemptContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> extends TaskInputOutputContextImpl<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
    public StandaloneTaskAttemptContext(Configuration conf, StatusReporter reporter) {
        this(conf, new TaskAttemptID(), reporter);
    }
    
    public StandaloneTaskAttemptContext(Configuration conf, TaskAttemptID taskAttemptID, StatusReporter reporter) {
        super(conf, taskAttemptID, null, null, reporter);
    }
    
    @Override
    public KEYIN getCurrentKey() throws IOException, InterruptedException {
        // not used
        return null;
    }
    
    @Override
    public VALUEIN getCurrentValue() throws IOException, InterruptedException {
        // not used
        return null;
    }
    
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        // not used
        return false;
    }
    
}
