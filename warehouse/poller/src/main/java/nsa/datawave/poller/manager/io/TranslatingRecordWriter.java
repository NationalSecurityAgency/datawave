package nsa.datawave.poller.manager.io;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public abstract class TranslatingRecordWriter<K1,V1,K2,V2> extends RecordWriter<K1,V1> implements Configurable {
    /**
     * Key of the boolean property looked up in the configuration to determine whether records should be deleted instead of added.
     */
    public static final String DELETE_RECORDS_PROP = "nsa.datawave.poller.manager.io.delete";
    
    private Configuration conf;
    protected RecordWriter<K2,V2> sink;
    
    public void setSink(RecordWriter<K2,V2> rw) {
        this.sink = rw;
    }
    
    @Override
    public void write(K1 key, V1 value) throws IOException, InterruptedException {
        translateAndWrite(key, value, sink, isDelete());
    }
    
    abstract public void translateAndWrite(K1 key, V1 value, RecordWriter<K2,V2> sink, boolean deleteRecord) throws IOException, InterruptedException;
    
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        if (sink != null) {
            sink.close(context);
        }
    }
    
    /**
     * Checks whether this writer is deleting entries rather than inserting them.
     * 
     * @return whether the writer is deleting entries
     */
    public boolean isDelete() {
        return conf != null && conf.getBoolean(DELETE_RECORDS_PROP, false);
    }
    
    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }
    
    @Override
    public Configuration getConf() {
        return conf;
    }
}
