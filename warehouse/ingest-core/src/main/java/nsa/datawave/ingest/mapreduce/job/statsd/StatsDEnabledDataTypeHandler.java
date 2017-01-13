package nsa.datawave.ingest.mapreduce.job.statsd;

import nsa.datawave.ingest.mapreduce.handler.DataTypeHandler;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Created on 4/25/16.
 */
public abstract class StatsDEnabledDataTypeHandler<KEYIN> extends StatsDHelper implements DataTypeHandler<KEYIN> {
    
    @Override
    public void setup(TaskAttemptContext context) {
        super.setup(context);
    }
    
    @Override
    public void close(TaskAttemptContext context) {
        super.close();
    }
}
