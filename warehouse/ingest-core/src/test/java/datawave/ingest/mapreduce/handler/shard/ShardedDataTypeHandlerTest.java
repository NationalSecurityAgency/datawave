package datawave.ingest.mapreduce.handler.shard;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Test;

public class ShardedDataTypeHandlerTest {
    
    @Test(expected = IllegalArgumentException.class)
    public void testSetup() {
        Configuration conf = new Configuration();
        ShardedDataTypeHandler<Text> handler = new AbstractColumnBasedHandler<>();
        handler.setup(new TaskAttemptContextImpl(conf, new TaskAttemptID()));
    }
}
