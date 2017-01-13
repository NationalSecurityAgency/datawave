package nsa.datawave.metrics.mapreduce.error;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import nsa.datawave.util.StringUtils;
import nsa.datawave.metrics.config.MetricsConfig;

import nsa.datawave.util.time.DateHelper;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class ProcessingErrorsReducer extends Reducer<Text,Text,Text,Mutation> {
    
    private static final Logger log = Logger.getLogger(ProcessingErrorsReducer.class);
    
    protected BatchWriter writer;
    
    protected BatchWriter warehouseWriter;
    
    protected void cleanup(Reducer.Context context) {
        if (writer != null)
            try {
                writer.close();
            } catch (MutationsRejectedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        if (warehouseWriter != null)
            try {
                warehouseWriter.close();
            } catch (MutationsRejectedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
    }
    
    protected void setup(Reducer<Text,Text,Text,Mutation>.Context context) {
        Configuration conf = context.getConfiguration();
        
        String zooKeepers = conf.get(MetricsConfig.ZOOKEEPERS);
        ClientConfiguration zkConfig = ClientConfiguration.loadDefault().withInstance(conf.get(MetricsConfig.INSTANCE)).withZkHosts(zooKeepers);
        ZooKeeperInstance instance = new ZooKeeperInstance(zkConfig);
        try {
            Connector connector = instance.getConnector(conf.get(MetricsConfig.USER), new PasswordToken(conf.get(MetricsConfig.PASS).getBytes()));
            
            writer = connector.createBatchWriter(conf.get(MetricsConfig.METRICS_TABLE, MetricsConfig.DEFAULT_METRICS_TABLE), new BatchWriterConfig()
                            .setMaxLatency(1, TimeUnit.SECONDS).setMaxMemory(128L * 1024L).setMaxWriteThreads(11));
        } catch (AccumuloException | TableNotFoundException | AccumuloSecurityException e) {
            writer = null;
        }
        
        try {
            
            zooKeepers = conf.get(MetricsConfig.WAREHOUSE_ZOOKEEPERS);
            zkConfig = ClientConfiguration.loadDefault().withInstance(conf.get(MetricsConfig.WAREHOUSE_INSTANCE)).withZkHosts(zooKeepers);
            instance = new ZooKeeperInstance(zkConfig);
            Connector connector = instance.getConnector(conf.get(MetricsConfig.WAREHOUSE_USERNAME), new PasswordToken(conf
                            .get(MetricsConfig.WAREHOUSE_PASSWORD).getBytes()));
            
            warehouseWriter = connector.createBatchWriter(conf.get(MetricsConfig.ERRORS_TABLE, MetricsConfig.DEFAULT_ERRORS_TABLE), new BatchWriterConfig()
                            .setMaxLatency(1, TimeUnit.SECONDS).setMaxMemory(128L * 1024L * 1024L).setMaxWriteThreads(11));
        } catch (AccumuloException | TableNotFoundException | AccumuloSecurityException e) {
            warehouseWriter = null;
        }
    }
    
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
        String[] keySplit = StringUtils.split(key.toString(), "\0");
        log.info("key is " + key);
        
        // we are dealing with counts for the specific data type.
        long count = 0;
        
        Mutation m = new Mutation(new Text("metrics"));
        String jobNamePrefix = "IngestJob_";
        String jobName = keySplit[1].trim();
        
        if (jobName.startsWith((jobNamePrefix))) {
            int end = jobName.lastIndexOf(".");
            if (end < 0)
                end = jobName.length();
            jobName = jobName.substring(jobNamePrefix.length(), end);
            
        }
        
        m.putDelete(new Text(jobName), new Text(""));
        
        try {
            warehouseWriter.addMutation(m);
        } catch (MutationsRejectedException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        
        for (Text value : values) {
            count++;
        }
        
        jobName = keySplit[1];
        m = new Mutation(new Text(jobName));
        
        String[] jobNameSplit = StringUtils.split(jobName, "_");
        
        String jobTime = StringUtils.split(jobNameSplit[1], '.')[0];
        
        Date date = null;
        
        String timeString = "";
        try {
            date = DateHelper.parseTimeExactToSeconds(jobTime);
            timeString = Long.toString(date.getTime());
        } catch (IllegalArgumentException e) {
            throw new IOException(e);
        }
        
        if (keySplit.length == 3 && "cnt".equals(keySplit[2])) {
            // dataType + "\0" + jobName + "\0cnt"
            
            Value value = new Value(WritableUtils.toByteArray(new LongWritable(count)));
            
            m.put(new Text("cnt"), new Text(keySplit[0]), value);
            
            context.getCounter("Count", keySplit[0]).increment(count);
            
            try {
                writer.addMutation(m);
            } catch (MutationsRejectedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
            // context.write(table,m);
            
            m = new Mutation(timeString);
            
            m.put(new Text("cnt"), new Text(keySplit[0]), value);
            
            try {
                writer.addMutation(m);
            } catch (MutationsRejectedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
            // context.write(table,m);
            
        } else if (keySplit.length == 4 && "infocnt".equals(keySplit[3])) {
            //
            // dataType + "\0" + jobName + "\0" + type + "\0infocnt"
            Value value = new Value(WritableUtils.toByteArray(new LongWritable(count)));
            
            m.put(new Text("infocnt\0" + keySplit[0]), new Text(keySplit[2]), value);
            
            // context.write(table,m);
            
            try {
                writer.addMutation(m);
            } catch (MutationsRejectedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
            m = new Mutation(timeString);
            
            m.put(new Text("infocnt\0" + keySplit[0]), new Text(keySplit[2]), value);
            
            // context.write(table,m);
            try {
                writer.addMutation(m);
            } catch (MutationsRejectedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
            m = new Mutation(new Text(keySplit[2]));
            
            m.put(new Text("total\0" + keySplit[1]), new Text(keySplit[2]), value);
            
            context.getCounter("ErrorType", keySplit[2]).increment(count);
            
            try {
                writer.addMutation(m);
            } catch (MutationsRejectedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
            // context.write(table,m);
            
        }
    }
}
