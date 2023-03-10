package datawave.ingest.mapreduce.job.statsd;

import com.google.common.collect.Multimap;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.metadata.RawRecordMetadata;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.security.Credentials;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created on 4/27/16.
 */
public class StatsDEnabledClassesTest {
    
    public enum TestCounters {
        COUNTER1, COUNTER2
    }
    
    @Test
    public void testHelper() {
        Configuration conf = new Configuration();
        // basic config
        conf.set("statsd.host", "localhost");
        conf.set("statsd.port", "8125");
        conf.set("mapreduce.job.queuename", "queue1");
        conf.set("mapreduce.job.name", "job1");
        // some valid aspect configs
        conf.set("statsd.final.gauge.MyGroup1", "CounterGroup1");
        conf.set("statsd.final.counter.MyGroup2", "CounterGroup2/Counter1");
        conf.set("statsd.live.time.MyGroup3.MyCounter2", "CounterGroup3/Counter2");
        conf.set("statsd.live.counter.TestGroup", TestCounters.class.getName());
        CounterToStatsDConfiguration config = new CounterToStatsDConfiguration(conf);
        
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID(new TaskID(), 0));
        
        StatsDHelper helper = new StatsDHelper();
        helper.setup(conf);
        
        Assert.assertNotNull(helper.getClient());
        
        TaskAttemptContext returnedContext = helper.getContext(context);
        Assert.assertEquals(CounterStatsDClient.class.getName() + '$' + "StatsDTaskAttemptContext", returnedContext.getClass().getName());
        
        Counter testCounter = helper.getCounter(context, TestCounters.COUNTER1);
        Assert.assertEquals(CounterStatsDClient.class.getName() + '$' + "StatsDCounter", testCounter.getClass().getName());
        
        testCounter = helper.getCounter(context, "CounterGroup1", "Counter1");
        Assert.assertEquals(CounterStatsDClient.class.getName() + '$' + "StatsDCounter", testCounter.getClass().getName());
        
        // now lets use our test helper which uses a wrapped client so we can see the messages on the other side
        helper = new TestStatsDHelper();
        helper.setup(conf);
        
        CounterStatsDClientTest.TestCounterStatsDClient client = (CounterStatsDClientTest.TestCounterStatsDClient) (helper.getClient());
        helper.getCounter(context, TestCounters.COUNTER1).increment(10);
        helper.getCounter(context, "CounterGroup2", "Counter2").increment(11);
        helper.getCounter(context, "CounterGroup3", "Counter2").increment(12);
        
        Assert.assertEquals(new ArrayList(Arrays.asList("count(TestGroup_COUNTER1,10)", "time(MyGroup3_MyCounter2,12)")), client.messages);
        client.messages.clear();
        
        helper.close();
        Assert.assertTrue(client.stopped);
    }
    
    @Test
    public void testMapper() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        // basic config
        conf.set("statsd.host", "localhost");
        conf.set("statsd.port", "8125");
        conf.set("mapreduce.job.queuename", "queue1");
        conf.set("mapreduce.job.name", "job1");
        // some valid aspect configs
        conf.set("statsd.final.gauge.MyGroup1", "CounterGroup1");
        conf.set("statsd.final.counter.MyGroup2", "CounterGroup2/Counter1");
        conf.set("statsd.live.time.MyGroup3.MyCounter2", "CounterGroup3/Counter2");
        conf.set("statsd.live.counter.TestGroup", TestCounters.class.getName());
        CounterToStatsDConfiguration config = new CounterToStatsDConfiguration(conf);
        
        TestStatsDEnabledMapper mapper = new TestStatsDEnabledMapper();
        
        Mapper.Context context = mapper.createTestContext(conf);
        
        mapper.setup(context);
        
        Assert.assertNotNull(mapper.getHelper());
        
        TaskAttemptContext returnedContext = mapper.getContext(context);
        Assert.assertEquals(CounterStatsDClient.class.getName() + '$' + "StatsDTaskAttemptContext", returnedContext.getClass().getName());
        
        Counter testCounter = mapper.getCounter(context, TestCounters.COUNTER1);
        Assert.assertEquals(CounterStatsDClient.class.getName() + '$' + "StatsDCounter", testCounter.getClass().getName());
        
        testCounter = mapper.getCounter(context, "CounterGroup1", "Counter1");
        Assert.assertEquals(CounterStatsDClient.class.getName() + '$' + "StatsDCounter", testCounter.getClass().getName());
        
        Assert.assertFalse(((CounterStatsDClientTest.TestCounterStatsDClient) (mapper.getHelper()).getClient()).stopped);
        mapper.cleanup(context);
        Assert.assertNull(mapper.getHelper().getClient());
    }
    
    @Test
    public void testDataTypeHandler() {
        Configuration conf = new Configuration();
        // basic config
        conf.set("statsd.host", "localhost");
        conf.set("statsd.port", "8125");
        conf.set("mapreduce.job.queuename", "queue1");
        conf.set("mapreduce.job.name", "job1");
        // some valid aspect configs
        conf.set("statsd.final.gauge.MyGroup1", "CounterGroup1");
        conf.set("statsd.final.counter.MyGroup2", "CounterGroup2/Counter1");
        conf.set("statsd.live.time.MyGroup3.MyCounter2", "CounterGroup3/Counter2");
        conf.set("statsd.live.counter.TestGroup", TestCounters.class.getName());
        CounterToStatsDConfiguration config = new CounterToStatsDConfiguration(conf);
        
        TestStatsDEnabledDataTypeHandler handler = new TestStatsDEnabledDataTypeHandler();
        
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID(new TaskID(), 0));
        
        handler.setup(context);
        
        TaskAttemptContext returnedContext = handler.getContext(context);
        Assert.assertEquals(CounterStatsDClient.class.getName() + '$' + "StatsDTaskAttemptContext", returnedContext.getClass().getName());
        
        Counter testCounter = handler.getCounter(context, TestCounters.COUNTER1);
        Assert.assertEquals(CounterStatsDClient.class.getName() + '$' + "StatsDCounter", testCounter.getClass().getName());
        
        testCounter = handler.getCounter(context, "CounterGroup1", "Counter1");
        Assert.assertEquals(CounterStatsDClient.class.getName() + '$' + "StatsDCounter", testCounter.getClass().getName());
        
        Assert.assertFalse(handler.getTestClient().stopped);
        handler.close(context);
        Assert.assertNull(handler.getTestClient());
    }
    
    public static class TestStatsDHelper extends StatsDHelper {
        
        @Override
        public void setup(Configuration conf) {
            statsd = new CounterStatsDClientTest.TestCounterStatsDClient(new CounterToStatsDConfiguration(conf));
        }
    }
    
    public static class TestStatsDEnabledMapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT> extends StatsDEnabledMapper {
        @Override
        public StatsDHelper createHelper() {
            return new TestStatsDHelper();
        }
        
        public Mapper.Context createTestContext(Configuration conf) {
            return new MapperContextImpl(conf);
        }
        
        public class MapperContextImpl extends Mapper.Context {
            
            protected Configuration conf;
            
            public MapperContextImpl(Configuration conf) {
                this.conf = conf;
            }
            
            @Override
            public InputSplit getInputSplit() {
                return null;
            }
            
            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                return false;
            }
            
            @Override
            public Object getCurrentKey() throws IOException, InterruptedException {
                return null;
            }
            
            @Override
            public Object getCurrentValue() throws IOException, InterruptedException {
                return null;
            }
            
            @Override
            public void write(Object key, Object value) throws IOException, InterruptedException {
                
            }
            
            @Override
            public OutputCommitter getOutputCommitter() {
                return null;
            }
            
            @Override
            public TaskAttemptID getTaskAttemptID() {
                return null;
            }
            
            @Override
            public void setStatus(String msg) {
                
            }
            
            @Override
            public String getStatus() {
                return null;
            }
            
            @Override
            public float getProgress() {
                return 0;
            }
            
            @Override
            public Counter getCounter(Enum<?> counterName) {
                return null;
            }
            
            @Override
            public Counter getCounter(String groupName, String counterName) {
                return null;
            }
            
            @Override
            public Configuration getConfiguration() {
                return conf;
            }
            
            @Override
            public Credentials getCredentials() {
                return null;
            }
            
            @Override
            public JobID getJobID() {
                return null;
            }
            
            @Override
            public int getNumReduceTasks() {
                return 0;
            }
            
            @Override
            public Path getWorkingDirectory() throws IOException {
                return null;
            }
            
            @Override
            public Class<?> getOutputKeyClass() {
                return null;
            }
            
            @Override
            public Class<?> getOutputValueClass() {
                return null;
            }
            
            @Override
            public Class<?> getMapOutputKeyClass() {
                return null;
            }
            
            @Override
            public Class<?> getMapOutputValueClass() {
                return null;
            }
            
            @Override
            public String getJobName() {
                return null;
            }
            
            @Override
            public Class<? extends InputFormat<?,?>> getInputFormatClass() throws ClassNotFoundException {
                return null;
            }
            
            @Override
            public Class<? extends Mapper<?,?,?,?>> getMapperClass() throws ClassNotFoundException {
                return null;
            }
            
            @Override
            public Class<? extends Reducer<?,?,?,?>> getCombinerClass() throws ClassNotFoundException {
                return null;
            }
            
            @Override
            public Class<? extends Reducer<?,?,?,?>> getReducerClass() throws ClassNotFoundException {
                return null;
            }
            
            @Override
            public Class<? extends OutputFormat<?,?>> getOutputFormatClass() throws ClassNotFoundException {
                return null;
            }
            
            @Override
            public Class<? extends Partitioner<?,?>> getPartitionerClass() throws ClassNotFoundException {
                return null;
            }
            
            @Override
            public RawComparator<?> getSortComparator() {
                return null;
            }
            
            @Override
            public String getJar() {
                return null;
            }
            
            @Override
            public RawComparator<?> getCombinerKeyGroupingComparator() {
                return null;
            }
            
            @Override
            public RawComparator<?> getGroupingComparator() {
                return null;
            }
            
            @Override
            public boolean getJobSetupCleanupNeeded() {
                return false;
            }
            
            @Override
            public boolean getTaskCleanupNeeded() {
                return false;
            }
            
            @Override
            public boolean getProfileEnabled() {
                return false;
            }
            
            @Override
            public String getProfileParams() {
                return null;
            }
            
            @Override
            public Configuration.IntegerRanges getProfileTaskRange(boolean isMap) {
                return null;
            }
            
            @Override
            public String getUser() {
                return null;
            }
            
            @Override
            public boolean getSymlink() {
                return false;
            }
            
            @Override
            public Path[] getArchiveClassPaths() {
                return new Path[0];
            }
            
            @Override
            public URI[] getCacheArchives() throws IOException {
                return new URI[0];
            }
            
            @Override
            public URI[] getCacheFiles() throws IOException {
                return new URI[0];
            }
            
            @Override
            public Path[] getLocalCacheArchives() throws IOException {
                return new Path[0];
            }
            
            @Override
            public Path[] getLocalCacheFiles() throws IOException {
                return new Path[0];
            }
            
            @Override
            public Path[] getFileClassPaths() {
                return new Path[0];
            }
            
            @Override
            public String[] getArchiveTimestamps() {
                return new String[0];
            }
            
            @Override
            public String[] getFileTimestamps() {
                return new String[0];
            }
            
            @Override
            public int getMaxMapAttempts() {
                return 0;
            }
            
            @Override
            public int getMaxReduceAttempts() {
                return 0;
            }
            
            @Override
            public void progress() {
                
            }
        }
        
    }
    
    public static class TestStatsDEnabledDataTypeHandler extends StatsDEnabledDataTypeHandler {
        
        @Override
        public void setup(Configuration conf) {
            statsd = new CounterStatsDClientTest.TestCounterStatsDClient(new CounterToStatsDConfiguration(conf));
        }
        
        public CounterStatsDClientTest.TestCounterStatsDClient getTestClient() {
            return ((CounterStatsDClientTest.TestCounterStatsDClient) statsd);
        }
        
        @Override
        public String[] getTableNames(Configuration conf) {
            return new String[0];
        }
        
        @Override
        public int[] getTableLoaderPriorities(Configuration conf) {
            return new int[0];
        }
        
        @Override
        public IngestHelperInterface getHelper(Type datatype) {
            return null;
        }
        
        @Override
        public RawRecordMetadata getMetadata() {
            return null;
        }
        
        @Override
        public Multimap<BulkIngestKey,Value> processBulk(Object key, RawRecordContainer event, Multimap fields, StatusReporter reporter) {
            return null;
        }
    }
    
}
