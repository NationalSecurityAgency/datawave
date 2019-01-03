package datawave.ingest.mapreduce.job.statsd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.counters.CounterGroupBase;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

/**
 * Created on 4/27/16.
 */
public class CounterToStatsDConfigurationTest {
    
    @Test
    public void testConfig() {
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
        // some invalid (and hence ignored) aspect configs
        conf.set("statsd.live.time", "CounterGroup4");
        conf.set("statsd.live.unk.MyGroup5", "CounterGroup5");
        conf.set("statsd.unk.time.MyGroup6", "CounterGroup6");
        conf.set("stats.live.time.MyGroup7", "CounterGroup7");
        conf.set("statsd.live.time.MyGroup8", "CounterGroup8/Counter1/extra");
        conf.set("statsd.live.time.MyGroup9.MyCounter9", "CounterGroup9");
        CounterToStatsDConfiguration config = new CounterToStatsDConfiguration(conf);
        
        Assert.assertTrue(config.isConfigured());
        Assert.assertEquals("localhost", config.getHost());
        Assert.assertEquals(8125, config.getPort());
        Assert.assertEquals("queue1", config.getQueueName());
        Assert.assertEquals("job1", config.getJobName());
        Assert.assertEquals(new CounterToStatsDConfiguration.StatsDAspect(CounterToStatsDConfiguration.StatsDType.GAUGE, "MyGroup1", null),
                        config.getAspect(CounterToStatsDConfiguration.StatsDOutputType.FINAL, "CounterGroup1", (String) null));
        Assert.assertEquals(new CounterToStatsDConfiguration.StatsDAspect(CounterToStatsDConfiguration.StatsDType.GAUGE, "MyGroup1", null),
                        config.getAspect(CounterToStatsDConfiguration.StatsDOutputType.FINAL, new TestCounterGroup("CounterGroup1"), (Counter) null));
        Assert.assertEquals(new CounterToStatsDConfiguration.StatsDAspect(CounterToStatsDConfiguration.StatsDType.GAUGE, "MyGroup1", null),
                        config.getAspect(CounterToStatsDConfiguration.StatsDOutputType.FINAL, "CounterGroup1", "Counter1"));
        Assert.assertEquals(new CounterToStatsDConfiguration.StatsDAspect(CounterToStatsDConfiguration.StatsDType.GAUGE, "MyGroup1", null),
                        config.getAspect(CounterToStatsDConfiguration.StatsDOutputType.FINAL, "CounterGroup1", new TestCounter("Counter1")));
        Assert.assertEquals(new CounterToStatsDConfiguration.StatsDAspect(CounterToStatsDConfiguration.StatsDType.GAUGE, "MyGroup1", null), config.getAspect(
                        CounterToStatsDConfiguration.StatsDOutputType.FINAL, new TestCounterGroup("CounterGroup1"), new TestCounter("Counter1")));
        Assert.assertEquals(new CounterToStatsDConfiguration.StatsDAspect(CounterToStatsDConfiguration.StatsDType.COUNTER, "MyGroup2", null),
                        config.getAspect(CounterToStatsDConfiguration.StatsDOutputType.FINAL, "CounterGroup2", "Counter1"));
        Assert.assertEquals(new CounterToStatsDConfiguration.StatsDAspect(CounterToStatsDConfiguration.StatsDType.TIME, "MyGroup3", "MyCounter2"),
                        config.getAspect(CounterToStatsDConfiguration.StatsDOutputType.LIVE, "CounterGroup3", "Counter2"));
        Assert.assertEquals(new CounterToStatsDConfiguration.StatsDAspect(CounterToStatsDConfiguration.StatsDType.TIME, "MyGroup3", "MyCounter2"),
                        config.getAspect(CounterToStatsDConfiguration.StatsDOutputType.LIVE, "CounterGroup3", "Counter2"));
        
        Assert.assertNull(config.getAspect(CounterToStatsDConfiguration.StatsDOutputType.LIVE, "CounterGroup1", (String) null));
        Assert.assertNull(config.getAspect(CounterToStatsDConfiguration.StatsDOutputType.LIVE, "CounterGroup2", "Counter1"));
        Assert.assertNull(config.getAspect(CounterToStatsDConfiguration.StatsDOutputType.FINAL, "CounterGroup2", (String) null));
        Assert.assertNull(config.getAspect(CounterToStatsDConfiguration.StatsDOutputType.FINAL, "CounterGroup2", "Counter2"));
        Assert.assertNull(config.getAspect(CounterToStatsDConfiguration.StatsDOutputType.FINAL, "CounterGroup3", "Counter2"));
        Assert.assertNull(config.getAspect(CounterToStatsDConfiguration.StatsDOutputType.LIVE, "CounterGroup3", "Counter3"));
        Assert.assertNull(config.getAspect(CounterToStatsDConfiguration.StatsDOutputType.LIVE, "CounterGroup3", (String) null));
        
        Assert.assertNull(config.getAspect(CounterToStatsDConfiguration.StatsDOutputType.LIVE, "CounterGroup4", (String) null));
        Assert.assertNull(config.getAspect(CounterToStatsDConfiguration.StatsDOutputType.LIVE, "CounterGroup5", (String) null));
        Assert.assertNull(config.getAspect(CounterToStatsDConfiguration.StatsDOutputType.LIVE, "CounterGroup6", (String) null));
        Assert.assertNull(config.getAspect(CounterToStatsDConfiguration.StatsDOutputType.LIVE, "CounterGroup7", (String) null));
        Assert.assertNull(config.getAspect(CounterToStatsDConfiguration.StatsDOutputType.LIVE, "CounterGroup8", (String) null));
        Assert.assertNull(config.getAspect(CounterToStatsDConfiguration.StatsDOutputType.LIVE, "CounterGroup9", (String) null));
    }
    
    public static class TestCounterGroup implements CounterGroup {
        private String name;
        
        public TestCounterGroup(String name) {
            this.name = name;
        }
        
        @Override
        public String getName() {
            return name;
        }
        
        @Override
        public String getDisplayName() {
            return null;
        }
        
        @Override
        public void setDisplayName(String displayName) {
            
        }
        
        @Override
        public void addCounter(Counter counter) {
            
        }
        
        @Override
        public Counter addCounter(String name, String displayName, long value) {
            return null;
        }
        
        @Override
        public Counter findCounter(String counterName, String displayName) {
            return null;
        }
        
        @Override
        public Counter findCounter(String counterName, boolean create) {
            return null;
        }
        
        @Override
        public Counter findCounter(String counterName) {
            return null;
        }
        
        @Override
        public int size() {
            return 0;
        }
        
        @Override
        public void incrAllCounters(CounterGroupBase<Counter> rightGroup) {
            
        }
        
        @Override
        public CounterGroupBase<Counter> getUnderlyingGroup() {
            return null;
        }
        
        @Override
        public Iterator<Counter> iterator() {
            return null;
        }
        
        @Override
        public void write(DataOutput out) throws IOException {
            
        }
        
        @Override
        public void readFields(DataInput in) throws IOException {
            
        }
    }
    
    public static class TestCounter implements Counter {
        private String name;
        private long value = 0;
        
        public TestCounter(String name) {
            this.name = name;
        }
        
        @Override
        public void setDisplayName(String displayName) {
            
        }
        
        @Override
        public String getName() {
            return name;
        }
        
        @Override
        public String getDisplayName() {
            return null;
        }
        
        @Override
        public long getValue() {
            return value;
        }
        
        @Override
        public void setValue(long value) {
            this.value = value;
        }
        
        @Override
        public void increment(long incr) {
            this.value = this.value + 1;
        }
        
        @Override
        public Counter getUnderlyingCounter() {
            return this;
        }
        
        @Override
        public void write(DataOutput out) throws IOException {
            
        }
        
        @Override
        public void readFields(DataInput in) throws IOException {
            
        }
    }
}
