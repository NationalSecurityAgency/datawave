package datawave.ingest.mapreduce.job.statsd;

import com.timgroup.statsd.StatsDClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counters;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created on 4/27/16.
 */
public class CounterStatsDClientTest {
    
    @Test
    public void testClient() {
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
        CounterToStatsDConfiguration config = new CounterToStatsDConfiguration(conf);
        
        TestCounterStatsDClient client = new TestCounterStatsDClient(config);
        Assert.assertEquals(TestCounterStatsDClient.MyStatsDClient.class, client.client.getClass());
        Assert.assertEquals("queue1.dwingest", client.prefix);
        Assert.assertEquals("localhost", client.host);
        Assert.assertEquals(8125, client.port);
        Assert.assertFalse(client.stopped);
        
        client.sendLiveStat("CounterGroup1", new CounterToStatsDConfigurationTest.TestCounter("Counter1"), 1);
        client.sendLiveStat("CounterGroup2", new CounterToStatsDConfigurationTest.TestCounter("Counter1"), 1);
        client.sendLiveStat("CounterGroup3", new CounterToStatsDConfigurationTest.TestCounter("Counter1"), 1);
        client.sendLiveStat("CounterGroup3", new CounterToStatsDConfigurationTest.TestCounter("Counter2"), 1);
        client.sendLiveStat("CounterGroup3", new CounterToStatsDConfigurationTest.TestCounter("Counter3"), 1);
        
        Assert.assertEquals(new ArrayList(Arrays.asList("time(MyGroup3_MyCounter2,1)")), client.messages);
        client.messages.clear();
        
        Counters counters = new Counters();
        counters.findCounter("CounterGroup1", "Counter1").setValue(10);
        counters.findCounter("CounterGroup1", "Counter2").setValue(10);
        counters.findCounter("CounterGroup2", "Counter1").setValue(11);
        counters.findCounter("CounterGroup2", "Counter2").setValue(12);
        counters.findCounter("CounterGroup3", "Counter1").setValue(13);
        counters.findCounter("CounterGroup3", "Counter2").setValue(13);
        client.sendFinalStats(counters);
        
        Assert.assertEquals(new ArrayList(Arrays.asList("gauge(MyGroup1_Counter1,10)", "gauge(MyGroup1_Counter2,10)", "count(MyGroup2_Counter1,11)")),
                        client.messages);
        client.messages.clear();
        
        Assert.assertFalse(client.stopped);
        client.close();
        Assert.assertTrue(client.stopped);
    }
    
    public static class TestCounterStatsDClient extends CounterStatsDClient {
        public MyStatsDClient client;
        public String prefix;
        public String host;
        public int port;
        public boolean stopped = false;
        public List<String> messages = new ArrayList<>();
        
        public TestCounterStatsDClient(CounterToStatsDConfiguration config) {
            super(config);
        }
        
        @Override
        protected StatsDClient createClient(String prefix, String host, int port) {
            this.client = new MyStatsDClient(prefix, host, port);
            return this.client;
        }
        
        public class MyStatsDClient implements StatsDClient {
            public MyStatsDClient(String prefix, String host, int port) {
                TestCounterStatsDClient.this.prefix = prefix;
                TestCounterStatsDClient.this.host = host;
                TestCounterStatsDClient.this.port = port;
            }
            
            @Override
            public void stop() {
                stopped = true;
            }
            
            @Override
            public void count(String aspect, long delta) {
                messages.add("count(" + aspect + ',' + delta + ')');
            }
            
            @Override
            public void count(String aspect, long delta, double sampleRate) {
                messages.add("count(" + aspect + ',' + delta + ',' + sampleRate + ')');
            }
            
            @Override
            public void incrementCounter(String aspect) {
                messages.add("incrementCounter(" + aspect + ')');
            }
            
            @Override
            public void increment(String aspect) {
                messages.add("increment(" + aspect + ')');
            }
            
            @Override
            public void decrementCounter(String aspect) {
                messages.add("decrementCounter(" + aspect + ')');
                
            }
            
            @Override
            public void decrement(String aspect) {
                messages.add("decrement(" + aspect + ')');
                
            }
            
            @Override
            public void recordGaugeValue(String aspect, long value) {
                messages.add("recordGaugeValue(" + aspect + ',' + value + ')');
                
            }
            
            @Override
            public void recordGaugeValue(String aspect, double value) {
                messages.add("recordGaugeValue(" + aspect + ',' + value + ')');
                
            }
            
            @Override
            public void recordGaugeDelta(String aspect, long delta) {
                messages.add("recordGaugeDelta(" + aspect + ',' + delta + ')');
                
            }
            
            @Override
            public void recordGaugeDelta(String aspect, double delta) {
                messages.add("recordGaugeDelta(" + aspect + ',' + delta + ')');
                
            }
            
            @Override
            public void gauge(String aspect, long value) {
                messages.add("gauge(" + aspect + ',' + value + ')');
                
            }
            
            @Override
            public void gauge(String aspect, double value) {
                messages.add("gauge(" + aspect + ',' + value + ')');
                
            }
            
            @Override
            public void recordSetEvent(String aspect, String eventName) {
                messages.add("recordSetEvent(" + aspect + ',' + eventName + ')');
                
            }
            
            @Override
            public void set(String aspect, String eventName) {
                messages.add("set(" + aspect + ',' + eventName + ')');
                
            }
            
            @Override
            public void recordExecutionTime(String aspect, long timeInMs) {
                messages.add("recordExecutionTime(" + aspect + ',' + timeInMs + ')');
                
            }
            
            @Override
            public void recordExecutionTime(String aspect, long timeInMs, double sampleRate) {
                messages.add("recordExecutionTime(" + aspect + ',' + timeInMs + ',' + sampleRate + ')');
                
            }
            
            @Override
            public void recordExecutionTimeToNow(String aspect, long systemTimeMillisAtStart) {
                messages.add("recordExecutionTimeToNow(" + aspect + ',' + systemTimeMillisAtStart + ')');
                
            }
            
            @Override
            public void time(String aspect, long value) {
                messages.add("time(" + aspect + ',' + value + ')');
            }
        }
    }
}
