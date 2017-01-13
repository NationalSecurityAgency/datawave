package nsa.datawave.ingest.mapreduce.job.metrics;

import com.google.common.collect.Multimap;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class MetricsConfigurationTest {
    
    @Test
    public void shouldParseConfigurationFromHadoopConf() {
        Configuration conf = MetricsTestData.loadDefaultTestConfig();
        
        assertEquals(true, MetricsConfiguration.isEnabled(conf));
        assertEquals("ingestMetrics", MetricsConfiguration.getTable(conf));
        assertEquals(1, MetricsConfiguration.getNumShards(conf));
        
        Multimap<String,String> labels = MetricsConfiguration.getLabels(conf);
        assertTrue(labels.containsEntry("table", "shard"));
        assertTrue(labels.containsEntry("table", "shardIndex"));
        
        Collection<MetricsReceiver> receivers = MetricsConfiguration.getReceivers(conf);
        assertTrue(receivers.iterator().next() instanceof TestKeyValueCountMetricsReceiver);
    }
}
