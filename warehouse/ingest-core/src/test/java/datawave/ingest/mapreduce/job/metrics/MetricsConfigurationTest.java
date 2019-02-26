package datawave.ingest.mapreduce.job.metrics;

import com.google.common.collect.Multimap;
import datawave.util.TableNames;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class MetricsConfigurationTest {
    
    @Test
    public void shouldParseConfigurationFromHadoopConf() {
        Configuration conf = MetricsTestData.loadDefaultTestConfig();
        
        assertTrue(MetricsConfiguration.isEnabled(conf));
        assertEquals("ingestMetrics", MetricsConfiguration.getTable(conf));
        assertEquals(1, MetricsConfiguration.getNumShards(conf));
        
        Multimap<String,String> labels = MetricsConfiguration.getLabels(conf);
        assertTrue(labels.containsEntry("table", TableNames.SHARD_TABLE_NAME));
        assertTrue(labels.containsEntry("table", TableNames.SHARD_INDEX_TABLE_NAME));
        
        Collection<MetricsReceiver> receivers = MetricsConfiguration.getReceivers(conf);
        assertTrue(receivers.iterator().next() instanceof TestKeyValueCountMetricsReceiver);
    }
}
