package datawave.ingest.mapreduce.job.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.google.common.collect.Multimap;

import datawave.util.TableName;

public class MetricsConfigurationTest {

    @Test
    public void shouldParseConfigurationFromHadoopConf() {
        Configuration conf = MetricsTestData.loadDefaultTestConfig();

        assertTrue(MetricsConfiguration.isEnabled(conf));
        assertEquals("ingestMetrics", MetricsConfiguration.getTable(conf));
        assertEquals(1, MetricsConfiguration.getNumShards(conf));

        Multimap<String,String> labels = MetricsConfiguration.getLabels(conf);
        assertTrue(labels.containsEntry("table", TableName.SHARD));
        assertTrue(labels.containsEntry("table", TableName.SHARD_INDEX));

        Collection<MetricsReceiver> receivers = MetricsConfiguration.getReceivers(conf);
        assertTrue(receivers.iterator().next() instanceof TestKeyValueCountMetricsReceiver);
    }
}
