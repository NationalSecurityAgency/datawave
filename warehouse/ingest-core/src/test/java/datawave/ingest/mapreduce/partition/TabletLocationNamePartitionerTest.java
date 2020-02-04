package datawave.ingest.mapreduce.partition;

import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.ShardedTableMapFile;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class TabletLocationNamePartitionerTest {
    Configuration conf = new Configuration();
    TabletLocationNamePartitioner partitioner = null;
    
    @Before
    public void setUp() {
        conf = new Configuration();
        partitioner = new TabletLocationNamePartitioner();
        partitioner.setConf(conf);
    }
    
    @After
    public void tearDown() {
        conf.clear();
        conf = null;
        partitioner = null;
    }
    
    @Test
    public void testSequentialLocationScheme() throws Exception {
        Map<String,Path> shardedTableMapFiles = new HashMap<>();
        // setup the location partition sheme
        URL file = getClass().getResource("/datawave/ingest/mapreduce/partition/_shards.lst");
        shardedTableMapFiles.put("shard", new Path(file.toURI().toString()));
        
        ShardedTableMapFile.addToConf(conf, shardedTableMapFiles);
        
        // now read in a list of shards and display the distribution
        file = getClass().getResource("/datawave/ingest/mapreduce/partition/shards.list");
        BufferedReader reader = new BufferedReader(new InputStreamReader(file.openStream()));
        String line = reader.readLine();
        int[] partitions = new int[912];
        while (line != null) {
            String shardId = line.trim();
            Key key = new Key(shardId);
            Value value = new Value();
            int partition = partitioner.getPartition(new BulkIngestKey(new Text("shard"), key), value, 912);
            partitions[partition]++;
            line = reader.readLine();
        }
        boolean errored = false;
        int[] distribution = new int[8];
        int[] expected = new int[] {77, 207, 266, 196, 112, 42, 9, 3};
        for (int i = 0; i < 912; i++) {
            distribution[partitions[i]]++;
        }
        for (int i = 0; i < 8; i++) {
            try {
                Assert.assertEquals("Unexpected distribution: ", expected[i], distribution[i]);
            } catch (Throwable e) {
                System.err.println(e.getMessage());
                errored = true;
            }
        }
        if (errored) {
            Assert.fail("Failed to get expected distribution.  See console for unexpected entries");
        }
        
    }
}
