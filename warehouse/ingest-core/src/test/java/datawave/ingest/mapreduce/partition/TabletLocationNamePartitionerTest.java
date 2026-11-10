package datawave.ingest.mapreduce.partition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.TableSplitsCache;

public class TabletLocationNamePartitionerTest {
    Configuration conf = new Configuration();
    TabletLocationNamePartitioner partitioner = null;

    @BeforeEach
    public void setUp() {
        conf = new Configuration();
        conf.setBoolean(TableSplitsCache.REFRESH_SPLITS, false);
        partitioner = new TabletLocationNamePartitioner();
        partitioner.setConf(conf);
    }

    @AfterEach
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

        // now read in a list of shards and display the distribution
        file = getClass().getResource("/datawave/ingest/mapreduce/partition/shards.list");
        conf.set(TableSplitsCache.SPLITS_CACHE_DIR, file.getPath().substring(0, file.getPath().lastIndexOf('/')));
        conf.set(TableSplitsCache.SPLITS_CACHE_FILE, "shards_n_locs.list");
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
                assertEquals(expected[i], distribution[i], "Unexpected distribution: ");
            } catch (Throwable e) {
                System.err.println(e.getMessage());
                errored = true;
            }
        }
        if (errored) {
            fail("Failed to get expected distribution.  See console for unexpected entries");
        }

    }
}
