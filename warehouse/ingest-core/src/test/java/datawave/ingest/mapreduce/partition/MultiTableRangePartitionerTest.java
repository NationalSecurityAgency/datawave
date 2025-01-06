package datawave.ingest.mapreduce.partition;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.TableConfigurationUtil;
import datawave.ingest.mapreduce.job.TableSplitsCache;
import datawave.util.TableName;

public class MultiTableRangePartitionerTest {
    private static final String TABLE_NAME = "abc";
    Configuration configuration;
    Job mockJob;

    @Before
    public void before() throws IOException {
        mockJob = Job.getInstance();
        configuration = mockJob.getConfiguration();
        configuration.setBoolean(TableSplitsCache.REFRESH_SPLITS, false);
        TableSplitsCache.getCurrentCache(configuration).clear();
        configuration.set(TableConfigurationUtil.JOB_OUTPUT_TABLE_NAMES, TableName.SHARD);
    }

    @Test
    public void testGoodSplitsFile() throws IOException, URISyntaxException {
        String filename = "trimmed_splits.txt";
        URL url = createUrl(filename);
        mockContextForLocalCacheFile(url);
        configuration.set(TableSplitsCache.SPLITS_CACHE_DIR, url.getPath().substring(0, url.getPath().lastIndexOf('/')));
        configuration.set(TableSplitsCache.SPLITS_CACHE_FILE, filename);
        Assert.assertEquals(5, getPartition());
    }

    @Test(expected = RuntimeException.class)
    public void testEmptySplitsThrowsException() throws IOException, URISyntaxException {
        String filename = "trimmed_empty_splits.txt";
        URL url = createUrl(filename);
        mockContextForLocalCacheFile(url);
        configuration.set(TableSplitsCache.SPLITS_CACHE_DIR, url.getPath().substring(0, url.getPath().lastIndexOf('/')));
        configuration.set(TableSplitsCache.SPLITS_CACHE_FILE, filename);
        getPartition();
    }

    @Test(expected = RuntimeException.class)
    public void testProblemGettingLocalCacheFiles() throws IOException, URISyntaxException {
        final URL url = createUrl("trimmed_splits.txt");

        MultiTableRangePartitioner.setContext(new MapContextImpl<Key,Value,Text,Mutation>(configuration, new TaskAttemptID(), null, null, null, null, null) {
            @Override
            public org.apache.hadoop.fs.Path[] getLocalCacheFiles() throws IOException {
                throw new IOException("Local cache files failure");
            }
        });

        getPartition();
    }

    private URL createUrl(String fileName) {
        return MultiTableRangePartitionerTest.class.getResource("/datawave/ingest/mapreduce/job/" + fileName);
    }

    private void mockContextForLocalCacheFile(final URL url) {
        MultiTableRangePartitioner.setContext(new MapContextImpl<Key,Value,Text,Mutation>(configuration, new TaskAttemptID(), null, null, null, null, null) {
            @Override
            public Path[] getLocalCacheFiles() throws IOException {
                return new Path[] {new Path(url.getPath())};
            }
        });
    }

    private int getPartition() {
        MultiTableRangePartitioner partitioner = new MultiTableRangePartitioner();

        partitioner.setConf(configuration);

        return partitioner.getPartition(new BulkIngestKey(new Text(TABLE_NAME), new Key("23432")), new Value("fdsafdsa".getBytes()), 100);
    }
}
