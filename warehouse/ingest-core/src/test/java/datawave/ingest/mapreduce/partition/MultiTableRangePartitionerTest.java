package datawave.ingest.mapreduce.partition;

import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.util.TableName;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

public class MultiTableRangePartitionerTest {
    private static final String TABLE_NAME = "abc";
    Configuration configuration;
    Job mockJob;
    
    @Before
    public void before() throws IOException {
        mockJob = new Job();
        configuration = mockJob.getConfiguration();
        configuration.set("job.table.names", TableName.SHARD);
        configuration.set("ingest.work.dir.qualified", "testtmp");
    }
    
    @Test
    public void testGoodSplitsFile() throws IOException, URISyntaxException {
        mockContextForLocalCacheFile(createUrl("trimmed_splits.txt"));
        Assert.assertEquals(5, getPartition());
    }
    
    @Test(expected = RuntimeException.class)
    public void testEmptySplitsThrowsException() throws IOException, URISyntaxException {
        mockContextForLocalCacheFile(createUrl("trimmed_empty_splits.txt"));
        getPartition();
    }
    
    @Test
    public void testCleanUpWorkDir() throws URISyntaxException, IOException {
        MultiTableRangePartitioner partitioner = new MultiTableRangePartitioner();
        partitioner.setConf(configuration);
        // create a temp path and uri
        Path workDirPath = new Path(configuration.get("ingest.work.dir.qualified"));
        FileSystem outputFs = FileSystem.get(configuration);
        outputFs.mkdirs(workDirPath);
        Assert.assertTrue(outputFs.exists(workDirPath));
        partitioner.cleanUpWorkDir();
        Assert.assertFalse(outputFs.exists(workDirPath));
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
        partitioner.setConf(new Configuration());
        return partitioner.getPartition(new BulkIngestKey(new Text(TABLE_NAME), new Key("23432")), new Value("fdsafdsa".getBytes()), 100);
    }
}
