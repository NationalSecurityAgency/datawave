package datawave.ingest.mapreduce.partition;

import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.SplitsFileType;
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

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.TreeMap;

public class SortedTabletLocationPartitionerTest {
    
    String splitsFileName = SplitsFileType.SPLITSANDLOCATIONS + "splits.txt";
    
    private static final String TABLE_NAME = "abc";
    Configuration configuration;
    Job mockJob;
    
    @Before
    public void before() throws IOException {
        mockJob = new Job();
        configuration = mockJob.getConfiguration();
        configuration.set("job.table.names", TABLE_NAME);
    }
    
    private URL createUrl(String fileName) {
        
        return SortedTabletLocationPartitionerTest.class.getResource("/datawave/ingest/mapreduce/job/" + fileName);
    }
    
    private BulkIngestKey getBulkIngestKey(String rowStr) {
        return new BulkIngestKey(new Text(TABLE_NAME), new Key(new Text(rowStr)));
    }
    
    private void mockContextForLocalCacheFile(final URL url) {
        SortedTabletLocationPartitioner
                        .setContext(new MapContextImpl<Key,Value,Text,Mutation>(configuration, new TaskAttemptID(), null, null, null, null, null) {
                            @Override
                            public Path[] getLocalCacheFiles() throws IOException {
                                return new Path[] {new Path(url.getPath())};
                            }
                            
                        });
    }
    
    @Test
    public void testAllDataForOneSplitGoesToOnePartitioner() {
        mockContextForLocalCacheFile(createUrl(splitsFileName));
        int numPartitions = 581;
        
        SortedTabletLocationPartitioner partitioner = new SortedTabletLocationPartitioner();
        partitioner.setConf(new Configuration());
        
        // first split is a, last is z
        for (int i = 0; i < 26; i++) {
            String precedingRow = Character.toString((char) ("a".codePointAt(0) + i - 1)) + "_";
            int resultForPrecedingRow = partitioner.getPartition(getBulkIngestKey(precedingRow), new Value(), numPartitions);
            
            String rowStr = Character.toString((char) ("a".codePointAt(0) + i));
            int resultRow = partitioner.getPartition(getBulkIngestKey(rowStr), new Value(), numPartitions);
            
            Assert.assertEquals("These should have matched: resultRow: " + resultRow + " , resultForPrecedingRow: " + resultForPrecedingRow, resultRow,
                            resultForPrecedingRow);
        }
    }
    
    @Test
    public void testOneToOne() {
        mockContextForLocalCacheFile(createUrl(splitsFileName));
        int numPartitions = 8;
        
        SortedTabletLocationPartitioner partitioner = new SortedTabletLocationPartitioner();
        partitioner.setConf(new Configuration());
        
        Map<Integer,Integer> numberTimesPartitionSeen = new TreeMap<>();
        
        countPartitions(numberTimesPartitionSeen, numPartitions, partitioner);
        Assert.assertEquals(numPartitions, numberTimesPartitionSeen.size());
        
        int resultRow1 = partitioner.getPartition(getBulkIngestKey("a"), new Value(), numPartitions);
        int resultRow2 = partitioner.getPartition(getBulkIngestKey("p"), new Value(), numPartitions);
        Assert.assertEquals(resultRow1, resultRow2);
        
        int resultRow3 = partitioner.getPartition(getBulkIngestKey("b"), new Value(), numPartitions);
        int resultRow4 = partitioner.getPartition(getBulkIngestKey("w"), new Value(), numPartitions);
        Assert.assertEquals(resultRow3, resultRow4);
        
        Assert.assertNotEquals(resultRow1, resultRow3);
        
    }
    
    @Test
    public void testFewerPartitions() {
        mockContextForLocalCacheFile(createUrl(splitsFileName));
        int numPartitions = 4;
        
        SortedTabletLocationPartitioner partitioner = new SortedTabletLocationPartitioner();
        partitioner.setConf(new Configuration());
        
        Map<Integer,Integer> numberTimesPartitionSeen = new TreeMap<>();
        
        countPartitions(numberTimesPartitionSeen, numPartitions, partitioner);
        Assert.assertEquals(numPartitions, numberTimesPartitionSeen.size());
        
        // 3rd location
        int resultRow1 = partitioner.getPartition(getBulkIngestKey("a"), new Value(), numPartitions);
        int resultRow2 = partitioner.getPartition(getBulkIngestKey("p"), new Value(), numPartitions);
        Assert.assertEquals(resultRow1, resultRow2);
        
        // 7th location mod 4 = 3
        int resultRow = partitioner.getPartition(getBulkIngestKey("i"), new Value(), numPartitions);
        Assert.assertEquals(resultRow1, resultRow);
        
        int resultRow3 = partitioner.getPartition(getBulkIngestKey("b"), new Value(), numPartitions);
        int resultRow4 = partitioner.getPartition(getBulkIngestKey("w"), new Value(), numPartitions);
        Assert.assertEquals(resultRow3, resultRow4);
        
        Assert.assertNotEquals(resultRow1, resultRow3);
        
    }
    
    @Test
    public void testMorePartitions() {
        mockContextForLocalCacheFile(createUrl(splitsFileName));
        int numPartitions = 10;
        
        SortedTabletLocationPartitioner partitioner = new SortedTabletLocationPartitioner();
        partitioner.setConf(new Configuration());
        
        Map<Integer,Integer> numberTimesPartitionSeen = new TreeMap<>();
        
        // number of locations = 8
        countPartitions(numberTimesPartitionSeen, numPartitions, partitioner);
        Assert.assertEquals(8, numberTimesPartitionSeen.size());
        
        int resultRow1 = partitioner.getPartition(getBulkIngestKey("a"), new Value(), numPartitions);
        int resultRow2 = partitioner.getPartition(getBulkIngestKey("p"), new Value(), numPartitions);
        Assert.assertEquals(resultRow1, resultRow2);
        
        int resultRow3 = partitioner.getPartition(getBulkIngestKey("b"), new Value(), numPartitions);
        int resultRow4 = partitioner.getPartition(getBulkIngestKey("w"), new Value(), numPartitions);
        Assert.assertEquals(resultRow3, resultRow4);
        
        Assert.assertNotEquals(resultRow1, resultRow3);
        
    }
    
    public void countPartitions(Map<Integer,Integer> timesSeenOrderedByPartition, int numPartitions, MultiTableRangePartitioner partitioner) {
        // first split is a, last is z
        for (int i = 0; i < 26; i++) {
            String rowStr = Character.toString((char) ("a".codePointAt(0) + i));
            int resultRow = partitioner.getPartition(getBulkIngestKey(rowStr), new Value(), numPartitions);
            updateCounter(timesSeenOrderedByPartition, resultRow);
        }
        
    }
    
    private static void updateCounter(Map<Integer,Integer> numberTimesPartitionSeen, int partition) {
        Integer timesSeen = numberTimesPartitionSeen.get(partition);
        if (null == timesSeen) {
            timesSeen = 0;
        }
        numberTimesPartitionSeen.put(partition, timesSeen + 1);
    }
    
}
