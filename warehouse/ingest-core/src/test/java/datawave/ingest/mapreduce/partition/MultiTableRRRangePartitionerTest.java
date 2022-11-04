/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package datawave.ingest.mapreduce.partition;

import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.util.TableName;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MultiTableRRRangePartitionerTest {
    
    /**
     * Test of calculateIndex method, of class MultiTableRRRangePartitioner. Index is the value returned from a binary search of the cutPointArray in the
     * MultiTableRangePartitioner. Asserts that an index that is equal to the cut point and index that is less than that cut point but greater than the previous
     * one fall into the same bin. Asserts that for a given index, the result is as expected. Asserts that an item that is less than all or greater than all of
     * the cut points is assigned a bin
     */
    @Test
    public void testCalculateIndex() {
        int index = -9;
        int indexTwo = 8;
        int indexThree = -1;
        int indexFour = -15;
        int expectedResult = 2;
        int numPartitions = 5;
        String tableName = "test";
        int cutPointArrayLength = 10;
        MultiTableRRRangePartitioner instance = new MultiTableRRRangePartitioner();
        int result = instance.calculateIndex(index, numPartitions, tableName, cutPointArrayLength);
        int resultTwo = instance.calculateIndex(indexTwo, numPartitions, tableName, cutPointArrayLength);
        int resultThree = instance.calculateIndex(indexThree, numPartitions, tableName, cutPointArrayLength);
        int resultFour = instance.calculateIndex(indexFour, numPartitions, tableName, cutPointArrayLength);
        assertEquals(result, resultTwo);
        assertEquals(result, expectedResult);
        assertNotNull(resultThree);
        assertNotNull(resultFour);
    }
    
    private static final String TABLE_NAME = "abc";
    Configuration configuration;
    Job mockJob;
    
    @BeforeEach
    public void before() throws IOException {
        mockJob = new Job();
        configuration = mockJob.getConfiguration();
        configuration.set("job.output.table.names", TableName.SHARD);
    }
    
    @Test
    public void testEmptySplitsThrowsException() throws IOException, URISyntaxException {
        mockContextForLocalCacheFile(createUrl("full_empty_splits.txt"));
        assertThrows(RuntimeException.class, () -> getPartition("23432"));
    }
    
    @Test
    public void testProblemGettingLocalCacheFiles() throws IOException, URISyntaxException {
        final URL url = createUrl("full_splits.txt");
        
        MultiTableRangePartitioner.setContext(new MapContextImpl<Key,Value,Text,Mutation>(configuration, new TaskAttemptID(), null, null, null, null, null) {
            @Override
            public org.apache.hadoop.fs.Path[] getLocalCacheFiles() throws IOException {
                throw new IOException("Local cache files failure");
            }
        });
        
        assertThrows(RuntimeException.class, () -> getPartition("23432"));
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
    
    private int getPartition(String row) {
        MultiTableRRRangePartitioner partitioner = new MultiTableRRRangePartitioner();
        partitioner.setConf(new Configuration());
        return partitioner.getPartition(new BulkIngestKey(new Text(TABLE_NAME), new Key(row)), new Value("fdsafdsa".getBytes()), 100);
    }
    
    @Test
    public void testAllDataForOneSplitGoesToOnePartitioner() {
        mockContextForLocalCacheFile(createUrl("full_splits.txt"));
        int numPartitions = 581;
        
        MultiTableRRRangePartitioner partitioner = new MultiTableRRRangePartitioner();
        partitioner.setConf(new Configuration());
        
        // first split is a, last is z
        for (int i = 0; i < 26; i++) {
            String precedingRow = Character.toString((char) ("a".codePointAt(0) + i - 1)) + "_";
            int resultForPrecedingRow = partitioner.getPartition(getBulkIngestKey(precedingRow), new Value(), numPartitions);
            
            String rowStr = Character.toString((char) ("a".codePointAt(0) + i));
            int resultRow = partitioner.getPartition(getBulkIngestKey(rowStr), new Value(), numPartitions);
            
            assertEquals(resultRow, resultForPrecedingRow, "These should have matched: resultRow: " + resultRow + " , resultForPrecedingRow: "
                            + resultForPrecedingRow);
        }
    }
    
    @Test
    public void testCalculateIndexRangeIsValid() {
        int numPartitions = 581;
        int cutPointArrayLength = 195;
        
        MultiTableRRRangePartitioner partitioner = new MultiTableRRRangePartitioner();
        for (int i = -1 * cutPointArrayLength - 1; i < cutPointArrayLength; i++) {
            int result = partitioner.calculateIndex(i, numPartitions, "someTableName", cutPointArrayLength);
            assertTrue(0 <= result, "i: " + i + " result: " + result);
            assertTrue(result < numPartitions, "i: " + i + " result: " + result);
        }
    }
    
    @Test
    public void testPartitionerSpaceIsValid() {
        mockContextForLocalCacheFile(createUrl("full_splits.txt"));
        int numPartitions = 581;
        
        MultiTableRRRangePartitioner partitioner = new MultiTableRRRangePartitioner();
        partitioner.setConf(new Configuration());
        
        // first split is a, last is z
        int numSplits = 26;
        for (int i = 0; i < numSplits; i++) {
            String rowStr = Character.toString((char) ("a".codePointAt(0) + i));
            int result = partitioner.getPartition(getBulkIngestKey(rowStr), new Value(), numPartitions);
            assertTrue(numPartitions - numSplits - 1 <= result, "rowStr: " + rowStr + " partition: " + result);
            assertTrue(result < numPartitions, "rowStr: " + rowStr + " partition: " + result);
        }
        
        // test rows before and after each split
        for (int i = -1; i < numSplits + 1; i++) {
            int result = partitioner.getPartition(getBulkIngestKey(Character.toString((char) ("a".codePointAt(0) + i)) + "_"), new Value(), numPartitions);
            assertTrue(numPartitions - numSplits - 1 <= result, "i: " + i + " partition: " + result);
            assertTrue(result < numPartitions, "i: " + i + " partition: " + result);
        }
    }
    
    @Test
    public void testEvenDistributionWithExtraReducers() {
        Map<Integer,Integer> numberTimesPartitionSeen = new TreeMap<>();
        MultiTableRRRangePartitioner partitioner = createPartitionerFromSplits();
        int numPartitions = 581;
        
        // first split is a, last is z
        countPartitions(numberTimesPartitionSeen, numPartitions, partitioner);
        
        assertEquals(27, numberTimesPartitionSeen.size(),
                        "Should have seen a total of 27 different partitions.  There is a split for each letter of the alphabet and the null split which is not in the file");
        for (Map.Entry<Integer,Integer> partitionAndNumSeen : numberTimesPartitionSeen.entrySet()) {
            assertEquals(2, partitionAndNumSeen.getValue().intValue(), "We haven't used the partition space so they should all be even, but partition "
                            + partitionAndNumSeen.getKey().intValue() + " did not see 2.");
        }
    }
    
    @Test
    public void testEvenDistributionWithFewerReducers() {
        Map<Integer,Integer> numberTimesPartitionSeen = new TreeMap<>();
        MultiTableRRRangePartitioner partitioner = createPartitionerFromSplits();
        int numPartitions = 10;
        
        // first split is a, last is z
        countPartitions(numberTimesPartitionSeen, numPartitions, partitioner);
        
        assertEquals(10, numberTimesPartitionSeen.size(), "Should have seen a total of 10 different partitions given the small reducer space");
        System.out.println(numberTimesPartitionSeen);
        // we partitioned 27 splits
        // over a space of 10 partitioners
        // so each partitioners should have 2 splits or 3 splits assigned to it
        // we partitioned two rows per split, so each partition should have seen 4 or 6 rows
        for (Map.Entry<Integer,Integer> partitionAndNumSeen : numberTimesPartitionSeen.entrySet()) {
            assertTrue(4 <= partitionAndNumSeen.getValue().intValue(), partitionAndNumSeen.toString());
            assertTrue(partitionAndNumSeen.getValue().intValue() <= 6, partitionAndNumSeen.toString());
        }
    }
    
    @Test
    public void testPartitionsInNonContiguousWay() {
        MultiTableRRRangePartitioner partitioner = createPartitionerFromSplits();
        int numPartitions = 10;
        
        HashSet<Integer> partitionsFound = new HashSet<>();
        // a - j go to different partitions
        for (int i = 0; i < numPartitions; i++) {
            String row = Character.toString((char) ("a".codePointAt(0) + i));
            partitionsFound.add(partitioner.getPartition(getBulkIngestKey(row), new Value(), numPartitions));
        }
        assertEquals(10, partitionsFound.size());
        
        // k - t go to different partitions
        partitionsFound.clear();
        for (int i = numPartitions; i < 2 * numPartitions; i++) {
            String row = Character.toString((char) ("a".codePointAt(0) + i));
            partitionsFound.add(partitioner.getPartition(getBulkIngestKey(row), new Value(), numPartitions));
        }
        assertEquals(10, partitionsFound.size());
    }
    
    @Test
    public void testOverlapsAtLastPartitions() {
        Map<Integer,Integer> numberTimesPartitionSeen = new TreeMap<>();
        MultiTableRRRangePartitioner partitioner = createPartitionerFromSplits();
        int numPartitions = 10;
        
        // first split is a, last is z
        countPartitions(numberTimesPartitionSeen, numPartitions, partitioner);
        
        int previousCount = 0;
        for (Map.Entry<Integer,Integer> partitionAndNumSeen : numberTimesPartitionSeen.entrySet()) {
            int currentCount = partitionAndNumSeen.getValue().intValue();
            assertTrue(previousCount <= currentCount, partitionAndNumSeen.toString());
            previousCount = currentCount;
        }
    }
    
    private MultiTableRRRangePartitioner createPartitionerFromSplits() {
        mockContextForLocalCacheFile(createUrl("full_splits.txt"));
        MultiTableRRRangePartitioner partitioner = new MultiTableRRRangePartitioner();
        partitioner.setConf(new Configuration());
        return partitioner;
    }
    
    private void countPartitions(Map<Integer,Integer> timesSeenOrderedByPartition, int numPartitions, MultiTableRRRangePartitioner partitioner) {
        // first split is a, last is z
        for (int i = 0; i < 26; i++) {
            String precedingRow = Character.toString((char) ("a".codePointAt(0) + i - 1)) + "_";
            int resultForPrecedingRow = partitioner.getPartition(getBulkIngestKey(precedingRow), new Value(), numPartitions);
            updateCounter(timesSeenOrderedByPartition, resultForPrecedingRow);
            
            String rowStr = Character.toString((char) ("a".codePointAt(0) + i));
            int resultRow = partitioner.getPartition(getBulkIngestKey(rowStr), new Value(), numPartitions);
            updateCounter(timesSeenOrderedByPartition, resultRow);
        }
        
        // also check 2 partitions after last split, to be fair
        int resultRow = partitioner.getPartition(getBulkIngestKey("za"), new Value(), numPartitions);
        updateCounter(timesSeenOrderedByPartition, resultRow);
        resultRow = partitioner.getPartition(getBulkIngestKey("zz"), new Value(), numPartitions);
        updateCounter(timesSeenOrderedByPartition, resultRow);
    }
    
    private void updateCounter(Map<Integer,Integer> numberTimesPartitionSeen, int partition) {
        Integer timesSeen = numberTimesPartitionSeen.get(partition);
        if (null == timesSeen) {
            timesSeen = 0;
        }
        numberTimesPartitionSeen.put(partition, timesSeen + 1);
    }
    
    private BulkIngestKey getBulkIngestKey(String rowStr) {
        return new BulkIngestKey(new Text(TABLE_NAME), new Key(new Text(rowStr)));
    }
}
