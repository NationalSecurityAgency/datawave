package datawave.ingest.mapreduce.partition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.TreeMap;

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

import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.SplitsCache;
import datawave.ingest.mapreduce.job.SplitsConstants;
import datawave.ingest.mapreduce.job.TableSplitsCache;

public class SortedTabletLocationPartitionerTest {

    String splitsFileName = "SPLITSANDLOCATIONS" + "splits.txt";

    private static final String TABLE_NAME = "abc";
    Configuration configuration;
    Job mockJob;

    @BeforeEach
    public void before() throws IOException {
        mockJob = new Job();
        configuration = mockJob.getConfiguration();
        configuration.set("job.table.names", TABLE_NAME);
        configuration.setBoolean(TableSplitsCache.REFRESH_SPLITS, false);
        configuration.set(SplitsConstants.SPLITS_CACHE_DIR,
                        createUrl(splitsFileName).getPath().substring(0, createUrl(splitsFileName).getPath().lastIndexOf('/')));
        configuration.set(SplitsConstants.SPLITS_CACHE_FILE, splitsFileName);

        SplitsCache cache = SplitsCache.getInstance(configuration);
        TableSplitsCache.getCurrentCache(configuration);

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
        partitioner.setConf(configuration);

        // first split is a, last is z
        for (int i = 0; i < 26; i++) {
            String precedingRow = Character.toString((char) ("a".codePointAt(0) + i - 1)) + "_";
            int resultForPrecedingRow = partitioner.getPartition(getBulkIngestKey(precedingRow), new Value(), numPartitions);

            String rowStr = Character.toString((char) ("a".codePointAt(0) + i));
            int resultRow = partitioner.getPartition(getBulkIngestKey(rowStr), new Value(), numPartitions);

            assertEquals("These should have matched: resultRow: " + resultRow + " , resultForPrecedingRow: " + resultForPrecedingRow, resultRow,
                            resultForPrecedingRow);
        }
    }

    @Test
    public void testOneToOne() {
        mockContextForLocalCacheFile(createUrl(splitsFileName));
        int numPartitions = 8;

        SortedTabletLocationPartitioner partitioner = new SortedTabletLocationPartitioner();
        partitioner.setConf(configuration);

        Map<Integer,Integer> numberTimesPartitionSeen = new TreeMap<>();

        countPartitions(numberTimesPartitionSeen, numPartitions, partitioner);
        assertEquals(numPartitions, numberTimesPartitionSeen.size());

        int resultRow1 = partitioner.getPartition(getBulkIngestKey("a"), new Value(), numPartitions);
        int resultRow2 = partitioner.getPartition(getBulkIngestKey("p"), new Value(), numPartitions);
        assertEquals(resultRow1, resultRow2);

        int resultRow3 = partitioner.getPartition(getBulkIngestKey("b"), new Value(), numPartitions);
        int resultRow4 = partitioner.getPartition(getBulkIngestKey("w"), new Value(), numPartitions);
        assertEquals(resultRow3, resultRow4);

        assertNotEquals(resultRow1, resultRow3);

    }

    @Test
    public void testFewerPartitions() {
        mockContextForLocalCacheFile(createUrl(splitsFileName));
        int numPartitions = 4;

        SortedTabletLocationPartitioner partitioner = new SortedTabletLocationPartitioner();
        partitioner.setConf(configuration);

        Map<Integer,Integer> numberTimesPartitionSeen = new TreeMap<>();

        countPartitions(numberTimesPartitionSeen, numPartitions, partitioner);
        assertEquals(numPartitions, numberTimesPartitionSeen.size());

        assertEquals(7, numberTimesPartitionSeen.get(0).intValue());
        assertEquals(6, numberTimesPartitionSeen.get(1).intValue());
        assertEquals(7, numberTimesPartitionSeen.get(2).intValue());
        assertEquals(7, numberTimesPartitionSeen.get(3).intValue());

        int resultRow1 = partitioner.getPartition(getBulkIngestKey("a"), new Value(), numPartitions);
        int resultRow2 = partitioner.getPartition(getBulkIngestKey("p"), new Value(), numPartitions);
        assertEquals(resultRow1, resultRow2);

        int resultRow3 = partitioner.getPartition(getBulkIngestKey("j"), new Value(), numPartitions);
        int resultRow4 = partitioner.getPartition(getBulkIngestKey("z"), new Value(), numPartitions);
        assertEquals(resultRow3, resultRow4);

        assertNotEquals(resultRow1, resultRow4);

        int resultRow5 = partitioner.getPartition(getBulkIngestKey("z1"), new Value(), numPartitions);
        assertEquals(0, resultRow5);

    }

    @Test
    public void testMorePartitions() {
        mockContextForLocalCacheFile(createUrl(splitsFileName));
        int numPartitions = 10;

        SortedTabletLocationPartitioner partitioner = new SortedTabletLocationPartitioner();
        partitioner.setConf(configuration);

        Map<Integer,Integer> numberTimesPartitionSeen = new TreeMap<>();

        // number of locations = 9
        countPartitions(numberTimesPartitionSeen, numPartitions, partitioner);
        assertEquals(9, numberTimesPartitionSeen.size());

        int resultRow1 = partitioner.getPartition(getBulkIngestKey("a"), new Value(), numPartitions);
        int resultRow2 = partitioner.getPartition(getBulkIngestKey("p"), new Value(), numPartitions);
        assertEquals(resultRow1, resultRow2);

        int resultRow3 = partitioner.getPartition(getBulkIngestKey("b"), new Value(), numPartitions);
        int resultRow4 = partitioner.getPartition(getBulkIngestKey("w"), new Value(), numPartitions);
        assertEquals(resultRow3, resultRow4);

        assertNotEquals(resultRow1, resultRow3);

    }

    public void countPartitions(Map<Integer,Integer> timesSeenOrderedByPartition, int numPartitions, MultiTableRangePartitioner partitioner) {
        // first split is a, last is z1
        for (int i = 0; i < 27; i++) {
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
