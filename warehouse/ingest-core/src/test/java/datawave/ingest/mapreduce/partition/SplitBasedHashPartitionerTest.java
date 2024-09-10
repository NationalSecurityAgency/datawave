package datawave.ingest.mapreduce.partition;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskInputOutputContextImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.TableSplitsCache;

public class SplitBasedHashPartitionerTest {
    private static final String TEST_FILE_LOCATION = "datawave/ingest/mapreduce/job/full_splits.txt";
    private static final String TABLE_NAME = "someTableName"; // matches entry in test file: full_splits.txt
    Configuration conf = new Configuration();

    @Before
    public void before() {
        final String testFilePath = SplitBasedHashPartitionerTest.class.getClassLoader().getResource(TEST_FILE_LOCATION).getPath();

        conf.setBoolean(TableSplitsCache.REFRESH_SPLITS, false);
        conf.set(TableSplitsCache.SPLITS_CACHE_DIR, testFilePath.substring(0, testFilePath.lastIndexOf('/')));
        conf.set(TableSplitsCache.SPLITS_CACHE_FILE, "full_splits.txt");

        TableSplitsCache.getCurrentCache(conf).clear();

        MultiTableRangePartitioner.context = getTaskInputOutputContext(testFilePath, conf);

    }

    @Test
    public void testMultiplier() throws IllegalAccessException, InstantiationException, InvocationTargetException, NoSuchMethodException {
        int multiplier = 3;
        int numReducers = 1000;

        conf.setInt(SplitBasedHashPartitioner.PARTITIONER_SPACE_MULTIPLIER, multiplier);
        verifyPartitionRangeWithNoWrap(multiplier, numReducers, createPartitioner(conf));
    }

    @Test
    public void testConfOverrideForTable() throws IllegalAccessException, InstantiationException, InvocationTargetException, NoSuchMethodException {
        int multiplier = 3;
        int numReducers = 1000;

        // configure generic multiplier to be a bit low which if used would make the test fail
        conf.setInt(SplitBasedHashPartitioner.PARTITIONER_SPACE_MULTIPLIER, multiplier - 1);
        conf.setInt(TABLE_NAME + '.' + SplitBasedHashPartitioner.PARTITIONER_SPACE_MULTIPLIER, multiplier);
        verifyPartitionRangeWithNoWrap(multiplier, numReducers, createPartitioner(conf));
    }

    @Test
    public void testDefaultMultiplier() throws IllegalAccessException, InstantiationException, InvocationTargetException, NoSuchMethodException {
        int multiplier = 2;
        int numReducers = 1000;

        // deliberately removed multiplier from configuration
        verifyPartitionRangeWithNoWrap(multiplier, numReducers, createPartitioner(conf));
    }

    @Test
    public void testMultiplierWrapping() throws IllegalAccessException, InstantiationException, InvocationTargetException, NoSuchMethodException {
        int multiplier = 2;
        int numReducers = 6;
        conf.setInt(SplitBasedHashPartitioner.PARTITIONER_SPACE_MULTIPLIER, multiplier);

        SplitBasedHashPartitioner partitioner = createPartitioner(conf);

        for (int i = 0; i < 1000; i++) {
            verifyPartitionInRange(partitioner.getPartition(createKeyFor("\u0007" + i), new Value(), numReducers), 0, multiplier);
        }
        for (int i = 0; i < 1000; i++) {
            // multiplier * expected index % num reducers => 2 * 4 % 6 = 2, 2 * 5 % 6 = 4
            verifyPartitionInRange(partitioner.getPartition(createKeyFor("\u0008" + i), new Value(), numReducers), 2, 4);
        }
    }

    @Test
    public void testMultiplierWrappingTableOverride() throws IllegalAccessException, InstantiationException, InvocationTargetException, NoSuchMethodException {
        int multiplier = 2;
        int numReducers = 6;
        conf.setInt(SplitBasedHashPartitioner.PARTITIONER_SPACE_MULTIPLIER, multiplier + 1);
        conf.setInt(TABLE_NAME + '.' + SplitBasedHashPartitioner.PARTITIONER_SPACE_MULTIPLIER, multiplier);
        SplitBasedHashPartitioner partitioner = createPartitioner(conf);

        for (int i = 0; i < 1000; i++) {
            verifyPartitionInRange(partitioner.getPartition(createKeyFor("\u0007" + i), new Value(), numReducers), 0, multiplier);
        }
        for (int i = 0; i < 1000; i++) {
            // multiplier * expected index % num reducers => 2 * 4 % 6 = 2, 2 * 5 % 6 = 4
            verifyPartitionInRange(partitioner.getPartition(createKeyFor("\u0008" + i), new Value(), numReducers), 2, 4);
        }
    }

    private void verifyPartitionRangeWithNoWrap(int multiplier, int numReducers, SplitBasedHashPartitioner partitioner) {
        for (int i = 0; i < 1000; i++) {
            verifyPartitionInRange(partitioner.getPartition(createKeyFor("\u0007" + i), new Value(), numReducers), 0, multiplier);
        }
        for (int i = 0; i < 1000; i++) {
            verifyPartitionInRange(partitioner.getPartition(createKeyFor("\u0008" + i), new Value(), numReducers), 4 * multiplier, 5 * multiplier);
        }
    }

    private SplitBasedHashPartitioner createPartitioner(Configuration conf)
                    throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        SplitBasedHashPartitioner partitioner = SplitBasedHashPartitioner.class.getDeclaredConstructor().newInstance();
        partitioner.setConf(conf);
        partitioner.configureWithPrefix(TABLE_NAME);
        return partitioner;
    }

    private void verifyPartitionInRange(int partition, int startRangeInclusive, int endRangeExclusive) {
        Assert.assertTrue("partition: " + partition, startRangeInclusive <= partition);
        Assert.assertTrue("partition: " + partition, partition < endRangeExclusive);
    }

    private BulkIngestKey createKeyFor(String row) {
        return new BulkIngestKey(new Text(TABLE_NAME), new Key(row));
    }

    private TaskInputOutputContextImpl getTaskInputOutputContext(final String testFilePath, final Configuration conf) {
        return new TaskInputOutputContextImpl(conf, new TaskAttemptID(), null, null, null) {
            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                throw new UnsupportedOperationException();
            }

            @Override
            public Object getCurrentKey() throws IOException, InterruptedException {
                throw new UnsupportedOperationException();
            }

            @Override
            public Object getCurrentValue() throws IOException, InterruptedException {
                throw new UnsupportedOperationException();
            }

            @Deprecated
            public Path[] getLocalCacheFiles() throws IOException {
                return new Path[] {new Path(testFilePath)};
            }
        };
    }
}
