package datawave.ingest.mapreduce.job;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;

/**
 * Counters for tracking the number of records inserted into bulk ingest tables. This class allows the user to register counters per table and then increment
 * the appropriate counter based on a {@link BulkIngestKey}. The default is to use a counter based solely on the table name (see
 * {@link BulkIngestKey#getTableName()}).
 */
public class BulkIngestCounters {
    private Map<Text,BulkIngestCounter> counters = new HashMap<>();
    private String shardedTableName;

    public BulkIngestCounters(Configuration conf) {
        shardedTableName = conf.get(ShardedDataTypeHandler.SHARD_TNAME, "");
    }

    /**
     * Creates a new counter for a table. Any {@link BulkIngestKey} passed to {@link #incrementCounter(BulkIngestKey)} or
     * {@link #incrementCounter(BulkIngestKey, int)} later will use the counter created here if the table name in the key matches the table name.
     *
     * @param tableName
     *            the table name
     * @param deleteMode
     *            deletion flag to set
     */
    public void createCounter(String tableName, boolean deleteMode) {
        if (shardedTableName.equals(tableName)) {
            counters.put(new Text(tableName), new ShardedTableCounter(tableName, deleteMode));
        } else {
            counters.put(new Text(tableName), new TableNameCounter(tableName, deleteMode));
        }
    }

    /**
     * Increments the appropriate counter for {@code key}.
     *
     * @param key
     *            the {@link BulkIngestKey} containing the information necessary to determine which counter to increment. In the case of a key for a sharded
     *            table, the column family will be used to determine the counter name (e.g., a column family starting with "e" will use the counter named
     *            Event).
     */
    public void incrementCounter(BulkIngestKey key) {
        BulkIngestCounter counter = counters.get(key.getTableName());
        if (counter != null)
            counter.incrementCounter(key);
    }

    /**
     * Increments the appropriate counter for {@code key}.
     *
     * @param key
     *            the {@link BulkIngestKey} containing the information necessary to determine which counter to increment. In the case of a key for a sharded
     *            table, the column family will be used to determine the counter name (e.g., a column family starting with "e" will use the counter named
     *            Event).
     * @param count
     *            the count to increment by
     */
    public void incrementCounter(BulkIngestKey key, int count) {
        BulkIngestCounter counter = counters.get(key.getTableName());
        if (counter != null)
            counter.incrementCounter(key, count);
    }

    /**
     * Flush the counters out the the provided context
     *
     * @param context
     *            the context provided
     */
    public void flush(TaskAttemptContext context) {
        for (BulkIngestCounter counter : counters.values()) {
            counter.flush(context);
        }
    }

    protected abstract static class BulkIngestCounter {
        protected String tableName;
        protected String counterGroup;

        public BulkIngestCounter(String tableName, boolean isDelete) {
            this.tableName = tableName;
            counterGroup = isDelete ? "CB Delete" : "CB Insert";
        }

        public abstract void incrementCounter(BulkIngestKey key);

        public abstract void incrementCounter(BulkIngestKey key, int count);

        public abstract void flush(TaskAttemptContext context);
    }

    protected static class TableNameCounter extends BulkIngestCounter {

        private long count = 0;

        public TableNameCounter(String tableName, boolean isDelete) {
            super(tableName, isDelete);
        }

        @Override
        public void incrementCounter(BulkIngestKey key) {
            this.count++;
        }

        @Override
        public void incrementCounter(BulkIngestKey key, int count) {
            this.count += count;
        }

        @Override
        public void flush(TaskAttemptContext context) {
            context.getCounter(counterGroup, tableName).increment(count);
            this.count = 0;
        }
    }

    protected static class ShardedTableCounter extends BulkIngestCounter {
        private String fieldIndex;
        private long count = 0;
        private long fiCount = 0;
        private static byte[] FI_CF = null;

        static {
            try {
                FI_CF = "fi\0".getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new IllegalStateException("UTF-8 encoding unknown", e);
            }
        }

        public ShardedTableCounter(String tableName, boolean isDelete) {
            super(tableName, isDelete);
            this.fieldIndex = tableName + "FieldIndex";
        }

        private void countFieldIndexKeys(BulkIngestKey key, int count) {
            ByteSequence cf = key.getKey().getColumnFamilyData();
            if (cf.length() >= FI_CF.length) {

                /************************************************************************/
                /* NOTE: */
                /* If the definition of the Family Index indicator changes then */
                /* the following optimization to the conditional needs to be */
                /* changed to match the new definition. */
                /************************************************************************/

                if ((FI_CF[0] == cf.byteAt(0)) && (FI_CF[1] == cf.byteAt(1)) && (FI_CF[2] == cf.byteAt(2))) {
                    this.fiCount += count;
                }
            }
        }

        @Override
        public void incrementCounter(BulkIngestKey key) {
            this.count++;
            countFieldIndexKeys(key, 1);
        }

        @Override
        public void incrementCounter(BulkIngestKey key, int count) {
            this.count += count;
            countFieldIndexKeys(key, count);
        }

        @Override
        public void flush(TaskAttemptContext context) {
            context.getCounter(counterGroup, tableName).increment(this.count);
            this.count = 0;
            context.getCounter(counterGroup, fieldIndex).increment(this.fiCount);
            this.fiCount = 0;
        }
    }
}
