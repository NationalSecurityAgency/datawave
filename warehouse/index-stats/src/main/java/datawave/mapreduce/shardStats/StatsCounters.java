package datawave.mapreduce.shardStats;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * POJO containing the statistical counts produced by the reducer for a field name/datatype pair. The data produced by this object are:
 * <ul>
 * <li>total count of the number of values</li>
 * <li>number of unique values</li>
 * <li>selectivity of the values</li>
 * </ul>
 */
public class StatsCounters implements WritableComparable<StatsCounters> {

    private static final int SELECTIVITY_MULTIPLIER = 100;

    private final VLongWritable count;
    private final VLongWritable uniqueCount;
    private final VIntWritable selectivity;

    // required for deserialization
    public StatsCounters() {
        this.count = new VLongWritable();
        this.uniqueCount = new VLongWritable();
        this.selectivity = new VIntWritable();
    }

    /**
     * Creates a shard stats for a field name/datatype pair.
     *
     * @param sumCount
     *            total number of values
     * @param unique
     *            total number of unique values
     */
    StatsCounters(long sumCount, long unique) {
        this.count = new VLongWritable(sumCount);
        int selVal;
        // hyperlog unique count could be greater than total count
        if (unique < sumCount) {
            this.uniqueCount = new VLongWritable(unique);
            selVal = (int) ((float) unique / (float) sumCount * SELECTIVITY_MULTIPLIER);
        } else {
            // use total count if unique is > total
            this.uniqueCount = new VLongWritable(sumCount);
            selVal = SELECTIVITY_MULTIPLIER;
        }
        this.selectivity = new VIntWritable(selVal);
    }

    public long getCount() {
        return count.get();
    }

    public long getUniqueCount() {
        return uniqueCount.get();
    }

    public int getSelectivity() {
        return selectivity.get();
    }

    public Value getValue() throws IOException {
        return new Value(toByteArray());
    }

    public byte[] toByteArray() throws IOException {
        try (final OutputStream baos = new ByteArrayOutputStream(); DataOutputStream dataOutput = new DataOutputStream(baos)) {
            write(dataOutput);
            return ((ByteArrayOutputStream) baos).toByteArray();
        }
    }

    @Override
    public int compareTo(StatsCounters o) {
        int result = this.count.compareTo(o.count);
        if (0 == result) {
            result = this.uniqueCount.compareTo(o.uniqueCount);
        }

        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.count.write(dataOutput);
        this.uniqueCount.write(dataOutput);
        this.selectivity.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.count.readFields(dataInput);
        this.uniqueCount.readFields(dataInput);
        this.selectivity.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        StatsCounters that = (StatsCounters) o;
        return Objects.equals(count, that.count) && Objects.equals(uniqueCount, that.uniqueCount) && Objects.equals(selectivity, that.selectivity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, uniqueCount, selectivity);
    }

    @Override
    public String toString() {
        // @formatter:off
        return "StatsCounters{" +
                "count=" + count +
                ", uniqueCount=" + uniqueCount +
                ", selectivity=" + selectivity +
                '}';
        // @formatter:on
    }
}
