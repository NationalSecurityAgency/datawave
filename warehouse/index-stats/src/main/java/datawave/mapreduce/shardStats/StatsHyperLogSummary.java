package datawave.mapreduce.shardStats;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;

/**
 * POJO that contains the mapper summation of the statistical counts for each field name/dataype pair. The data consists of the following:
 * <ul>
 * <li>total count of all entries</li>
 * <li>{@link HyperLogLogPlus} object that represents all of the entries</li>
 * <li>actual unique count for analysis debug purposes</li>
 * </ul>
 */
class StatsHyperLogSummary implements WritableComparable<StatsHyperLogSummary> {

    private static final Logger log = Logger.getLogger(StatsHyperLogSummary.class);

    /**
     * Total count of all entries for field name/datatype pair.
     */
    private final VLongWritable count;
    /**
     * Serialized {@link HyperLogLogPlus} object.
     */
    private final BytesWritable hyperLog;
    /**
     * Used for analysis purposes on when the actual unique count is requested.
     */
    private final VIntWritable uniqueCount;

    // required for deserialization
    StatsHyperLogSummary() {
        this.count = new VLongWritable();
        this.hyperLog = new BytesWritable();
        this.uniqueCount = new VIntWritable();
    }

    /**
     * Creates a stats HyperLog summary object.
     *
     * @param sumCount
     *            total number of field name/dataype pair entries
     * @param logPlus
     *            populated hyperlog object
     * @param uniqueCount
     *            actual count of unique values (debug only)
     * @throws IOException
     *             serialization error
     */
    StatsHyperLogSummary(long sumCount, HyperLogLogPlus logPlus, int uniqueCount) throws IOException {
        this.count = new VLongWritable(sumCount);
        this.hyperLog = new BytesWritable(logPlus.getBytes());
        this.uniqueCount = new VIntWritable(uniqueCount);
    }

    StatsHyperLogSummary(final Value value) throws IOException {
        this();
        readFields(value.get());
    }

    public long getCount() {
        return count.get();
    }

    public HyperLogLogPlus getHyperLogPlus() throws IOException {
        byte[] buf = this.hyperLog.getBytes();
        return HyperLogLogPlus.Builder.build(buf);
    }

    public int getUniqueCount() {
        return uniqueCount.get();
    }

    public byte[] toByteArray() throws IOException {
        try (final OutputStream baos = new ByteArrayOutputStream()) {
            DataOutputStream dataOutput = new DataOutputStream(baos);
            write(dataOutput);
            return ((ByteArrayOutputStream) baos).toByteArray();
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.count.write(dataOutput);
        this.hyperLog.write(dataOutput);
        this.uniqueCount.write(dataOutput);
    }

    void readFields(final byte[] bytes) throws IOException {
        try (InputStream bis = new ByteArrayInputStream(bytes)) {
            DataInput is = new DataInputStream(bis);
            readFields(is);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.count.readFields(dataInput);
        this.hyperLog.readFields(dataInput);
        this.uniqueCount.readFields(dataInput);
    }

    @Override
    public int compareTo(StatsHyperLogSummary o) {
        int result = this.count.compareTo(o.count);
        if (0 == result) {
            result = this.hyperLog.compareTo(o.hyperLog);
        }

        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        StatsHyperLogSummary that = (StatsHyperLogSummary) o;
        return Objects.equals(count, that.count) && Objects.equals(hyperLog, that.hyperLog);
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, hyperLog);
    }

    /**
     * Used for debug analysis of the mapper data. The output string contains:
     * <ul>
     * <li>total count of field name/datatype pair</li>
     * <li>hperlog cardinality</li>
     * <li>hyperlog computed selectivity (0 - 1000)</li>
     * <li>actual cardinality</li>
     * <li>actual selectivity (0 - 1000)</li>
     * <li>difference between hyperlog and actual cardinality</li>
     * <li>error ration between hyperlog and actual cardinality</li>
     * </ul>
     *
     * @return analysis string for current state of object
     */
    String statsString() {
        try {
            long card = getHyperLogPlus().cardinality();
            int cardSel = (int) ((float) card / (float) this.count.get() * 1000.0);
            int actualSel = (int) ((float) uniqueCount.get() / (float) this.count.get() * 1000.0);
            long diff = Math.abs(card - this.uniqueCount.get());
            double err = (float) diff / (float) this.uniqueCount.get() * 100.0;
            // @formatter:off
            return "StatsHyperLogSummary{" +
                    "count(" + count.get() +
                    ") hyperLog cardinality(" + card +
                    ") hyperLog select(" + cardSel +
                    ") unique(" + uniqueCount +
                    ") actual select(" + actualSel +
                    ") diff(" + diff +
                    ") err(" + err + ")" +
                    '}';
            // @formatter:on
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    @Override
    public String toString() {
        try {
            long card = getHyperLogPlus().cardinality();
            // @formatter:off
            return "StatsHyperLogSummary{" +
                    "count=" + count +
                    ", hyperLog cardinality=" + card +
                    ", uniqueCount=" + uniqueCount +
                    '}';
            // @formatter:on
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }
}
