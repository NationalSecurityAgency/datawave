package datawave.mapreduce.shardStats;

import java.io.IOException;
import java.util.Objects;

import org.apache.accumulo.core.data.Value;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;

/**
 * POJO for summary data for each field name/datatype pair processed by the reducer.
 */
class HyperLogFieldSummary implements FieldSummary {

    private long count;
    private final HyperLogLogPlus logPlus;

    HyperLogFieldSummary(HyperLogLogPlus hllp) {
        this.logPlus = hllp;
    }

    @Override
    public long getCount() {
        return this.count;
    }

    @Override
    public void add(Value value) throws IOException {
        StatsHyperLogSummary stats = new StatsHyperLogSummary(value);
        this.count += stats.getCount();
        HyperLogLogPlus hllpAdd = stats.getHyperLogPlus();
        try {
            this.logPlus.addAll(hllpAdd);
        } catch (CardinalityMergeException e) {
            // addAll throws an out of scope exception
            throw new IOException(e);
        }
    }

    @Override
    public StatsCounters toStatsCounters() {
        return new StatsCounters(this.count, this.logPlus.cardinality());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        HyperLogFieldSummary that = (HyperLogFieldSummary) o;
        return count == that.count && Objects.equals(logPlus, that.logPlus);
    }

    @Override
    public int hashCode() {

        return Objects.hash(count, logPlus);
    }

    @Override
    public String toString() {
        // @formatter:off
        return "HyperLogFieldSummary{" +
                "count=" + count +
                ", logPlus=" + logPlus.cardinality() +
                '}';
        // @formatter:on
    }
}
