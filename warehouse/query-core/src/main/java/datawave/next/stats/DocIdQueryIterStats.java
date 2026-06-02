package datawave.next.stats;

import java.io.Serializable;
import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * Tracks timing information for the {@link datawave.next.DocIdQueryIterator} and also tracks how many candidates were found.
 */
public class DocIdQueryIterStats implements KryoSerializable, Serializable {

    private static final long serialVersionUID = -2139467589556624363L;

    // totals are serialized
    private long totalDocumentIds = 0L;
    private long totalInitTime = 0L;
    private long totalScanTime = 0L;
    private long totalRetrievalTime = 0L;
    private long totalElapsedTime = 0L;

    // supporting variables used to calculate totals, values are not serialized
    private long scanInitMS = 0L;
    private long retrievalStartMS = 0L;

    public void incrementTotalDocumentIds(long totalDocumentIds) {
        this.totalDocumentIds += totalDocumentIds;
    }

    public long getTotalDocumentIds() {
        return totalDocumentIds;
    }

    public void markScanInit(long ms) {
        this.scanInitMS = ms;
    }

    public void markScanStart(long ms) {
        this.totalInitTime = ms - scanInitMS;
    }

    public void markScanStop(long ms) {
        this.totalScanTime = ms - scanInitMS;
    }

    public void markRetrievalStart(long ms) {
        this.retrievalStartMS = ms;
    }

    public void markRetrievalStop(long ms) {
        this.totalRetrievalTime = ms - retrievalStartMS;
        this.totalElapsedTime = ms - scanInitMS;
    }

    public long getTotalInitTime() {
        return totalInitTime;
    }

    public long getTotalScanTime() {
        return totalScanTime;
    }

    public long getTotalRetrievalTime() {
        return totalRetrievalTime;
    }

    public long getTotalElapsedTime() {
        return totalElapsedTime;
    }

    /**
     * Merge {@link DocIdQueryIterStats}
     *
     * @param other
     *            another instance of doc id query iterator stats
     */
    public void merge(DocIdQueryIterStats other) {
        // merge totals
        this.totalDocumentIds += other.totalDocumentIds;
        this.totalInitTime += other.totalInitTime;
        this.totalScanTime += other.totalScanTime;
        this.totalRetrievalTime += other.totalRetrievalTime;
        this.totalElapsedTime += other.totalElapsedTime;

        // skip supporting variables used to calculate totals
    }

    public static DocIdQueryIterStats fromString(String data) {
        String[] fields = data.split(",");
        Preconditions.checkArgument(fields.length == 5);

        DocIdQueryIterStats stats = new DocIdQueryIterStats();
        stats.totalDocumentIds = Long.parseLong(fields[0]);
        stats.totalInitTime = Long.parseLong(fields[1]);
        stats.totalScanTime = Long.parseLong(fields[2]);
        stats.totalRetrievalTime = Long.parseLong(fields[3]);
        stats.totalElapsedTime = Long.parseLong(fields[4]);
        return stats;
    }

    @Override
    public String toString() {
        List<Long> values = List.of(totalDocumentIds, getTotalInitTime(), getTotalScanTime(), getTotalRetrievalTime(), getTotalElapsedTime());
        return Joiner.on(',').join(values);
    }

    @Override
    public void write(Kryo kryo, Output output) {
        // important variables
        output.writeLong(totalDocumentIds, true);
        output.writeLong(totalInitTime, true);
        output.writeLong(totalScanTime, true);
        output.writeLong(totalRetrievalTime, true);
        output.writeLong(totalElapsedTime, true);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        // important variables
        this.totalDocumentIds = input.readLong(true);
        this.totalInitTime = input.readLong(true);
        this.totalScanTime = input.readLong(true);
        this.totalRetrievalTime = input.readLong(true);
        this.totalElapsedTime = input.readLong(true);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof DocIdQueryIterStats) {
            DocIdQueryIterStats other = (DocIdQueryIterStats) o;
            //  @formatter:off
            return new EqualsBuilder()
                    //  main stats
                    .append(totalDocumentIds, other.totalDocumentIds)
                    .append(totalInitTime, other.totalInitTime)
                    .append(totalScanTime, other.totalScanTime)
                    .append(totalRetrievalTime, other.totalRetrievalTime)
                    .append(totalElapsedTime, other.totalElapsedTime)
                    .isEquals();
            //  @formatter:on
        }
        return false;
    }

    @Override
    public int hashCode() {
        //  @formatter:off
        return new HashCodeBuilder(17, 37)
                //  main stats
                .append(totalDocumentIds)
                .append(totalInitTime)
                .append(totalScanTime)
                .append(totalRetrievalTime)
                .append(totalElapsedTime)
                .toHashCode();
        //  @formatter:on
    }
}
