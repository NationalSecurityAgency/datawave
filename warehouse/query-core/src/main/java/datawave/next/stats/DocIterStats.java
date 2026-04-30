package datawave.next.stats;

import java.io.Serializable;
import java.util.List;

import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * Provide basic stats for the following iterators
 * <ul>
 * <li>{@link datawave.next.DocIdIterator}</li>
 * <li>{@link datawave.next.RangeDocIdIterator}</li>
 * <li>{@link datawave.next.RegexDocIdIterator}</li>
 * <li>{@link datawave.next.ListDocIdIterator}</li>
 * </ul>
 * Per iterator stats are merged into a single stats instance and returned to the client.
 */
public class DocIterStats implements KryoSerializable, Serializable {

    private static final long serialVersionUID = 7609796063950808927L;

    private long nextCount = 0L;
    private long seekCount = 0L;
    private long datatypeFilterMiss = 0L;
    private long timeFilterMiss = 0L;
    private long regexMiss = 0L;

    public void incrementNextCount() {
        nextCount++;
    }

    public void incrementNextCount(long count) {
        nextCount += count;
    }

    public void incrementSeekCount() {
        seekCount++;
    }

    public void incrementSeekCount(long count) {
        seekCount += count;
    }

    public void incrementDatatypeFilterMiss() {
        datatypeFilterMiss++;
    }

    public void incrementDatatypeFilterMiss(long count) {
        datatypeFilterMiss += count;
    }

    public void incrementTimeFilterMiss() {
        timeFilterMiss++;
    }

    public void incrementTimeFilterMiss(long count) {
        timeFilterMiss += count;
    }

    public void incrementRegexMiss() {
        regexMiss++;
    }

    public void incrementRegexMiss(long count) {
        regexMiss += count;
    }

    public long getNextCount() {
        return nextCount;
    }

    public long getSeekCount() {
        return seekCount;
    }

    public long getDatatypeFilterMiss() {
        return datatypeFilterMiss;
    }

    public long getTimeFilterMiss() {
        return timeFilterMiss;
    }

    public long getRegexMiss() {
        return regexMiss;
    }

    public void merge(DocIterStats other) {
        nextCount += other.nextCount;
        seekCount += other.seekCount;
        datatypeFilterMiss += other.datatypeFilterMiss;
        timeFilterMiss += other.timeFilterMiss;
        regexMiss += other.regexMiss;
    }

    public static DocIterStats fromString(String data) {
        String[] fields = data.split(",");
        Preconditions.checkArgument(fields.length == 5);

        DocIterStats stats = new DocIterStats();
        stats.nextCount = Long.parseLong(fields[0]);
        stats.seekCount = Long.parseLong(fields[1]);
        stats.datatypeFilterMiss = Long.parseLong(fields[2]);
        stats.timeFilterMiss = Long.parseLong(fields[3]);
        stats.regexMiss = Long.parseLong(fields[4]);
        return stats;
    }

    /**
     * Always present the next and seek counts. Conditionally present the datatype, time, and regex miss counts.
     *
     * @return a string representation of the iterator status
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        List<Long> values = List.of(nextCount, seekCount, datatypeFilterMiss, timeFilterMiss, regexMiss);
        sb.append(Joiner.on(',').join(values));

        return sb.toString();
    }

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeLong(nextCount, true);
        output.writeLong(seekCount, true);
        output.writeLong(datatypeFilterMiss, true);
        output.writeLong(timeFilterMiss, true);
        output.writeLong(regexMiss, true);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        nextCount = input.readLong(true);
        seekCount = input.readLong(true);
        datatypeFilterMiss = input.readLong(true);
        timeFilterMiss = input.readLong(true);
        regexMiss = input.readLong(true);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof DocIterStats) {
            DocIterStats other = (DocIterStats) o;
            return nextCount == other.nextCount && seekCount == other.seekCount && datatypeFilterMiss == other.datatypeFilterMiss
                            && timeFilterMiss == other.timeFilterMiss && regexMiss == other.regexMiss;
        }
        return false;
    }

    @Override
    public int hashCode() {
        //  @formatter:off
        return new HashCodeBuilder(17, 37)
                .append(nextCount)
                .append(seekCount)
                .append(datatypeFilterMiss)
                .append(timeFilterMiss)
                .append(regexMiss)
                .toHashCode();
        //  @formatter:on
    }
}
