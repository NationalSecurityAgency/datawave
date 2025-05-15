package datawave.next.stats;

import java.io.Serializable;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

public class DocumentIteratorStats implements Serializable {

    private static final long serialVersionUID = 7609796063950808927L;

    private long nextCount = 0;
    private long seekCount = 0;
    private long datatypeFilterMiss = 0;
    private long timeFilterMiss = 0;
    private long regexMiss = 0;

    public void incrementNextCount() {
        nextCount++;
    }

    public void incrementSeekCount() {
        seekCount++;
    }

    public void incrementDatatypeFilterMiss() {
        datatypeFilterMiss++;
    }

    public void incrementTimeFilterMiss() {
        timeFilterMiss++;
    }

    public void incrementRegexMiss() {
        regexMiss++;
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

    public void merge(DocumentIteratorStats other) {
        nextCount += other.nextCount;
        seekCount += other.seekCount;
        datatypeFilterMiss += other.datatypeFilterMiss;
        timeFilterMiss += other.timeFilterMiss;
        regexMiss += other.regexMiss;
    }

    public static DocumentIteratorStats fromString(String data) {
        String[] fields = data.split(",");
        Preconditions.checkArgument(fields.length == 5);

        DocumentIteratorStats stats = new DocumentIteratorStats();
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
}
