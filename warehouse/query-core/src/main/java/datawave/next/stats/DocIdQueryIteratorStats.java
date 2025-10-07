package datawave.next.stats;

import java.io.Serializable;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * Holds stats for document iterators by extending {@link DocumentIteratorStats} and also tracks total wall clock time and how many results were found
 */
public class DocIdQueryIteratorStats implements Serializable {

    private static final long serialVersionUID = -2139467589556624363L;

    // these four variables are the important ones
    private long totalDocumentIds = 0L;
    private long initTime = 0L;
    private long scanTime = 0L;
    private long retrievalTime = 0L;
    private long elapsedTime = 0L;

    // supporting variables
    private long scanInitNanos = 0L;
    private long scanStartNanos = 0L;
    private long scanStopNanos = 0L;
    private long retrievalStartNanos = 0L;
    private long retrievalStopNanos = 0L;

    public void incrementTotalDocumentIds(long totalDocumentIds) {
        this.totalDocumentIds += totalDocumentIds;
    }

    public long getTotalDocumentIds() {
        return totalDocumentIds;
    }

    public void markScanInit() {
        this.scanInitNanos = System.nanoTime();
    }

    public void markScanStart() {
        this.scanStartNanos = System.nanoTime();
    }

    public void markScanStop() {
        this.scanStopNanos = System.nanoTime();
    }

    public void markRetrievalStart() {
        this.retrievalStartNanos = System.nanoTime();
    }

    public void markRetrievalStop() {
        this.retrievalStopNanos = System.nanoTime();
    }

    public long getInitTime() {
        if (initTime == 0L) {
            initTime = scanStartNanos - scanInitNanos;
        }
        return initTime;
    }

    public long getScanTime() {
        if (scanTime == 0L) {
            scanTime = scanStopNanos - scanInitNanos;
        }
        return scanTime;
    }

    public long getRetrievalTime() {
        if (retrievalTime == 0L) {
            retrievalTime = retrievalStopNanos - retrievalStartNanos;
        }
        return retrievalTime;
    }

    public long getTotalElapsed() {
        if (elapsedTime == 0L) {
            elapsedTime = retrievalStopNanos - scanInitNanos;
        }
        return elapsedTime;
    }

    public void merge(DocIdQueryIteratorStats other) {
        this.totalDocumentIds += other.totalDocumentIds;
        this.initTime += other.initTime;
        this.scanTime += other.scanTime;
        this.retrievalTime += other.retrievalTime;
        this.elapsedTime += other.elapsedTime;
    }

    public static DocIdQueryIteratorStats fromString(String data) {
        String[] fields = data.split(",");
        Preconditions.checkArgument(fields.length == 5);

        DocIdQueryIteratorStats stats = new DocIdQueryIteratorStats();
        stats.totalDocumentIds = Long.parseLong(fields[0]);
        stats.initTime = Long.parseLong(fields[1]);
        stats.scanTime = Long.parseLong(fields[2]);
        stats.retrievalTime = Long.parseLong(fields[3]);
        stats.elapsedTime = Long.parseLong(fields[4]);
        return stats;
    }

    @Override
    public String toString() {
        List<Long> values = List.of(totalDocumentIds, getInitTime(), getScanTime(), getRetrievalTime(), getTotalElapsed());
        return Joiner.on(',').join(values);
    }
}
