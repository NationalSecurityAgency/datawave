package datawave.next.stats;

import java.io.Serializable;

/**
 * The scheduler stats are associated with the config object which is shared among multiple threads.
 * <p>
 * Every method is synchronized because this instance is the single point at which per-scan stats are aggregated. Search threads merge candidate stats,
 * retrieval threads merge timing stats, and the query thread reads all of it. The nested stats objects are plain data holders and are only safe to touch while
 * holding this lock. Note that the merges happen once per scan rather than once per key, so the lock is not on a hot path.
 */
public class DocumentSchedulerStats implements Serializable {

    private static final long serialVersionUID = 6253805455359165344L;

    private long totalDocumentScansSubmitted = 0L;
    private long totalResultsReturned = 0L;

    private QueryDataConsumerStats consumerStats;

    // fine-grain stats for next, seek, etc
    private final DocIterStats iteratorStats = new DocIterStats();

    // field index timing stats
    private final DocIdQueryIterStats queryStats = new DocIdQueryIterStats();

    // field index timing stats, plus others. replaces doc id query iterator stats
    private final DocumentCandidateStats candidateStats = new DocumentCandidateStats();

    // document retrieval timing stats
    private final DocumentRetrievalStats retrievalStats = new DocumentRetrievalStats();

    public synchronized void setConsumerStats(QueryDataConsumerStats consumerStats) {
        this.consumerStats = consumerStats;
    }

    public synchronized void incrementTotalDocumentScansSubmitted() {
        this.totalDocumentScansSubmitted++;
    }

    public synchronized long getTotalDocumentScansSubmitted() {
        return totalDocumentScansSubmitted;
    }

    public synchronized void incrementTotalResultsReturned() {
        this.totalResultsReturned++;
    }

    public synchronized long getTotalResultsReturned() {
        return totalResultsReturned;
    }

    /**
     * Merge iterator stats returned by a field index scan. Called by every search thread.
     *
     * @param iteratorStats
     *            the stats to merge
     */
    public synchronized void merge(DocIterStats iteratorStats) {
        this.iteratorStats.merge(iteratorStats);
    }

    /**
     * Merge candidate stats returned by a field index scan. Called by every search thread.
     *
     * @param queryStats
     *            the stats to merge
     */
    public synchronized void merge(DocIdQueryIterStats queryStats) {
        this.candidateStats.merge(queryStats);
    }

    /**
     * Merge timing stats for a completed document retrieval. Called by every retrieval thread.
     *
     * @param retrievalStats
     *            the stats to merge
     */
    public synchronized void merge(ScanTimeStats retrievalStats) {
        this.retrievalStats.merge(retrievalStats);
    }

    public synchronized String logStats(String queryId) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n=== ").append(queryId).append(" DocumentScheduler stats ===\n");
        if (consumerStats != null) {
            sb.append("query data seen: ").append(consumerStats.getQueryDataSeen()).append("\n");
            sb.append("doc/shard scans submitted: (").append(consumerStats.getNumDocScans()).append("/").append(consumerStats.getNumShardScans()).append(")\n");
        }

        sb.append("total next/seek calls: (").append(iteratorStats.getNextCount()).append("/").append(iteratorStats.getSeekCount()).append(")\n");

        //  @formatter:off
        sb.append("total datatype/regex/time misses: (")
                .append(iteratorStats.getDatatypeFilterMiss()).append("/")
                .append(iteratorStats.getRegexMiss()).append("/")
                .append(iteratorStats.getTimeFilterMiss()).append(")\n");
        //  @formatter:on

        // candidate and retrieval timing stats
        sb.append("candidate scan stats: ").append(candidateStats.getScanStats()).append("\n");
        sb.append("retrieval scan stats: ").append(retrievalStats.getScanStats()).append("\n");
        sb.append("slowest retrieval took : ").append(retrievalStats.getSlowestScan()).append("\n");

        //  @formatter:off
        sb.append("total candidates/doc scans/results: (")
                .append(queryStats.getTotalDocumentIds()).append("/")
                .append(totalDocumentScansSubmitted).append("/")
                .append(totalResultsReturned).append(")\n");
        //  @formatter:on

        return sb.toString();
    }
}
