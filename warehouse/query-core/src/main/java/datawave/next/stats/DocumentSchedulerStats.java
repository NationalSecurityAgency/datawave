package datawave.next.stats;

import java.io.Serializable;

/**
 * The scheduler stats are associated with the config object which is shared among multiple threads.
 * <p>
 * Note: great care must be taken when incrementing the underlying objects. In each case only a single thread would ever update the variable.
 */
public class DocumentSchedulerStats implements Serializable {

    private static final long serialVersionUID = 6253805455359165344L;

    private long totalDocumentScansSubmitted = 0L;
    private long totalResultsReturned = 0L;

    private QueryDataConsumerStats consumerStats;

    // fine-grain stats for next, seek, etc
    private final DocumentIteratorStats iteratorStats = new DocumentIteratorStats();

    // field index timing stats
    private final DocIdQueryIteratorStats queryStats = new DocIdQueryIteratorStats();

    // field index timing stats, plus others. replaces doc id query iterator stats
    private final DocumentCandidateStats candidateStats = new DocumentCandidateStats();

    // document retrieval timing stats
    private final DocumentRetrievalStats retrievalStats = new DocumentRetrievalStats();

    public void setConsumerStats(QueryDataConsumerStats consumerStats) {
        this.consumerStats = consumerStats;
    }

    public void incrementTotalDocumentScansSubmitted() {
        this.totalDocumentScansSubmitted++;
    }

    public long getTotalDocumentScansSubmitted() {
        return totalDocumentScansSubmitted;
    }

    public void incrementTotalResultsReturned() {
        this.totalResultsReturned++;
    }

    public long getTotalResultsReturned() {
        return totalResultsReturned;
    }

    public void merge(DocumentIteratorStats iteratorStats) {
        this.iteratorStats.merge(iteratorStats);
    }

    public void merge(DocIdQueryIteratorStats queryStats) {
        this.candidateStats.merge(queryStats);
    }

    public void merge(ScanTimeStats retrievalStats) {
        this.retrievalStats.merge(retrievalStats);
    }

    public String logStats(String queryId) {
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
