package datawave.next.scanner;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import datawave.core.query.configuration.Result;
import datawave.next.async.ContextThreadFactory;
import datawave.next.stats.DocumentSchedulerStats;
import datawave.query.tables.async.event.VisitorFunction;

/**
 * Configuration object that is passed around the {@link DocumentScanner} ecosystem.
 * <p>
 * Externally we only care about configuring the queue capacities, thread pool sizes, and max tasks per thread pool.
 */
public class DocumentScannerConfig implements Serializable {

    private static final long serialVersionUID = 404271911443485394L;

    private AccumuloClient client;
    private Authorizations authorizations;
    private transient BlockingQueue<KeyWithContext> candidateQueue;
    private transient BlockingQueue<Result> results;
    private transient ContextThreadFactory searchThreadFactory;
    private transient ContextThreadFactory retrievalThreadFactory;
    private transient ExecutorService searchExecutorPool;
    private transient ExecutorService retrievalExecutorPool;

    // sort candidate queue by uid and datatype to prevent hot spotting during retrieval
    private boolean sortedCandidateQueue = false;

    // batch ids to minimize new scanner connections
    private int candidateBatchSize = 1;
    private boolean allowPartialIntersections = false;

    // the number of document ids/result documents to buffer
    private int candidateQueueCapacity = 1;
    private int resultQueueCapacity = 1;

    // the number of field index/document scans to conduct in parallel
    private int searchThreads = 1;
    private int retrievalThreads = 1;

    // the maximum number of doc id/document tasks to submit. The FixedThreadPool constructor uses
    // an unbounded queue, so we can submit more tasks than exist execution threads -- effectively
    // queuing up work
    private int maxSearchTasks = 2;
    private int maxRetrievalTasks = 2;

    // the current number of field index/document scans
    private final AtomicInteger numSearchScans = new AtomicInteger(0);
    private final AtomicInteger numRetrievalScans = new AtomicInteger(0);

    private AtomicBoolean queryDataConsumerExecuting = new AtomicBoolean(false);
    private AtomicBoolean documentIdConsumerExecuting = new AtomicBoolean(false);

    private long candidateQueueOfferTimeMillis = 5_000L;
    private long candidateQueuePollTimeMillis = 5_000L;
    private long resultQueueOfferTimeMillis = 25L;
    private long resultQueuePollTimeMillis = 25L;

    private long scanTimeoutMillis = 10_000L;

    // should the scheduler use the query iterator or a document iterator?
    private boolean useQueryIterator = true;

    private transient VisitorFunction visitorFunction;
    private String queryId;

    // accumulo execution hints control which executor pool handles our scans
    private String searchScanHintTable;
    private String searchScanHintPool;

    private String retrievalScanHintTable;
    private String retrievalScanHintPool;

    // IMMEDIATE or EVENTUAL
    private String searchConsistencyLevel;
    private String retrievalConsistencyLevel;

    private final DocumentSchedulerStats stats = new DocumentSchedulerStats();

    public DocumentScannerConfig() {
        // empty constructor
    }

    public AccumuloClient getClient() {
        return client;
    }

    public void setClient(AccumuloClient client) {
        this.client = client;
    }

    public Authorizations getAuthorizations() {
        return authorizations;
    }

    public void setAuthorizations(Authorizations authorizations) {
        this.authorizations = authorizations;
    }

    public BlockingQueue<KeyWithContext> getCandidateQueue() {
        return candidateQueue;
    }

    public void setCandidateQueue(BlockingQueue<KeyWithContext> candidateQueue) {
        this.candidateQueue = candidateQueue;
    }

    public BlockingQueue<Result> getResults() {
        return results;
    }

    public void setResults(BlockingQueue<Result> results) {
        this.results = results;
    }

    public ExecutorService getSearchExecutorPool() {
        return searchExecutorPool;
    }

    public void setSearchExecutorPool(ExecutorService searchExecutorPool) {
        this.searchExecutorPool = searchExecutorPool;
    }

    public ExecutorService getRetrievalExecutorPool() {
        return retrievalExecutorPool;
    }

    public void setRetrievalExecutorPool(ExecutorService retrievalExecutorPool) {
        this.retrievalExecutorPool = retrievalExecutorPool;
    }

    public int getCandidateQueueCapacity() {
        return candidateQueueCapacity;
    }

    public void setCandidateQueueCapacity(int candidateQueueCapacity) {
        this.candidateQueueCapacity = candidateQueueCapacity;
    }

    public int getResultQueueCapacity() {
        return resultQueueCapacity;
    }

    public void setResultQueueCapacity(int resultQueueCapacity) {
        this.resultQueueCapacity = resultQueueCapacity;
    }

    public int getSearchThreads() {
        return searchThreads;
    }

    public void setSearchThreads(int searchThreads) {
        this.searchThreads = searchThreads;
    }

    public int getRetrievalThreads() {
        return retrievalThreads;
    }

    public void setRetrievalThreads(int retrievalThreads) {
        this.retrievalThreads = retrievalThreads;
    }

    public int getMaxSearchTasks() {
        return maxSearchTasks;
    }

    public void setMaxSearchTasks(int maxSearchTasks) {
        this.maxSearchTasks = maxSearchTasks;
    }

    public int getMaxRetrievalTasks() {
        return maxRetrievalTasks;
    }

    public void setMaxRetrievalTasks(int maxRetrievalTasks) {
        this.maxRetrievalTasks = maxRetrievalTasks;
    }

    public AtomicInteger getNumSearchScans() {
        return numSearchScans;
    }

    public AtomicInteger getNumRetrievalScans() {
        return numRetrievalScans;
    }

    public AtomicBoolean getQueryDataConsumerExecuting() {
        return queryDataConsumerExecuting;
    }

    public void setQueryDataConsumerExecuting(AtomicBoolean queryDataConsumerExecuting) {
        this.queryDataConsumerExecuting = queryDataConsumerExecuting;
    }

    public AtomicBoolean getDocumentIdConsumerExecuting() {
        return documentIdConsumerExecuting;
    }

    public void setDocumentIdConsumerExecuting(AtomicBoolean documentIdConsumerExecuting) {
        this.documentIdConsumerExecuting = documentIdConsumerExecuting;
    }

    public VisitorFunction getVisitorFunction() {
        return visitorFunction;
    }

    public void setVisitorFunction(VisitorFunction visitorFunction) {
        this.visitorFunction = visitorFunction;
    }

    public DocumentSchedulerStats getStats() {
        return stats;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DocumentScannerConfig) {
            DocumentScannerConfig other = (DocumentScannerConfig) obj;
            //  @formatter:off
            return new EqualsBuilder()
                    .append(candidateQueueCapacity, other.candidateQueueCapacity)
                    .append(resultQueueCapacity, other.resultQueueCapacity)
                    .append(searchThreads, other.searchThreads)
                    .append(retrievalThreads, other.retrievalThreads)
                    .append(maxSearchTasks, other.maxSearchTasks)
                    .append(maxRetrievalTasks, other.maxRetrievalTasks)
                    .append(sortedCandidateQueue, other.sortedCandidateQueue)
                    .append(candidateBatchSize, other.candidateBatchSize)
                    .append(searchScanHintTable, other.searchScanHintTable)
                    .append(searchScanHintPool, other.searchScanHintPool)
                    .append(retrievalScanHintTable, other.retrievalScanHintTable)
                    .append(retrievalScanHintPool, other.retrievalScanHintPool)
                    .append(searchConsistencyLevel, other.searchConsistencyLevel)
                    .append(retrievalConsistencyLevel, other.retrievalConsistencyLevel)
                    .isEquals();
            //  @formatter:on
        }
        return false;
    }

    @Override
    public int hashCode() {
        //  @formatter:off
        return new HashCodeBuilder()
                .append(candidateQueueCapacity)
                .append(resultQueueCapacity)
                .append(searchThreads)
                .append(retrievalThreads)
                .append(maxSearchTasks)
                .append(maxRetrievalTasks)
                .append(sortedCandidateQueue)
                .append(candidateBatchSize)
                .append(searchScanHintTable)
                .append(searchScanHintPool)
                .append(retrievalScanHintTable)
                .append(retrievalScanHintPool)
                .append(searchConsistencyLevel)
                .append(retrievalConsistencyLevel)
                .hashCode();
        //  @formatter:on
    }

    public ContextThreadFactory getSearchThreadFactory() {
        return searchThreadFactory;
    }

    public void setSearchThreadFactory(ContextThreadFactory searchThreadFactory) {
        this.searchThreadFactory = searchThreadFactory;
    }

    public ContextThreadFactory getRetrievalThreadFactory() {
        return retrievalThreadFactory;
    }

    public void setRetrievalThreadFactory(ContextThreadFactory retrievalThreadFactory) {
        this.retrievalThreadFactory = retrievalThreadFactory;
    }

    public boolean isSortedCandidateQueue() {
        return sortedCandidateQueue;
    }

    public void setSortedCandidateQueue(boolean sortedCandidateQueue) {
        this.sortedCandidateQueue = sortedCandidateQueue;
    }

    public int getCandidateBatchSize() {
        return candidateBatchSize;
    }

    public void setCandidateBatchSize(int candidateBatchSize) {
        this.candidateBatchSize = candidateBatchSize;
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public long getResultQueueOfferTimeMillis() {
        return resultQueueOfferTimeMillis;
    }

    public void setResultQueueOfferTimeMillis(long resultQueueOfferTimeMillis) {
        this.resultQueueOfferTimeMillis = resultQueueOfferTimeMillis;
    }

    public long getCandidateQueueOfferTimeMillis() {
        return candidateQueueOfferTimeMillis;
    }

    public void setCandidateQueueOfferTimeMillis(long candidateQueueOfferTimeMillis) {
        this.candidateQueueOfferTimeMillis = candidateQueueOfferTimeMillis;
    }

    public long getCandidateQueuePollTimeMillis() {
        return candidateQueuePollTimeMillis;
    }

    public void setCandidateQueuePollTimeMillis(long candidateQueuePollTimeMillis) {
        this.candidateQueuePollTimeMillis = candidateQueuePollTimeMillis;
    }

    public long getResultQueuePollTimeMillis() {
        return resultQueuePollTimeMillis;
    }

    public void setResultQueuePollTimeMillis(long resultQueuePollTimeMillis) {
        this.resultQueuePollTimeMillis = resultQueuePollTimeMillis;
    }

    public long getScanTimeoutMillis() {
        return scanTimeoutMillis;
    }

    public void setScanTimeoutMillis(long scanTimeoutMillis) {
        this.scanTimeoutMillis = scanTimeoutMillis;
    }

    public boolean isUseQueryIterator() {
        return useQueryIterator;
    }

    public void setUseQueryIterator(boolean useQueryIterator) {
        this.useQueryIterator = useQueryIterator;
    }

    public String getSearchScanHintTable() {
        return searchScanHintTable;
    }

    public void setSearchScanHintTable(String searchScanHintTable) {
        this.searchScanHintTable = searchScanHintTable;
    }

    public String getSearchScanHintPool() {
        return searchScanHintPool;
    }

    public void setSearchScanHintPool(String searchScanHintPool) {
        this.searchScanHintPool = searchScanHintPool;
    }

    public String getRetrievalScanHintTable() {
        return retrievalScanHintTable;
    }

    public void setRetrievalScanHintTable(String retrievalScanHintTable) {
        this.retrievalScanHintTable = retrievalScanHintTable;
    }

    public String getRetrievalScanHintPool() {
        return retrievalScanHintPool;
    }

    public void setRetrievalScanHintPool(String retrievalScanHintPool) {
        this.retrievalScanHintPool = retrievalScanHintPool;
    }

    public String getSearchConsistencyLevel() {
        return searchConsistencyLevel;
    }

    public void setSearchConsistencyLevel(String searchConsistencyLevel) {
        this.searchConsistencyLevel = searchConsistencyLevel;
    }

    public String getRetrievalConsistencyLevel() {
        return retrievalConsistencyLevel;
    }

    public void setRetrievalConsistencyLevel(String retrievalConsistencyLevel) {
        this.retrievalConsistencyLevel = retrievalConsistencyLevel;
    }

    public boolean isAllowPartialIntersections() {
        return allowPartialIntersections;
    }

    public void setAllowPartialIntersections(boolean allowPartialIntersections) {
        this.allowPartialIntersections = allowPartialIntersections;
    }
}
