package datawave.next.scanner;

import java.util.concurrent.ArrayBlockingQueue;
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
public class DocumentScannerConfig {

    private AccumuloClient client;
    private Authorizations authorizations;
    private BlockingQueue<KeyWithContext> docIdQueue;
    private ArrayBlockingQueue<Result> results;
    private ContextThreadFactory recordIdFactory;
    private ContextThreadFactory documentIdFactory;
    private ExecutorService docIdExecutorPool;
    private ExecutorService documentExecutorPool;

    private boolean sortedCandidateQueue = false;

    // the number of document ids/result documents to buffer
    private int docIdQueueCapacity = 1;
    private int resultQueueCapacity = 1;

    // the number of field index/document scans to conduct in parallel
    private int maxDocIdThreads = 1;
    private int maxDocumentThreads = 1;

    // the maximum number of doc id/document tasks to submit. The FixedThreadPool constructor uses
    // an unbounded queue, so we can submit more tasks than exist execution threads -- effectively
    // queuing up work
    private int maxDocIdTasks = 2;
    private int maxDocumentTasks = 2;

    // the current number of field index/document scans
    private final AtomicInteger numFiScans = new AtomicInteger(0);
    private final AtomicInteger numDocScans = new AtomicInteger(0);

    private AtomicBoolean queryDataConsumerExecuting = new AtomicBoolean(false);
    private AtomicBoolean documentIdConsumerExecuting = new AtomicBoolean(false);

    private VisitorFunction visitorFunction;

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

    public BlockingQueue<KeyWithContext> getDocIdQueue() {
        return docIdQueue;
    }

    public void setDocIdQueue(BlockingQueue<KeyWithContext> docIdQueue) {
        this.docIdQueue = docIdQueue;
    }

    public ArrayBlockingQueue<Result> getResults() {
        return results;
    }

    public void setResults(ArrayBlockingQueue<Result> results) {
        this.results = results;
    }

    public ExecutorService getDocIdExecutorPool() {
        return docIdExecutorPool;
    }

    public void setDocIdExecutorPool(ExecutorService docIdExecutorPool) {
        this.docIdExecutorPool = docIdExecutorPool;
    }

    public ExecutorService getDocumentExecutorPool() {
        return documentExecutorPool;
    }

    public void setDocumentExecutorPool(ExecutorService documentExecutorPool) {
        this.documentExecutorPool = documentExecutorPool;
    }

    public int getDocIdQueueCapacity() {
        return docIdQueueCapacity;
    }

    public void setDocIdQueueCapacity(int docIdQueueCapacity) {
        this.docIdQueueCapacity = docIdQueueCapacity;
    }

    public int getResultQueueCapacity() {
        return resultQueueCapacity;
    }

    public void setResultQueueCapacity(int resultQueueCapacity) {
        this.resultQueueCapacity = resultQueueCapacity;
    }

    public int getMaxDocIdThreads() {
        return maxDocIdThreads;
    }

    public void setMaxDocIdThreads(int maxDocIdThreads) {
        this.maxDocIdThreads = maxDocIdThreads;
    }

    public int getMaxDocumentThreads() {
        return maxDocumentThreads;
    }

    public void setMaxDocumentThreads(int maxDocumentThreads) {
        this.maxDocumentThreads = maxDocumentThreads;
    }

    public int getMaxDocIdTasks() {
        return maxDocIdTasks;
    }

    public void setMaxDocIdTasks(int maxDocIdTasks) {
        this.maxDocIdTasks = maxDocIdTasks;
    }

    public int getMaxDocumentTasks() {
        return maxDocumentTasks;
    }

    public void setMaxDocumentTasks(int maxDocumentTasks) {
        this.maxDocumentTasks = maxDocumentTasks;
    }

    public AtomicInteger getNumFiScans() {
        return numFiScans;
    }

    public AtomicInteger getNumDocScans() {
        return numDocScans;
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
                    .append(docIdQueueCapacity, other.docIdQueueCapacity)
                    .append(resultQueueCapacity, other.resultQueueCapacity)
                    .append(maxDocIdThreads, other.maxDocIdThreads)
                    .append(maxDocumentThreads, other.maxDocumentThreads)
                    .append(maxDocIdTasks, other.maxDocIdTasks)
                    .append(maxDocumentTasks, other.maxDocumentTasks)
                    .append(sortedCandidateQueue, other.sortedCandidateQueue)
                    .isEquals();
            //  @formatter:on
        }
        return false;
    }

    @Override
    public int hashCode() {
        //  @formatter:off
        return new HashCodeBuilder()
                .append(docIdQueueCapacity)
                .append(resultQueueCapacity)
                .append(maxDocIdThreads)
                .append(maxDocumentThreads)
                .append(maxDocIdTasks)
                .append(maxDocumentTasks)
                .append(sortedCandidateQueue)
                .hashCode();
        //  @formatter:on
    }

    public ContextThreadFactory getRecordIdFactory() {
        return recordIdFactory;
    }

    public void setRecordIdFactory(ContextThreadFactory recordIdFactory) {
        this.recordIdFactory = recordIdFactory;
    }

    public ContextThreadFactory getDocumentIdFactory() {
        return documentIdFactory;
    }

    public void setDocumentIdFactory(ContextThreadFactory documentIdFactory) {
        this.documentIdFactory = documentIdFactory;
    }

    public boolean isSortedCandidateQueue() {
        return sortedCandidateQueue;
    }

    public void setSortedCandidateQueue(boolean sortedCandidateQueue) {
        this.sortedCandidateQueue = sortedCandidateQueue;
    }
}
