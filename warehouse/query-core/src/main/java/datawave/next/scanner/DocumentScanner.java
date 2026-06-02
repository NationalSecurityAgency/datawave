package datawave.next.scanner;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.core.query.configuration.QueryData;
import datawave.core.query.configuration.Result;
import datawave.next.async.ContextThreadFactory;
import datawave.query.tables.BatchScannerSession;
import datawave.query.tables.async.Scan;
import datawave.query.tables.async.event.VisitorFunction;

/**
 * An alternate to the {@link BatchScannerSession}.
 * <p>
 * The {@link Scan} is replaced by the concept of a tablet worker. This worker operates two scanners in parallel, one scanner finds candidate documents and the
 * other scanner submits document-range queries.
 */
public class DocumentScanner implements Iterator<Result>, Closeable, Thread.UncaughtExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(DocumentScanner.class);

    protected Result result;

    private final long resultQueuePollTimeMillis;
    private final BlockingQueue<Result> results;

    // special executor pool for our consumers
    private final ExecutorService consumerExecutorPool = Executors.newFixedThreadPool(2);

    // fetches document ids
    private final ExecutorService searchExecutor;

    // fetches and evaluates document candidates
    private final ExecutorService retrievalExecutor;

    protected final DocumentScannerConfig config;

    private final Iterator<QueryData> queryDataIterator;

    /**
     * Default constructor. Creates and configures thread pools used for document search and retrieval.
     *
     * @param config
     *            the {@link DocumentScannerConfig}
     * @param queryDataIterator
     *            the iterator of {@link QueryData}
     */
    public DocumentScanner(DocumentScannerConfig config, Iterator<QueryData> queryDataIterator) {
        this.config = config;
        this.queryDataIterator = queryDataIterator;
        this.resultQueuePollTimeMillis = this.config.getResultQueuePollTimeMillis();
        this.results = this.config.getResults();

        ContextThreadFactory searchThreadFactory = new ContextThreadFactory("fi scan");
        searchThreadFactory.setUncaughtExceptionHandler(this);
        config.setSearchThreadFactory(searchThreadFactory);

        ContextThreadFactory retrievalThreadFactory = new ContextThreadFactory("doc scan");
        retrievalThreadFactory.setUncaughtExceptionHandler(this);
        config.setRetrievalThreadFactory(retrievalThreadFactory);

        ThreadPoolExecutor searchExecutor = new ThreadPoolExecutor(config.getSearchThreads(), config.getSearchThreads(), 10L, TimeUnit.MINUTES,
                        new LinkedBlockingQueue<>(), searchThreadFactory);

        ThreadPoolExecutor retrievalExecutor = new ThreadPoolExecutor(config.getRetrievalThreads(), config.getRetrievalThreads(), 10, TimeUnit.MINUTES,
                        new LinkedBlockingQueue<>(), retrievalThreadFactory);

        config.setSearchExecutorPool(searchExecutor);
        config.setRetrievalExecutorPool(retrievalExecutor);

        this.searchExecutor = this.config.getSearchExecutorPool();
        this.retrievalExecutor = this.config.getRetrievalExecutorPool();

        config.getQueryDataConsumerExecuting().set(true);
        config.getDocumentIdConsumerExecuting().set(true);

        log.info("created search scanner with {} threads and {} max tasks", config.getSearchThreads(), config.getMaxSearchTasks());
        log.info("created retrieval scanner with {} threads and {} max tasks", config.getRetrievalThreads(), config.getMaxRetrievalTasks());
    }

    public void start() {
        // takes query data and either submits fi scans or pushes document scans directly to the doc id queue
        QueryDataConsumer queryDataConsumer = new QueryDataConsumer(config, queryDataIterator);
        Thread searchThread = new Thread(queryDataConsumer);
        // searchThread.setDaemon(true);
        searchThread.setUncaughtExceptionHandler(this);
        consumerExecutorPool.execute(searchThread);

        // a document id consumer creates document range scans which push results onto the results queue
        DocumentIdConsumer docIdConsumer = new DocumentIdConsumer(config);
        Thread retrievalThread = new Thread(docIdConsumer);
        // retrievalThread.setDaemon(true);
        retrievalThread.setUncaughtExceptionHandler(this);
        consumerExecutorPool.execute(retrievalThread);
    }

    public void setVisitorFunction(VisitorFunction visitorFunction) {
        this.config.setVisitorFunction(visitorFunction);
    }

    public Iterator<Result> iterator() {
        hasNext();
        return this;
    }

    @Override
    public boolean hasNext() {
        while ((config.getDocumentIdConsumerExecuting().get() || !config.getCandidateQueue().isEmpty() || config.getNumSearchScans().get() > 0
                        || config.getNumRetrievalScans().get() > 0 || !config.getResults().isEmpty()) && result == null) {
            try {
                result = results.poll(resultQueuePollTimeMillis, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                log.error("error while polling for next result", e);
                throw new RuntimeException(e);
            }
        }

        if (result == null) {
            try {
                close();
            } catch (IOException e) {
                // exception closing
                log.error("exception while closing", e);
            }
        }

        return result != null;
    }

    @Override
    public Result next() {
        Result next = result;
        result = null;
        if (next != null) {
            config.getStats().incrementTotalResultsReturned();
        }
        return next;
    }

    @Override
    public void close() throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("closing DocumentScanner");
        }

        config.getSearchThreadFactory().logThreadsCreated();
        config.getRetrievalThreadFactory().logThreadsCreated();

        consumerExecutorPool.shutdownNow();
        searchExecutor.shutdownNow();
        retrievalExecutor.shutdownNow();
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        log.error("Uncaught exception in {}", t.getName(), e);
        t.interrupt();
        try {
            close();
        } catch (IOException ex) {
            log.error("Exception while closing DocumentScanner: {}", ex.getMessage());
            throw new RuntimeException("Exception while closing DocumentScanner", e);
        }
    }
}
