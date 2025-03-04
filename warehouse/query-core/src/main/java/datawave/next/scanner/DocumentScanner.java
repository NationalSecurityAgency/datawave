package datawave.next.scanner;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.core.query.configuration.QueryData;
import datawave.core.query.configuration.Result;
import datawave.query.tables.BatchScannerSession;
import datawave.query.tables.async.Scan;
import datawave.query.tables.async.event.VisitorFunction;

/**
 * An alternate to the {@link BatchScannerSession}.
 * <p>
 * The {@link Scan} is replaced by the concept of a tablet worker. This worker operates two scanners in parallel, one scanner finds candidate documents and the
 * other scanner submits document-range queries.
 */
public class DocumentScanner implements Iterator<Result>, Closeable {

    private static final Logger log = LoggerFactory.getLogger(DocumentScanner.class);

    private Result result;

    private final ArrayBlockingQueue<Result> results;

    // special executor pool for our consumers
    private final ExecutorService consumerExecutorPool = Executors.newFixedThreadPool(2);

    // fetches document ids
    private final ExecutorService docIdExecutorPool;

    // fetches and evaluates document candidates
    private final ExecutorService documentExecutorPool;

    private final DocumentScannerConfig config;

    /**
     * Default constructor, will likely need to swap this out for a config object constructor
     *
     * @param config
     *            the {@link DocumentScannerConfig}
     * @param queryDataIterator
     *            the iterator of {@link QueryData}
     */
    public DocumentScanner(DocumentScannerConfig config, Iterator<QueryData> queryDataIterator) {
        this.config = config;
        this.results = this.config.getResults();

        this.docIdExecutorPool = this.config.getDocIdExecutorPool();
        this.documentExecutorPool = this.config.getDocumentExecutorPool();

        // takes query data and either submits fi scans or pushes document scans directly to the doc id queue
        QueryDataConsumer queryDataConsumer = new QueryDataConsumer(config, queryDataIterator);
        consumerExecutorPool.execute(queryDataConsumer);

        // a document id consumer creates document range scans which push results onto the results queue
        DocumentIdConsumer docIdConsumer = new DocumentIdConsumer(config);
        consumerExecutorPool.execute(docIdConsumer);
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
        while ((config.getDocumentIdConsumerExecuting().get() || !config.getDocIdQueue().isEmpty() || config.getNumFiScans().get() > 0
                        || config.getNumDocScans().get() > 0 || !config.getResults().isEmpty()) && result == null) {
            try {
                result = results.poll(1, TimeUnit.MILLISECONDS);
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

        consumerExecutorPool.shutdownNow();
        docIdExecutorPool.shutdownNow();
        documentExecutorPool.shutdownNow();
    }
}
