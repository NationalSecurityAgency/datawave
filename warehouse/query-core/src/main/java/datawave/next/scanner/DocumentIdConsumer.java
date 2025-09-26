package datawave.next.scanner;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.data.Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This thread reads document ids from a queue and submits document range scans
 */
public class DocumentIdConsumer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(DocumentIdConsumer.class);

    private final DocumentScannerConfig config;

    private final long candidateQueuePollTimeMillis;
    private final BlockingQueue<KeyWithContext> documentIdQueue;

    private final ExecutorService executor;
    private final AtomicBoolean producerExecuting;
    private final AtomicBoolean executing;
    private final AtomicInteger numSearchScans;
    private final AtomicInteger numRetrievalScans;
    private final int maxRetrievalTasks;

    private long totalIdsConsumed = 0;

    public DocumentIdConsumer(DocumentScannerConfig config) {
        this.config = config;
        this.candidateQueuePollTimeMillis = config.getCandidateQueuePollTimeMillis();
        this.documentIdQueue = config.getCandidateQueue();
        this.executor = config.getRetrievalExecutorPool();

        this.producerExecuting = config.getQueryDataConsumerExecuting();
        this.executing = config.getDocumentIdConsumerExecuting();

        // this is both a consumer and a producer and needs to track the state of the producer
        this.numSearchScans = config.getNumSearchScans();
        this.numRetrievalScans = config.getNumRetrievalScans();
        this.maxRetrievalTasks = config.getMaxRetrievalTasks();
    }

    @Override
    public void run() {
        try {
            Thread.currentThread().setName(config.getQueryId() + " document id consumer");
            if (log.isTraceEnabled()) {
                log.trace("document id consumer started");
            }

            KeyWithContext keyWithContext;
            while (producerExecuting.get() || !documentIdQueue.isEmpty() || numSearchScans.get() > 0) {
                keyWithContext = documentIdQueue.poll(candidateQueuePollTimeMillis, TimeUnit.MILLISECONDS);
                if (keyWithContext != null) {

                    // wait until there's room to run
                    while (numRetrievalScans.get() >= maxRetrievalTasks) {
                        // Note: the max document tasks submitted may exceed the number of executor threads. This
                        // effectively queues work and ensures the executor is always running at capacity.
                        Thread.onSpinWait();
                        Thread.sleep(10);
                    }

                    if (log.isTraceEnabled()) {
                        log.trace("found key with context: {}", keyWithContext.getKey());
                    }
                    // construct query iterator
                    config.getStats().incrementTotalDocumentScansSubmitted();
                    long currentRetrievalScans = numRetrievalScans.incrementAndGet();

                    if (log.isTraceEnabled()) {
                        log.trace("retrieval scans: {}", currentRetrievalScans);
                    }

                    DocumentRangeScan scan = new DocumentRangeScan(keyWithContext, config);

                    Key key = keyWithContext.getKey();
                    String context = key.getRow().toString() + "-" + key.getColumnFamily().toString();
                    scan.setContext(config.getQueryId() + " retrieval scan " + ++totalIdsConsumed + " - " + context);
                    executor.execute(scan);
                }
            }
        } catch (Exception e) {
            log.error("exception while consuming document ids", e);
            throw new RuntimeException("exception while consuming document ids", e);
        } finally {
            executing.set(false);
            if (log.isTraceEnabled()) {
                log.trace("document id consumer stopped");
            }
        }
    }

}
