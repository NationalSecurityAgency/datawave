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
    private final AtomicInteger numFiScans;
    private final AtomicInteger numDocScans;
    private final int maxDocScans;

    private long totalIdsConsumed = 0;

    public DocumentIdConsumer(DocumentScannerConfig config) {
        this.config = config;
        this.candidateQueuePollTimeMillis = config.getCandidateQueuePollTimeMillis();
        this.documentIdQueue = config.getDocIdQueue();
        this.executor = config.getDocumentExecutorPool();

        this.producerExecuting = config.getQueryDataConsumerExecuting();
        this.executing = config.getDocumentIdConsumerExecuting();

        // this is both a consumer and a producer and needs to track the state of the producer
        this.numFiScans = config.getNumFiScans();
        this.numDocScans = config.getNumDocScans();
        this.maxDocScans = config.getMaxDocumentTasks();
    }

    @Override
    public void run() {
        try {
            Thread.currentThread().setName(config.getQueryId() + " document id consumer");
            if (log.isDebugEnabled()) {
                log.debug("document id consumer started");
            }

            KeyWithContext keyWithContext;
            while (producerExecuting.get() || !documentIdQueue.isEmpty() || numFiScans.get() > 0) {
                try {
                    keyWithContext = documentIdQueue.poll(candidateQueuePollTimeMillis, TimeUnit.MILLISECONDS);
                    if (keyWithContext != null) {

                        // wait until there's room to run
                        while (numDocScans.get() >= maxDocScans) {
                            // Note: the max document tasks submitted may exceed the number of executor threads. This
                            // effectively queues work and ensures the executor is always running at capacity.
                            Thread.onSpinWait();
                        }

                        if (log.isDebugEnabled()) {
                            log.debug("found key with context: {}", keyWithContext.getKey());
                        }
                        // construct query iterator
                        config.getStats().incrementTotalDocumentScansSubmitted();
                        long currentDocScanCount = numDocScans.incrementAndGet();

                        log.info("current doc scans: {}", currentDocScanCount);
                        DocumentRangeScan scan = new DocumentRangeScan(keyWithContext, config);

                        Key key = keyWithContext.getKey();
                        String context = key.getRow().toString() + "-" + key.getColumnFamily().toString();
                        scan.setContext(config.getQueryId() + " doc scan " + ++totalIdsConsumed + " - " + context);
                        executor.execute(scan);
                    }
                } catch (Exception e) {
                    log.error("exception while consuming document ids", e);
                    // TODO: might need to zero out numFiScans
                }
            }
        } finally {
            executing.set(false);
            if (log.isDebugEnabled()) {
                log.debug("document id consumer stopped");
            }
        }
    }

}
