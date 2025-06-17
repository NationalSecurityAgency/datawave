package datawave.next.scanner;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import datawave.core.query.configuration.QueryData;
import datawave.next.stats.QueryDataConsumerStats;

/**
 * Thread that consumes {@link QueryData} and submits them to the {@link DocumentScanner}'s executor pool.
 * <p>
 * The document id thread takes any document ids and submits
 */
public class QueryDataConsumer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(QueryDataConsumer.class);

    private final Iterator<QueryData> iterator;
    private final ExecutorService executor;
    private final AtomicBoolean executing;
    private final AtomicInteger numSearchScans;
    private final DocumentScannerConfig config;
    private final int maxSearchTasks;

    private final long candidateQueueOfferTimeMillis;
    private final BlockingQueue<KeyWithContext> candidateQueue;

    private final QueryDataConsumerStats stats;
    private long fiScansSubmitted = 0L;

    public QueryDataConsumer(DocumentScannerConfig config, Iterator<QueryData> iterator) {
        this.config = config;
        this.iterator = iterator;
        this.executor = this.config.getSearchExecutorPool();
        this.executing = this.config.getQueryDataConsumerExecuting();
        this.numSearchScans = this.config.getNumSearchScans();
        this.maxSearchTasks = this.config.getMaxSearchTasks();
        this.candidateQueueOfferTimeMillis = this.config.getCandidateQueueOfferTimeMillis();
        this.candidateQueue = this.config.getCandidateQueue();
        this.stats = new QueryDataConsumerStats();
    }

    @Override
    public void run() {
        try {
            Thread.currentThread().setName(config.getQueryId() + " query data consumer");
            if (log.isDebugEnabled()) {
                log.debug("query data consumer started");
            }
            while (iterator.hasNext()) {
                QueryData queryData = iterator.next();
                if (log.isDebugEnabled()) {
                    log.debug("got query data: {}", queryData.getRanges().iterator().next().getStartKey().toStringNoTime());
                }
                if (queryData == null) {
                    stats.incrementNullDataSeen();
                    log.info("query data was null");
                    continue;
                }
                if (queryData.getSettings() == null) {
                    log.info("query data settings was null");
                }
                if (queryData.getRanges() == null) {
                    log.info("query data ranges was null");
                }
                stats.incrementQueryDataSeen();

                Preconditions.checkArgument(queryData.getSettings().size() == 1);
                Preconditions.checkArgument(queryData.getRanges().size() == 1);

                Range range = queryData.getRanges().iterator().next();
                if (isDocumentRange(range)) {
                    putDocId(queryData, range);
                } else {
                    putFiScan(queryData, range);
                }
            }
        } catch (Exception e) {
            log.error("ScannerChunkConsumer saw error", e);
            throw new RuntimeException(e);
        } finally {
            executing.set(false);
            if (log.isDebugEnabled()) {
                log.debug("query data consumer stopped");
            }

            config.getStats().setConsumerStats(stats);
        }
    }

    private boolean isDocumentRange(Range range) {
        Key start = range.getStartKey();
        return start.getColumnFamily().getLength() > 0;
    }

    private void putFiScan(QueryData queryData, Range range) throws TableNotFoundException {
        stats.incrementNumShardScans();

        // wait until there's room to run
        while (numSearchScans.get() >= maxSearchTasks) {
            // Note: the max field index tasks submitted may exceed the number of executor threads. This
            // effectively queues work and ensures the executor is always running at capacity.
            Thread.onSpinWait();
        }

        int currentFiScanCount = numSearchScans.incrementAndGet();
        log.debug("current fi scans: {}", currentFiScanCount);
        DocumentIdProducer fiScan = new DocumentIdProducer(config, queryData, range);

        String context = range.getStartKey().getRow().toString();
        fiScan.setContext("fi scan " + ++fiScansSubmitted + " - " + context);
        executor.execute(fiScan);
    }

    private void putDocId(QueryData queryData, Range range) {
        stats.incrementNumDocScans();
        KeyWithContext keyWithContext = new KeyWithContext(range.getStartKey(), queryData, config.isSortedCandidateQueue());

        boolean accepted = false;
        while (!accepted) {
            try {
                accepted = candidateQueue.offer(keyWithContext, candidateQueueOfferTimeMillis, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                log.error("Interrupted while waiting", e);
                throw new RuntimeException(e);
            }
        }
    }
}
