package datawave.next.scanner;

import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import datawave.core.query.configuration.QueryData;
import datawave.next.DocIdQueryIterator;
import datawave.next.stats.QueryDataConsumerStats;
import datawave.query.iterator.QueryOptions;

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
    private final AtomicInteger numFiScans;
    private final DocumentScannerConfig config;
    private final int maxFiScans;

    private final QueryDataConsumerStats stats;
    private long fiScansSubmitted = 0L;

    public QueryDataConsumer(DocumentScannerConfig config, Iterator<QueryData> iterator) {
        this.config = config;
        this.iterator = iterator;
        this.executor = this.config.getDocIdExecutorPool();
        this.executing = this.config.getQueryDataConsumerExecuting();
        this.numFiScans = this.config.getNumFiScans();
        this.maxFiScans = this.config.getMaxDocIdTasks();
        this.stats = new QueryDataConsumerStats();
    }

    @Override
    public void run() {
        try {
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

        IteratorSetting settings = queryData.getSettings().get(0);

        IteratorSetting next = new IteratorSetting(settings.getPriority(), "DocIdQueryIterator", DocIdQueryIterator.class);
        next.addOption(QueryOptions.QUERY, queryData.getQuery());
        next.addOption(QueryOptions.START_TIME, settings.getOptions().get(QueryOptions.START_TIME));
        next.addOption(QueryOptions.END_TIME, settings.getOptions().get(QueryOptions.END_TIME));
        next.addOption(QueryOptions.INDEXED_FIELDS, settings.getOptions().get(QueryOptions.INDEXED_FIELDS));
        if (settings.getOptions().containsKey(QueryOptions.DATATYPE_FILTER)) {
            next.addOption(QueryOptions.DATATYPE_FILTER, settings.getOptions().get(QueryOptions.DATATYPE_FILTER));
        }
        next.addOption(DocIdQueryIterator.BATCH_SIZE, String.valueOf(config.getCandidateBatchSize()));

        // TODO: migrate to scanner factory to pick up configuration for scan hints and consistency level
        Scanner scanner = config.getClient().createScanner(queryData.getTableName(), config.getAuthorizations());

        // this check exists because datawave can produce day ranges for certain unit tests. The document scheduler is optimized for shard-specific plans and
        // thus is not compatible with day ranges.
        Range scanRange = Range.exact(range.getStartKey().getRow());
        if (!scanRange.equals(range)) {
            log.warn("prev: {}", range);
            log.warn("next: {}", scanRange);
            throw new RuntimeException("Scan range differed from input range");
        }

        scanner.setRange(range);
        scanner.addScanIterator(next);

        // wait until there's room to run
        while (numFiScans.get() >= maxFiScans) {
            // Note: the max field index tasks submitted may exceed the number of executor threads. This
            // effectively queues work and ensures the executor is always running at capacity.
        }

        numFiScans.incrementAndGet();
        DocumentIdProducer fiScan = new DocumentIdProducer(config, scanner, queryData);

        String context = range.getStartKey().getRow().toString();
        fiScan.setContext("fi scan " + ++fiScansSubmitted + " - " + context);
        executor.submit(fiScan);
    }

    private void putDocId(QueryData queryData, Range range) {
        stats.incrementNumDocScans();
        KeyWithContext keyWithContext = new KeyWithContext(range.getStartKey(), queryData, config.isSortedCandidateQueue());

        boolean accepted = false;
        while (!accepted) {
            try {
                accepted = config.getDocIdQueue().offer(keyWithContext, 5_000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                log.error("Interrupted while waiting", e);
                throw new RuntimeException(e);
            }
        }
    }
}
