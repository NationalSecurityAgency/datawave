package datawave.next.scanner;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iteratorsImpl.system.IterationInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import datawave.core.query.configuration.QueryData;
import datawave.next.DocIdQueryIterator;
import datawave.next.async.RunnableWithContext;
import datawave.next.stats.DocIdQueryIteratorStats;
import datawave.next.stats.DocumentIteratorStats;
import datawave.query.iterator.QueryOptions;

/**
 * A runnable that handles async scanning of a tablet to find document candidates.
 */
public class DocumentIdProducer implements RunnableWithContext {

    private static final Logger log = LoggerFactory.getLogger(DocumentIdProducer.class);

    private final DocumentScannerConfig config;
    private final long candidateQueueOfferTimeMillis;
    private final BlockingQueue<KeyWithContext> candidateQueue;
    private final Range range;
    private final QueryData context;
    private final AtomicInteger numSearchScans;

    private String runnableContext;

    public DocumentIdProducer(DocumentScannerConfig config, QueryData context, Range range) {
        this.config = config;
        this.context = context;
        this.range = range;
        this.candidateQueueOfferTimeMillis = this.config.getCandidateQueueOfferTimeMillis();
        this.candidateQueue = this.config.getCandidateQueue();
        this.numSearchScans = this.config.getNumSearchScans();
    }

    @Override
    public void run() {
        Thread.currentThread().setName(getContext());
        if (log.isDebugEnabled()) {
            log.debug("scanning shard {} for candidates", range.getStartKey());
        }

        try {
            boolean executing = true;
            while (executing) {
                try {
                    executeScan();
                    executing = false;
                } catch (IterationInterruptedException e) {
                    log.warn("time sliced, resubmitting scan for {}", getContext());
                }
            }
        } catch (Exception e) {
            log.error("exception found while scanning the field index", e);
            throw new RuntimeException("exception found while scanning the field index", e);
        } finally {
            numSearchScans.getAndDecrement();
        }
    }

    private void executeScan() throws TableNotFoundException, InterruptedException {
        try (Scanner scanner = createScanner()) {
            boolean offered;
            for (Map.Entry<Key,Value> entry : scanner) {
                Key key = entry.getKey();
                String payload = entry.getValue().toString();
                KeyWithContext keyWithContext = parseEntry(key, payload);

                if (keyWithContext == null) {
                    continue;
                }

                offered = false;
                while (!offered) {
                    offered = candidateQueue.offer(keyWithContext, candidateQueueOfferTimeMillis, TimeUnit.MILLISECONDS);
                }
            }
        }
    }

    private Scanner createScanner() throws TableNotFoundException {
        // this check exists because datawave can produce day ranges for certain unit tests. The document scheduler is optimized for shard-specific plans and
        // thus is not compatible with day ranges.
        Range scanRange = Range.exact(range.getStartKey().getRow());
        if (!scanRange.equals(range)) {
            log.warn("prev: {}", range);
            log.warn("next: {}", scanRange);
            throw new RuntimeException("Scan range differed from input range");
        }

        Scanner scanner = config.getClient().createScanner(context.getTableName(), config.getAuthorizations());
        scanner.setRange(range);
        scanner.addScanIterator(createIteratorSetting());

        if (config.getSearchScanHintTable() != null && config.getSearchScanHintPool() != null) {
            Preconditions.checkArgument(context.getTableName().equals(config.getRetrievalScanHintTable()), "Table name did not match execution hint");
            scanner.setExecutionHints(Map.of("scan_type", config.getSearchScanHintPool()));
        }

        if (config.getSearchConsistencyLevel() != null) {
            scanner.setConsistencyLevel(ConsistencyLevel.valueOf(config.getSearchConsistencyLevel()));
        }
        return scanner;
    }

    private IteratorSetting createIteratorSetting() {
        IteratorSetting settings = context.getSettings().get(0);

        IteratorSetting next = new IteratorSetting(settings.getPriority(), "DocIdQueryIterator", DocIdQueryIterator.class);
        next.addOption(QueryOptions.QUERY, context.getQuery());
        next.addOption(QueryOptions.START_TIME, settings.getOptions().get(QueryOptions.START_TIME));
        next.addOption(QueryOptions.END_TIME, settings.getOptions().get(QueryOptions.END_TIME));
        next.addOption(QueryOptions.INDEXED_FIELDS, settings.getOptions().get(QueryOptions.INDEXED_FIELDS));
        if (settings.getOptions().containsKey(QueryOptions.DATATYPE_FILTER)) {
            next.addOption(QueryOptions.DATATYPE_FILTER, settings.getOptions().get(QueryOptions.DATATYPE_FILTER));
        }
        next.addOption(DocIdQueryIterator.BATCH_SIZE, String.valueOf(config.getCandidateBatchSize()));
        next.addOption(DocIdQueryIterator.PARTIAL_INTERSECTIONS, String.valueOf(config.isAllowPartialIntersections()));
        return next;
    }

    private KeyWithContext parseEntry(Key key, String payload) {
        if (isBulkContext(payload)) {
            // handle parsing bulk entry and any stats
            String[] parts = payload.split(";");
            String row = parts[0];
            String columnFamilies = parts[1];

            if (parts.length == 3) {
                String stats = parts[2];
                updateStats(stats);
            }

            Set<Key> bulk = new HashSet<>();
            for (String columnFamily : columnFamilies.split(",")) {
                bulk.add(new Key(row, columnFamily));
            }

            if (key.getColumnFamily().toString().equals("STATS")) {
                // fake key was generated to return stats, return null so the producer skips this key
                return null;
            }

            return new BulkKeyWithContext(key, bulk, context, config.isSortedCandidateQueue());
        }

        if (isStats(payload)) {
            // parse any stats, might be final key
            updateStats(payload);

            if (key.getColumnFamily().toString().equals("STATS")) {
                // fake key was generated to return stats, return null so the producer skips this key
                return null;
            }
        }

        // otherwise return a simple key with context;
        return new KeyWithContext(key, context, config.isSortedCandidateQueue());
    }

    private boolean isBulkContext(String payload) {
        return payload.contains(";");
    }

    private boolean isStats(String payload) {
        return payload.contains(":");
    }

    private void updateStats(String stats) {
        String[] parts = stats.split(":");

        DocumentIteratorStats iteratorStats = DocumentIteratorStats.fromString(parts[0]);
        config.getStats().merge(iteratorStats);

        DocIdQueryIteratorStats queryStats = DocIdQueryIteratorStats.fromString(parts[1]);
        config.getStats().merge(queryStats);
    }

    @Override
    public void setContext(String context) {
        this.runnableContext = context;
    }

    @Override
    public String getContext() {
        return runnableContext;
    }
}
