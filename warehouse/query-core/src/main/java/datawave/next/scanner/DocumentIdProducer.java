package datawave.next.scanner;

import static datawave.next.DocIdQueryIterator.STATS;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
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
import datawave.query.iterator.QueryOptions;
import datawave.scan.ScannerBuilder;

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

    private void executeScan() throws TableNotFoundException, InterruptedException, IOException {
        try (Scanner scanner = createScanner()) {
            boolean offered;
            for (Map.Entry<Key,Value> entry : scanner) {
                Key key = entry.getKey();
                Value value = entry.getValue();
                KeyWithContext keyWithContext = parseEntry(key, value);

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

    /**
     * Create a scanner for the field index and configure it with an execution hint and consistency level.
     * <p>
     * This is a search scan, so it is governed by the search hint rather than the retrieval hint. Both are required, because the document scheduler relies on
     * its scans being routed to a dedicated executor pool.
     *
     * @return a configured scanner
     * @throws TableNotFoundException
     *             if the table does not exist
     */
    protected Scanner createScanner() throws TableNotFoundException {
        // this check exists because datawave can produce day ranges for certain unit tests. The document scheduler is optimized for shard-specific plans and
        // thus is not compatible with day ranges.
        Range scanRange = Range.exact(range.getStartKey().getRow());
        if (!scanRange.equals(range)) {
            log.warn("prev: {}", range);
            log.warn("next: {}", scanRange);
            throw new RuntimeException("Scan range differed from input range");
        }

        String tableName = context.getTableName();

        Preconditions.checkNotNull(tableName);
        Preconditions.checkNotNull(config.getSearchScanHintTable(), "SearchScanHintTable cannot be null");
        Preconditions.checkNotNull(config.getSearchExecutorPool(), "SearchExecutorPool cannot be null");
        Preconditions.checkArgument(tableName.equals(config.getSearchScanHintTable()), "Table name did not match execution hint");
        Preconditions.checkNotNull(config.getSearchConsistencyLevel(), "SearchConsistencyLevel cannot be null");

        //  @formatter:off
        ScannerBuilder builder = ScannerBuilder.create(config.getClient())
                .setTableName(tableName)
                .setAuthorizations(config.getAuthorizations())
                .setConsistencyLevel(config.getSearchConsistencyLevel())
                .setScanType(config.getSearchScanHintPool())
                .setScanPriority(1);
        //  @formatter:on

        Scanner scanner = builder.build();
        scanner.setRange(range);
        scanner.addScanIterator(createIteratorSetting());
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

    CandidateResultSerializer serializer = new CandidateResultSerializer();

    private KeyWithContext parseEntry(Key key, Value value) throws IOException {
        String cf = key.getColumnFamily().toString();

        if (cf.equals(STATS)) {
            byte[] payload = value.get();
            if (payload.length == 0) {
                return null;
            }
            CandidateResult result = serializer.deserialize(payload);
            config.getStats().merge(result.getQueryStats());
            config.getStats().merge(result.getIterStats());
            // STATS key only passes stats, no candidates exist
            return null;
        }

        if (config.getCandidateBatchSize() == 1) {
            return new KeyWithContext(key, context, config.isSortedCandidateQueue());
        }

        // else bulk results
        byte[] payload = value.get();
        CandidateResult result = serializer.deserialize(payload);
        if (result.getQueryStats() != null) {
            // final batch of results will also send back iterator stats
            config.getStats().merge(result.getQueryStats());
            config.getStats().merge(result.getIterStats());
        }

        return new BulkKeyWithContext(key, result.getCandidates(), context, config.isSortedCandidateQueue());
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
