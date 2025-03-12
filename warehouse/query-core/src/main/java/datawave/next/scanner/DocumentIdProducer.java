package datawave.next.scanner;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.core.query.configuration.QueryData;
import datawave.next.async.RunnableWithContext;
import datawave.next.stats.DocIdQueryIteratorStats;
import datawave.next.stats.DocumentIteratorStats;

/**
 * A runnable that handles async scanning of a tablet to find document candidates.
 */
public class DocumentIdProducer implements RunnableWithContext {

    private static final Logger log = LoggerFactory.getLogger(DocumentIdProducer.class);

    private final DocumentScannerConfig config;
    private final long candidateQueueOfferTimeMillis;
    private final BlockingQueue<KeyWithContext> candidateQueue;
    private final Scanner scanner;
    private final QueryData context;
    private final AtomicInteger numFiScans;

    private String runnableContext;

    public DocumentIdProducer(DocumentScannerConfig config, Scanner scanner, QueryData context) {
        this.config = config;
        this.scanner = scanner;
        this.context = context;
        this.candidateQueueOfferTimeMillis = this.config.getCandidateQueueOfferTimeMillis();
        this.candidateQueue = this.config.getDocIdQueue();
        this.numFiScans = this.config.getNumFiScans();
    }

    @Override
    public void run() {
        try {
            Thread.currentThread().setName(getContext());
            if (log.isDebugEnabled()) {
                log.debug("scanning shard {} for document ids", context.getRanges().iterator().next().getStartKey());
            }
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

        } catch (Exception e) {
            log.error("exception found while scanning the field index", e);
        } finally {
            numFiScans.getAndDecrement();
            scanner.close();
        }
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
