package datawave.next.scanner;

import java.util.Map;
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
    private final BlockingQueue<KeyWithContext> candidateQueue;
    private final Scanner scanner;
    private final QueryData context;
    private final AtomicInteger numFiScans;

    private String runnableContext;

    public DocumentIdProducer(DocumentScannerConfig config, Scanner scanner, QueryData context) {
        this.config = config;
        this.scanner = scanner;
        this.context = context;
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

                // check here for a final document key or stats key
                if (entry.getValue().getSize() > 0) {
                    String stat = entry.getValue().toString();
                    String[] parts = stat.split(":");

                    DocumentIteratorStats iteratorStats = DocumentIteratorStats.fromString(parts[0]);
                    config.getStats().merge(iteratorStats);

                    DocIdQueryIteratorStats queryStats = DocIdQueryIteratorStats.fromString(parts[1]);
                    config.getStats().merge(queryStats);

                    if (key.getColumnFamily().toString().equals("STATS")) {
                        // skip this key
                        continue;
                    }
                }

                offered = false;
                KeyWithContext keyWithContext = new KeyWithContext(key, context, config.isSortedCandidateQueue());
                while (!offered) {
                    offered = candidateQueue.offer(keyWithContext, 500, TimeUnit.MILLISECONDS);
                }
            }

        } catch (Exception e) {
            log.error("exception found while scanning the field index", e);
        } finally {
            numFiScans.getAndDecrement();
            scanner.close();
        }
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
