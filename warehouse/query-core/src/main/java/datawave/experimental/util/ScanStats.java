package datawave.experimental.util;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

/**
 * Scan stats provide some insight into how the {@link datawave.experimental.executor.QueryExecutor} performs across several categories
 * <p>
 * Low level iterator stats
 * <ul>
 * <li>next count</li>
 * <li>seek count</li>
 * <li>keys traversed</li>
 * <li>keys returned</li>
 * </ul>
 * <p>
 * Document stats
 * <ul>
 * <li>documents evaluated</li>
 * <li>documents returned</li>
 * </ul>
 * <p>
 * Shard Stats
 * <ul>
 * <li>Number of shards searched</li>
 * <li>Number of shards with zero results</li>
 * <li>Min/Max/Avg documents considered per shard</li>
 * <li>Min/Max/Avg documents evaluated per shard</li>
 * <li>Min/Max/Avg documents returned per shard</li>
 * </ul>
 * <p>
 * Timing Stats
 * <ul>
 * <li>field index, time to find candidate documents</li>
 * <li>index only, time to aggregate index-only fields</li>
 * <li>event, time to aggregate event data</li>
 * <li>tf, time to aggregate term offsets</li>
 * <li>potentially other stats like evaluation time, offer time, etc</li>
 * </ul>
 *
 */
public class ScanStats {

    private final AtomicLong documentsEvaluated = new AtomicLong(); // documents seen
    private final AtomicLong documentsReturned = new AtomicLong(); // documents matched

    private final AtomicLong shardsSearched = new AtomicLong(); // shards searched

    public ScanStats() {
        // empty constructor
    }

    public ScanStats merge(ScanStats other) {
        this.documentsEvaluated.addAndGet(other.documentsEvaluated.get());
        this.documentsReturned.addAndGet(other.documentsReturned.get());
        this.shardsSearched.addAndGet(other.shardsSearched.get());
        return this;
    }

    public void incrementDocumentsEvaluated() {
        documentsEvaluated.getAndIncrement();
    }

    public void incrementDocumentsReturned() {
        documentsReturned.getAndIncrement();
    }

    public void incrementShardsSearched() {
        shardsSearched.getAndIncrement();
    }

    public void logMinimizedStats(Logger log) {
        log.info("docs.evaluated: " + documentsEvaluated.get() + " docs.returned: " + documentsReturned.get() + " shards.searched: " + shardsSearched.get());
    }

    public void logStats(Logger log) {
        log.info("docs.evaluated: " + documentsEvaluated.get());
        log.info("docs.returned: " + documentsReturned.get());
        log.info("shards.searched: " + shardsSearched.get());
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("docs.evaluated: ").append(documentsEvaluated.get()).append('\n');
        sb.append("docs.returned: ").append(documentsReturned.get()).append('\n');
        sb.append("shards.searched: ").append(shardsSearched.get());
        return sb.toString();
    }

}
