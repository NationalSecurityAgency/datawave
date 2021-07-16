package datawave.ingest.table.aggregator;

/**
 * An extension of {@link GlobalIndexUidAggregator} that will always preserve "count only" Uid.List protobufs, even when their count goes below 1. This iterator
 * will prevent the accidental deletion of count only protobufs in case extra deletions occur.
 */
public class KeepCountOnlyUidAggregator extends GlobalIndexUidAggregator {
    @Override
    public boolean propogateKey() {
        return isSeenIgnore() || super.propogateKey();
    }
}
