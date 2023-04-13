package datawave.ingest.table.aggregator;

/**
 * This is like the {@link KeepCountOnlyUidAggregator} except it never preserves the uid list.
 * <p>
 * An extension of {@link GlobalIndexUidAggregator} that will always preserve "count only" Uid.List protobufs, even when their count goes below 1. A count only
 * protobuf is one where we have dropped the actual uid list due to exceeding the max list size. These protobufs will have their 'ignore' flag set.
 * <p>
 * The intent of this iterator is to prevent accidental deletion of count only protobufs in case extra deletions occur.
 */
public class KeepCountOnlyNoUidAggregator extends GlobalIndexUidAggregator {
    
    public KeepCountOnlyNoUidAggregator(int max) {
        super(0);
    }
    
    public KeepCountOnlyNoUidAggregator() {
        super(0);
    }
    
    @Override
    public boolean propogateKey() {
        return isSeenIgnore() || super.propogateKey();
    }
}
