package datawave.ingest.table.aggregator;

/**
 * An extension of {@link GlobalIndexUidAggregator} that will always preserve "count only" Uid.List protobufs, even when their count goes below 1. A count only
 * protobuf is one where we have dropped the actual uid list due to exceeding the max list size. These protobufs will have their 'ignore' flag set.
 *
 * The intent of this iterator is to prevent accidental deletion of count only protobufs in case extra deletions occur.
 */
public class KeepCountOnlyUidAggregator extends GlobalIndexUidAggregator {

    public KeepCountOnlyUidAggregator(int max) {
        super(max);
    }

    public KeepCountOnlyUidAggregator() {
        super();
    }

    @Override
    public boolean propogateKey() {
        return isSeenIgnore() || super.propogateKey();
    }
}
