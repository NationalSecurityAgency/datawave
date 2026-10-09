package datawave.iterators;

import org.apache.accumulo.core.data.Key;

import datawave.ingest.table.aggregator.TruncatingTimestampIterator;

/**
 * This is a propagating iterator that will truncate the key timestamps down to the beginning of the day. Currently, this iterator is only used on index tables.
 */
public class TotalAggregatingIterator extends PropagatingIterator {
    /**
     * The only difference with this iterator is that the timestamps are truncated
     */

    @Override
    public Key getTopKey() {
        return TruncatingTimestampIterator.getTruncatedTimestampKey(super.getTopKey());
    }

}
