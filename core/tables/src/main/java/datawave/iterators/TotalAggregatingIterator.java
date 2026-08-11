package datawave.iterators;

import org.apache.accumulo.core.data.Key;

import datawave.ingest.table.aggregator.TruncatingTimestampIterator;

/**
 *
 * This is a propogating iterator that will truncate the key timestamps down to the begininning of the day. Currently this iterator is only used on index
 * tables.
 *
 */

public class TotalAggregatingIterator extends PropogatingIterator {
    /**
     * The only difference with this iterator is that the timestamps are truncated
     */

    @Override
    public Key getTopKey() {
        return TruncatingTimestampIterator.getTruncatedTimestampKey(super.getTopKey());
    }

}
