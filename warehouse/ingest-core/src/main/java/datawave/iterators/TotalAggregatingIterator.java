package datawave.iterators;

/**
 * This iterator is a copy of org.apache.accumulo.core.iterators.AggregatingIterator. The difference is that this iterator does not need configuration. It will
 * aggregate all values with the same key (row, colf, colq, colVis.).
 *
 */

public class TotalAggregatingIterator extends PropogatingIterator {
    /**
     * All functionality is kept within propogating iterator. this class is kept so we do not break any other implementations
     */
}
