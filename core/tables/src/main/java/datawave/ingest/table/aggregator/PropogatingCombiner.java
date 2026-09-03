package datawave.ingest.table.aggregator;

import java.util.Iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;

import datawave.iterators.ValueCombiner;

/**
 *
 * Aids in determining if a value and the corresponding keys should propogate
 *
 */
public abstract class PropogatingCombiner extends Combiner {

    private static final Value EMPTY_VALUE = new Value(new byte[0]);

    private static final Logger log = Logger.getLogger(PropogatingCombiner.class);

    /**
     * Flag to determine if we propogate the removals
     */
    protected boolean propogate = true;

    /**
     * Get the iterator of values to be combined
     *
     * @param iterator
     * @return a value iterator
     */
    public Iterator<Value> getValues(SortedKeyValueIterator<Key,Value> iterator) {
        return new ValueCombiner(iterator);
    }

    /**
     * Shpuld return a thread safe value.
     *
     * @return a value
     */
    public Value aggregate() {
        return EMPTY_VALUE;
    }

    /**
     * Collects a value into the current state;
     *
     * @param value
     *            a value
     */
    public void collect(Value value) {

    }

    /**
     * Sets the propogating factor in the aggregator.
     *
     * @param propogate
     *            the boolean to set
     */
    public void setPropogate(boolean propogate) {
        this.propogate = propogate;
    }

    /**
     * Determines whether or not to propogate the key depending on the result of the value
     *
     * @return a boolean on whether to propogate
     */
    public boolean propogateKey() {
        return this.propogate;
    }

    /**
     * Method to reset the state within the propogating aggregator.
     */
    public void reset() {
        // empty method
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.accumulo.core.iterators.Combiner#reduce(org.apache.accumulo.core.data.Key, java.util.Iterator)
     */
    @Override
    public Value reduce(Key key, Iterator<Value> iter) {
        if (log.isTraceEnabled()) {
            log.trace("combining key " + key + ", iter.hasNext? " + iter.hasNext());
        }
        while (iter.hasNext()) {
            collect(iter.next());
        }
        return aggregate();
    }

}
