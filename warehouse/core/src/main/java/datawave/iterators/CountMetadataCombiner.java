package datawave.iterators;

import datawave.data.MetadataCardinalityCounts;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.log4j.Logger;

import java.util.Iterator;

/**
 * Combines count metadata with different values.
 *
 */
public class CountMetadataCombiner extends Combiner {

    private static final Logger log = Logger.getLogger(CountMetadataCombiner.class);

    /**
     * Reduces a list of Values into a single Value.
     *
     * @param key
     *            The most recent version of the Key being reduced.
     *
     * @param iter
     *            An iterator over the Values for different versions of the key.
     *
     * @return The combined Value.
     */
    @Override
    public Value reduce(Key key, Iterator<Value> iter) {

        MetadataCardinalityCounts counts = null;
        Value singletonValue = null;

        while (iter.hasNext()) {
            Value value = iter.next();
            try {
                MetadataCardinalityCounts newCounts = new MetadataCardinalityCounts(key, value);
                if (counts == null) {
                    counts = newCounts;
                    singletonValue = value;
                } else {
                    if (log.isTraceEnabled()) {
                        log.trace("Merging " + counts + " with " + newCounts);
                    }
                    counts.merge(newCounts);
                    if (log.isTraceEnabled()) {
                        log.trace("Resulted in " + counts);
                    }
                    singletonValue = null;
                }
            } catch (Exception e) {
                log.error("Unable to decode counts from " + key + " / " + value);
            }
        }

        if (singletonValue != null) {
            return singletonValue;
        } else if (counts != null) {
            return counts.getValue();
        } else {
            return new Value();
        }
    }

}
