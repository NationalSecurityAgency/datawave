package datawave.iterators;

import datawave.edge.util.ExtendedHyperLogLogPlus;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import static datawave.edge.util.EdgeKey.EDGE_FORMAT.STATS;
import static datawave.edge.util.EdgeKey.STATS_TYPE.LINKS;

public class StatsLinksEdgeCombiner extends WrappingIterator implements OptionDescriber {
    private static final Logger LOG = LoggerFactory.getLogger(StatsLinksEdgeCombiner.class);
    private static final Text STATS_LINKS = new Text(STATS + "/" + LINKS + "/");
    private static final byte[] ERROR_EHLLP_BYTES;

    static {
        try {
            final ExtendedHyperLogLogPlus ehllp = new ExtendedHyperLogLogPlus();

            ERROR_EHLLP_BYTES = ehllp.getBytes();
        } catch (final IOException e) {
            throw (new RuntimeException("Unable to initialize ExtendedHyperLogLogPlus", e));
        }
    }

    private Key topKey;
    private Value topValue;

    @Override
    public Key getTopKey() {
        return ((topKey == null) ? super.getTopKey() : topKey);
    }

    @Override
    public Value getTopValue() {
        return ((topValue == null) ? super.getTopValue() : topValue);
    }

    @Override
    public boolean hasTop() {
        return (topKey != null) || super.hasTop();
    }

    @Override
    public void next() throws IOException {
        if (null == topKey) {
            super.next();
        } else {
            topKey = null;
            topValue = null;
        }

        findTop();
    }

    private final Key workKey = new Key();
    private final Text workColumnFamily = new Text();

    /**
     * Sets the topKey and topValue based on the top key of the source. If the column of the source top key is in the set of combiners, topKey will be the top
     * key of the source and topValue will be the result of the reduce method. Otherwise, topKey and topValue will be unchanged. (They are always set to null
     * before this method is called.)
     */
    private void findTop() {
        // check if aggregation is needed
        if (super.hasTop()) {
            workKey.set(super.getTopKey());

            if (workKey.isDeleted()) {
                return;
            }

            // FIll workColumnFamily with current column family
            workKey.getColumnFamily(workColumnFamily);

            // Only combine STATS/LINKS edges
            if (startsWith(workColumnFamily, STATS_LINKS)) {
                topKey = workKey;
                topValue = combineStatsLinksEdgeValues(getSource());
            }
        }
    }

    private Value combineStatsLinksEdgeValues(final SortedKeyValueIterator<Key,Value> sortedKeyValueIterator) {
        return (combineStatsLinksEdgeValues(topKey, new ValueIterator(sortedKeyValueIterator)));
    }

    public static Value combineStatsLinksEdgeValues(final Object key, final Iterator<Value> valueIterator) {
        final ExtendedHyperLogLogPlus ehllp = new ExtendedHyperLogLogPlus();

        while (valueIterator.hasNext()) {
            try {
                ehllp.addAll(new ExtendedHyperLogLogPlus(valueIterator.next()));
            } catch (final IOException e) {
                LOG.error("Failed to add the hyperloglog value for {}", key);
            }
        }

        try {
            return (new Value(ehllp.getBytes()));
        } catch (final IOException e) {
            LOG.error("Failed to build the value for  {}", key);
        }

        // This "should" never be returned, but just in case
        return (new Value(ERROR_EHLLP_BYTES));
    }

    private static boolean startsWith(final Text workColumnFamily2, final Text statsLinks) {
        return (WritableComparator.compareBytes(workColumnFamily2.getBytes(), 0, statsLinks.getLength(), statsLinks.getBytes(), 0,
                        statsLinks.getLength()) == 0);
    }

    @Override
    public void seek(final Range range, final Collection<ByteSequence> columnFamilies, final boolean inclusive) throws IOException {
        // do not want to seek to the middle of a value that should be combined...
        final Range seekRange = IteratorUtil.maximizeStartKeyTimeStamp(range);

        super.seek(seekRange, columnFamilies, inclusive);

        findTop();

        if (range.getStartKey() != null) {
            while (hasTop() && getTopKey().equals(range.getStartKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS)
                            && (getTopKey().getTimestamp() > range.getStartKey().getTimestamp())) {
                // the value has a more recent time stamp, so pass it up
                next();
            }

            while (hasTop() && range.beforeStartKey(getTopKey())) {
                next();
            }
        }
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(final IteratorEnvironment env) {
        try {
            final StatsLinksEdgeCombiner newInstance = this.getClass().newInstance();
            newInstance.setSource(getSource().deepCopy(env));

            return (newInstance);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public IteratorOptions describeOptions() {
        // No custom options

        return (new IteratorOptions("statsLinksEdgeCOmbiner", "Combines multiple STATS/LINKS edges with equal keys", null, null));
    }

    @Override
    public boolean validateOptions(final Map<String,String> options) {
        // No custom options to validate
        return (true);
    }

    /**
     * A Java Iterator that iterates over the Values for a given Key from a source SortedKeyValueIterator.
     */
    public static class ValueIterator implements Iterator<Value> {
        private final Key topKey;
        private final SortedKeyValueIterator<Key,Value> source;
        private boolean hasNext;

        /**
         * Constructs an iterator over Values whose Keys are versions of the current topKey of the source SortedKeyValueIterator.
         *
         * @param source
         *            The {@code SortedKeyValueIterator<Key,Value>} from which to read data.
         */
        public ValueIterator(final SortedKeyValueIterator<Key,Value> source) {
            this.source = source;
            topKey = new Key(source.getTopKey());
            hasNext = _hasNext();
        }

        private boolean _hasNext() {
            return (source.hasTop() && !source.getTopKey().isDeleted() && topKey.equals(source.getTopKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS));
        }

        @Override
        public boolean hasNext() {
            return (hasNext);
        }

        @Override
        public Value next() {
            if (!hasNext) {
                throw (new NoSuchElementException());
            }

            final Value topValue = new Value(source.getTopValue());
            try {
                source.next();
                hasNext = _hasNext();
            } catch (final IOException e) {
                throw (new RuntimeException(e));
            }

            return (topValue);
        }

        /**
         * This method is unsupported in this iterator.
         *
         * @throws UnsupportedOperationException
         *             when called
         */
        @Override
        public void remove() {
            throw (new UnsupportedOperationException());
        }
    }
}
