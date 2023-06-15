package datawave.metrics.iterators;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Map;

/**
 *
 */
public class CountersAggregatingIterator extends WrappingIterator implements OptionDescriber {
    private static String MAX_AGGREGATE_OPT = "maxAgg";
    private static Logger log = Logger.getLogger(CountersAggregatingIterator.class);
    private long maxRecordCount = -1;
    private Key topKey;
    private Value topValue;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        if (options.containsKey(MAX_AGGREGATE_OPT)) {
            try {
                maxRecordCount = Long.parseLong(options.get(MAX_AGGREGATE_OPT));
            } catch (NumberFormatException e) {
                log.error("Invalid value (" + options.get(MAX_AGGREGATE_OPT) + ") for " + MAX_AGGREGATE_OPT + " option: " + e.getMessage());
                throw e;
            }
        }
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        super.seek(range, columnFamilies, inclusive);
        findTop();
    }

    @Override
    public Key getTopKey() {
        if (topKey == null)
            return super.getTopKey();
        return topKey;
    }

    @Override
    public Value getTopValue() {
        if (topKey == null)
            return super.getTopValue();
        return topValue;
    }

    @Override
    public boolean hasTop() {
        return topKey != null || super.hasTop();
    }

    @Override
    public void next() throws IOException {
        if (topKey != null) {
            topKey = null;
            topValue = null;
        } else {
            super.next();
        }

        findTop();
    }

    private void findTop() throws IOException {
        // Iterate over the source until we exhaust it and/or reach the maximum number of records to aggregate.
        // While iterating, build up a combined counters from all the values we see along the way. Return that
        // counters as the top value, and the last seen source key (i.e., the last key included in the aggregate
        // count) as the top key.
        long recordCount = 0;
        Counters counters = new Counters();
        Counters workingCounter = new Counters();
        Key workKey = new Key();
        while (super.hasTop() && (maxRecordCount < 0 || recordCount < maxRecordCount)) {
            recordCount++;
            workKey.set(super.getTopKey());
            Value topValue = super.getTopValue();
            try {
                // Be careful -- don't use the findCounter(Enum) method if we're reusing the same counters object this way
                // since the cache of enum->Counter isn't cleared when new counters are read.
                workingCounter.readFields(ByteStreams.newDataInput(topValue.get()));
                counters.incrAllCounters(workingCounter);
            } catch (IOException e) {
                // We might not have a counters object in the value -- just ignore (and log)
                if (log.isTraceEnabled())
                    log.trace("Exception reading value for key " + workKey + ": " + e.getMessage(), e.getCause());
            }

            super.next();
        }
        if (recordCount > 0) {
            topKey = workKey;

            try {
                ByteArrayDataOutput bad = ByteStreams.newDataOutput();
                counters.write(bad);
                topValue = new Value(bad.toByteArray());
            } catch (IOException e) {
                // ignore -- can't happen from a byte output stream
            }
        }
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        CountersAggregatingIterator copy;
        try {
            copy = getClass().getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            // Shouldn't happen so just throw as a runtime exception
            throw new RuntimeException(e);
        }

        copy.setSource(getSource().deepCopy(env));
        copy.maxRecordCount = maxRecordCount;
        return copy;
    }

    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions io = new IteratorOptions("countersAgg", getClass().getSimpleName() + " aggregates Hadoop Counters objects in the value together", null,
                        null);
        io.addNamedOption(MAX_AGGREGATE_OPT,
                        "Indicates the max number of values to aggregate together before returning a value (-1, the default, is unlimited)");
        return io;
    }

    @Override
    public boolean validateOptions(Map<String,String> options) {
        boolean valid = (options == null || options.isEmpty());
        if (!valid && options.containsKey(MAX_AGGREGATE_OPT)) {
            try {
                Long.parseLong(options.get(MAX_AGGREGATE_OPT));
                valid = true;
            } catch (Exception e) {
                // ignore -- it's an invalid option
            }
        }
        return valid;
    }
}
