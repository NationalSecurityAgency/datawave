package datawave.test.iter;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * A sorted key value iterator that iterates very slowly, according to the specified delay interval
 */
public class DelayIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {

    public static String DELAY_MILLIS = "delay";

    private Key tk;
    private Value tv;

    private SortedKeyValueIterator<Key,Value> source;

    private long delay = 0L;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        this.source = source;
        validateOptions(options);
    }

    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions options = new IteratorOptions(getClass().getName(), "Delayed iteration", null, null);
        options.addNamedOption(DELAY_MILLIS, "The time in milliseconds to delay between each next and seek call");
        return options;
    }

    @Override
    public boolean validateOptions(Map<String,String> map) {
        String option = map.get(DELAY_MILLIS);
        if (option == null) {
            throw new IllegalStateException("Missing option: " + DELAY_MILLIS);
        } else {
            delay = Long.parseLong(option);
        }

        return true;
    }

    @Override
    public boolean hasTop() {
        return tk != null;
    }

    @Override
    public void next() throws IOException {
        delayIteration();
        tk = null;
        tv = null;
        if (source.hasTop()) {
            tk = source.getTopKey();
            tv = source.getTopValue();
            source.next();
        }
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        delayIteration();
        source.seek(range, columnFamilies, inclusive);
        next();
    }

    private void delayIteration() {
        if (delay > 0) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public Key getTopKey() {
        return tk;
    }

    @Override
    public Value getTopValue() {
        return tv;
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        DelayIterator copy = new DelayIterator();
        copy.source = source.deepCopy(env);
        return copy;
    }

}
