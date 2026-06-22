package datawave.test.iter;

import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
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
 * An iterator that advances an internal clock on next and seek calls by a specified amount.
 * <p>
 * See {@link ClockIteratorTest} for instrumentation examples.
 */
public class ClockIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {

    public static final String ADVANCE_ON_NEXT = "advanceOnNext";
    public static final String ADVANCE_ON_SEEK = "advanceOnSeek";
    public static final String ADVANCE_ON_BOTH = "advanceOnBoth";
    public static final String ADVANCE_AMOUNT = "advanceAmount";

    private Key tk;
    private Value tv;

    private SortedKeyValueIterator<Key,Value> source;

    private boolean advanceOnNext;
    private boolean advanceOnSeek;
    private boolean advanceOnBoth;
    private long advanceAmount;

    // this cannot be final despite what your IDE may say.
    private static Clock clock = Clock.systemUTC();

    /**
     * Advance the lock by the {@link #advanceAmount}
     */
    private void advanceClock() {
        long current = clock.millis();
        when(clock.millis()).thenReturn(current + advanceAmount);
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        this.source = source;
        validateOptions(options);
    }

    @Override
    public boolean hasTop() {
        return tk != null;
    }

    @Override
    public void next() throws IOException {
        if (advanceOnNext || advanceOnBoth) {
            advanceClock();
        }
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
        if (advanceOnSeek || advanceOnBoth) {
            advanceClock();
        }
        source.seek(range, columnFamilies, inclusive);
        next();
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
        ClockIterator copy = new ClockIterator();
        copy.source = source.deepCopy(env);
        copy.advanceOnNext = advanceOnNext;
        copy.advanceOnSeek = advanceOnSeek;
        copy.advanceOnBoth = advanceOnBoth;
        copy.advanceAmount = advanceAmount;
        return copy;
    }

    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions options = new IteratorOptions(getClass().getName(), "An iterator that advances a mock clock", null, null);
        options.addNamedOption(ADVANCE_ON_NEXT, "Flag to advance the clock on every next call");
        options.addNamedOption(ADVANCE_ON_SEEK, "Flag to advance the clock on every seek call");
        options.addNamedOption(ADVANCE_ON_BOTH, "Flag to advance the clock on every next and seek call");
        options.addNamedOption(ADVANCE_AMOUNT, "The number of milliseconds the clock is advanced");
        return options;
    }

    @Override
    public boolean validateOptions(Map<String,String> options) {
        String option = options.get(ADVANCE_ON_NEXT);
        if (option != null) {
            advanceOnNext = Boolean.parseBoolean(option);
        }

        option = options.get(ADVANCE_ON_SEEK);
        if (option != null) {
            advanceOnSeek = Boolean.parseBoolean(option);
        }

        option = options.get(ADVANCE_ON_BOTH);
        if (option != null) {
            advanceOnBoth = Boolean.parseBoolean(option);
        }

        if (!(advanceOnNext || advanceOnSeek || advanceOnBoth)) {
            throw new IllegalStateException("At least one advance option must be specified");
        }

        option = options.get(ADVANCE_AMOUNT);
        if (option == null) {
            throw new IllegalStateException("ADVANCE_AMOUNT option must be specified");
        } else {
            advanceAmount = Long.parseLong(option);
        }

        return true;
    }
}
