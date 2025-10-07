package datawave.query.index.stats;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * Searches a subsection of the row, and optionally columnfamily, and returns KVs within that subsection.
 *
 */
public class MinMaxIterator implements SortedKeyValueIterator<Key,Value> {
    public static final String MIN_OPT = "mmi.min";
    public static final String MAX_OPT = "mmi.max";

    private static final Logger log = Logger.getLogger(MinMaxIterator.class);
    private static final Set<ByteSequence> EMPTY_COLFAMS = Collections.emptySet();

    private SortedKeyValueIterator<Key,Value> src;
    private Key matchingKey;
    private Value topValue;
    private Text min;
    private Text max;
    private Range range;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        src = source;

        String min = options.get(MIN_OPT);
        if (min != null) {
            this.min = new Text(min);
        } else {
            throw new IOException(new IllegalArgumentException("mmi.min must be set for MinMaxIterator."));
        }

        String max = options.get(MAX_OPT);
        if (max != null) {
            this.max = new Text(max);
        } else {
            throw new IOException(new IllegalArgumentException("mmi.max must be set for MinMaxIterator."));
        }

        log.trace("MinMaxIterator initialized with [min=" + min + ", max=" + max + "]");
    }

    @Override
    public boolean hasTop() {
        return matchingKey != null;
    }

    private final Text currCF = new Text();

    @Override
    public void next() throws IOException {
        log.trace("next()");
        matchingKey = null;
        while (src.hasTop()) {
            src.getTopKey().getColumnFamily(currCF);

            int relToMax = currCF.compareTo(max);
            int relToMin = currCF.compareTo(min);

            if (relToMax > 0) {
                log.trace("Passed max value-- seeking to next row.");
                Key followingRow = src.getTopKey().followingKey(PartialKey.ROW);
                if (this.range.contains(followingRow)) {
                    src.seek(new Range(new Key(followingRow.getRow(), min, new Text()), true, this.range.getEndKey(), this.range.isEndKeyInclusive()),
                                    EMPTY_COLFAMS, false);
                } else {
                    log.trace("We're done!");
                    return;
                }
            } else if (relToMin < 0) {
                log.trace("Before min value-- seeking.");
                Key nextKey = new Key(src.getTopKey().getRow(), min, new Text());
                if (this.range.contains(nextKey)) {
                    Range seekTo = new Range(new Key(src.getTopKey().getRow(), min, new Text()), true, this.range.getEndKey(), this.range.isEndKeyInclusive());
                    src.seek(seekTo, EMPTY_COLFAMS, false);
                } else {
                    log.trace("We're done!");
                    return;
                }
            } else if (relToMin >= 0 && relToMax <= 0) {
                if (log.isTraceEnabled()) {
                    log.trace("Setting matching key to " + src.getTopKey().toStringNoTime());
                }
                matchingKey = src.getTopKey();
                topValue = src.getTopValue();
                src.next();
                return;
            }
        }
    }

    /**
     * Calls reseek but does work to set the top key.
     */
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        log.trace("seek()");
        this.range = range;
        src.seek(range, columnFamilies, inclusive);
        next();
    }

    @Override
    public Key getTopKey() {
        if (log.isTraceEnabled()) {
            log.trace("Returning " + matchingKey.toStringNoTime());
        }
        return matchingKey;
    }

    @Override
    public Value getTopValue() {
        return topValue;
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        MinMaxIterator mmi = new MinMaxIterator();
        mmi.src = src.deepCopy(env);
        return mmi;
    }
}
