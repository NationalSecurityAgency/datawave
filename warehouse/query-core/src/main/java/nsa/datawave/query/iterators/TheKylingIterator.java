package nsa.datawave.query.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import nsa.datawave.core.iterators.EvaluatingIterator;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;

/**
 * An iterator that calls next() some number of times before actually seeking the source. This is intended to alleviate index lookups when seeking within some
 * RFile block.
 *
 */
@Deprecated
public class TheKylingIterator implements SortedKeyValueIterator<Key,Value> {
    private static final Logger log = Logger.getLogger(TheKylingIterator.class);
    
    public static final int MAX_CHECKS = 128;
    
    private Range scanRange;
    private int maxChecks = MAX_CHECKS;
    private SortedKeyValueIterator<Key,Value> src;
    private Key tk;
    private Value tv;
    
    /**
     * Sets this iterator's <code>scanRange</code> to null, as it signifies we've been reinitialized but not <code>seek</code>'d yet.
     */
    @Override
    public void init(SortedKeyValueIterator<Key,Value> src, Map<String,String> opts, IteratorEnvironment env) throws IOException {
        this.src = src;
        scanRange = null;
    }
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        TheKylingIterator itr = new TheKylingIterator();
        try {
            itr.init(src.deepCopy(env), null, env);
        } catch (IOException e) {
            log.warn("BARF");
        }
        return itr;
    }
    
    /**
     * Intercepts the <code>seek</code> call by higher level iterators. There are conditions that can cause different behaviors:
     *
     * 1) <code>scanRange</code> cannot be null. If this iterator's <code>scanRange</code> is null, that indicates that this iterator has been initialized but
     * not seek'd yet. This will cause the underlying iterator to be seek'd to the supplied range.
     *
     * 2) If the supplied range has a null start key, then we are seeking backwards, so the underlying iterator will be seek'ed with <code>range</code> and
     * <code>scanRange</code> will be set to <code>range</code>.
     *
     * 3) The <code>endKey</code> of the supplied range is different from that of the <code>scanRange</code>, which means that we need to let the lower level
     * iterators know of this change. The sub iterator will be seeked using <code>range</code> and <code>scanRange</code> will be set to <code>range</code>.
     *
     * 4) If 1-3 do not hold, we can safely emulate a <code>seek</code> by repeatedly calling <code>next</code>.
     */
    @Override
    public void seek(Range range, Collection<ByteSequence> colfs, boolean inclusive) throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("seek, Range: " + range);
            for (ByteSequence bs : colfs) {
                log.trace("seekColumnFamilies for this iterator: " + (new String(bs.toArray())).replaceAll(EvaluatingIterator.NULL_BYTE_STRING, "%00"));
            }
        }
        if (scanRange == null || range.getStartKey() == null || (scanRange.getEndKey() != null && !scanRange.getEndKey().equals(range.getEndKey()))) {
            // indicative of a first seek; so we should just accept this as our scan range
            src.seek(range, colfs, inclusive);
            next();
            scanRange = range;
            if (log.isTraceEnabled()) {
                log.trace("initial seek, top key: " + (src.hasTop() ? src.getTopKey() : "null"));
            }
        } else {
            Key minKey = range.getStartKey();
            if (src.hasTop() && range.contains(src.getTopKey())) {
                return;
            }
            for (int i = 0; i < maxChecks && src.hasTop(); ++i) {
                src.next();
                if (src.hasTop() && src.getTopKey().compareTo(minKey) >= 0) {
                    next();
                    return;
                }
            }
            if (src.hasTop()) {
                scanRange = range;
                src.seek(range, colfs, inclusive);
            }
        }
    }
    
    public Range scanRange() {
        return scanRange;
    }
    
    @Override
    public boolean hasTop() {
        return tk != null;
    }
    
    @Override
    public void next() throws IOException {
        if (src.hasTop()) {
            tk = src.getTopKey();
            tv = src.getTopValue();
            src.next();
        } else {
            tk = null;
            tv = null;
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
}
