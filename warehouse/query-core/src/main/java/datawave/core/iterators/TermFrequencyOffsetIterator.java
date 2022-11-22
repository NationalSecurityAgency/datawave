package datawave.core.iterators;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Alternate implementation of the {@link TermFrequencyIterator} that operates on precomputed column qualifiers.
 *
 * Term Frequency keys take the form {shard:tf:datatype\0uid\0value\0field}.
 *
 * This iterator fetches offsets for known datatype, uid, value, and field combinations. It does not support fetching offsets for a partial column qualifier.
 */
public class TermFrequencyOffsetIterator implements SortedKeyValueIterator<Key,Value> {
    
    private static final Logger log = Logger.getLogger(TermFrequencyOffsetIterator.class);
    
    private Key tk = null;
    private Value tv = null;
    
    // A set of full column qualifiers that describe the offset's exact location (datatype\0uid\0value\0FIELD)
    private final SortedSet<Text> searchSpace;
    
    private Range initialRange;
    private static final Collection<ByteSequence> seekCFs = Collections.singleton(new ArrayByteSequence("tf".getBytes()));
    
    private SortedKeyValueIterator<Key,Value> source;
    
    public TermFrequencyOffsetIterator(SortedSet<Text> searchSpace) {
        this.searchSpace = searchSpace;
    }
    
    public TermFrequencyOffsetIterator(TermFrequencyOffsetIterator other, IteratorEnvironment env) {
        this.source = other.source.deepCopy(env);
        this.searchSpace = other.searchSpace;
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        this.source = source;
    }
    
    @Override
    public void next() throws IOException {
        this.tk = null;
        this.tv = null;
        if (source.hasTop()) {
            source.next();
            findTop();
        }
    }
    
    @Override
    public void seek(Range range, Collection<ByteSequence> cfs, boolean inclusive) throws IOException {
        this.initialRange = new Range(range);
        this.tk = null;
        this.tv = null;
        
        source.seek(range, seekCFs, true);
        findTop();
    }
    
    public void findTop() throws IOException {
        this.tk = null;
        this.tv = null;
        Key key;
        while (true) {
            // If the source iterator ran out keys we're done.
            if (!source.hasTop()) {
                break;
            }
            
            key = source.getTopKey();
            
            // If the source iterator has keys outside our range, we're done.
            if (!initialRange.contains(key)) {
                break;
            }
            
            if (searchSpace.contains(key.getColumnQualifier())) {
                this.tk = key;
                this.tv = source.getTopValue();
                break;
            } else {
                // Always seek, for now.
                if (!seekToNextHit(key)) {
                    break;
                }
            }
        }
    }
    
    /**
     * Seek to the next known hit given our pre-built search space
     *
     * @param key
     *            the current tf key
     * @return true if there are more hits to scan
     * @throws IOException
     *             if there is a problem while seeking
     */
    private boolean seekToNextHit(Key key) throws IOException {
        Range seekRange = getNextSeekRange(key);
        if (seekRange == null)
            return false;
        source.seek(seekRange, seekCFs, true);
        return true;
    }
    
    /**
     * Given a TF key, find the next highest search space via column qualifier; the {datatype\0uid\0value\0FIELD}
     *
     * @param key
     *            the current tf key
     * @return the next highest range as defined by the search space, or null if the search space is exhausted
     */
    private Range getNextSeekRange(Key key) {
        Text nextCQ = ((TreeSet<Text>) searchSpace).higher(key.getColumnQualifier());
        if (nextCQ == null)
            return null;
        
        // Update the start key's column qualifier
        Key start = initialRange.getStartKey();
        start = new Key(start.getRow(), start.getColumnFamily(), nextCQ);
        
        // Update the initial range with the new start key
        return new Range(start, true, initialRange.getEndKey(), initialRange.isEndKeyInclusive());
    }
    
    // other overrides
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new TermFrequencyOffsetIterator(this, env);
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
    public boolean hasTop() {
        return (tk != null);
    }
}
