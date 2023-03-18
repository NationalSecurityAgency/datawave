package datawave.query.jexl.functions;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Collection;

public abstract class SeekingAggregator implements FieldIndexAggregator {
    private int maxNextCount = -1;
    
    public SeekingAggregator(int maxNextCount) {
        this.maxNextCount = maxNextCount;
    }
    
    @Override
    public Key apply(SortedKeyValueIterator<Key,Value> itr, Range current, Collection<ByteSequence> columnFamilies, boolean includeColumnFamilies)
                    throws IOException {
        Key currentKey = itr.getTopKey();
        ByteSequence pointer = parsePointer(currentKey);
        Key result = getResult(currentKey, pointer);
        
        advanceItr(itr, pointer, current, columnFamilies, includeColumnFamilies);
        
        return result;
    }
    
    /**
     * Advance an iterator until skip(...) returns false. May be a combination of seek() and next() calls
     * 
     * @param itr
     *            the iterator
     * @param pointer
     *            a byte sequence pointer
     * @param currentRange
     *            the current range
     * @param columnFamilies
     *            the column families
     * @param includeColumnFamilies
     *            flag to include column families
     * @throws IOException
     *             for issues with read/write
     */
    protected void advanceItr(SortedKeyValueIterator<Key,Value> itr, ByteSequence pointer, Range currentRange, Collection<ByteSequence> columnFamilies,
                    boolean includeColumnFamilies) throws IOException {
        Key current = itr.getTopKey();
        Text row = current.getRow();
        int nextCount = 0;
        while (current != null && skip(current, row, pointer)) {
            if (maxNextCount == -1 || nextCount < maxNextCount) {
                itr.next();
                nextCount++;
            } else {
                Key startKey = getSeekStartKey(current, pointer);
                Range newRange = new Range(startKey, false, currentRange.getEndKey(), currentRange.isEndKeyInclusive());
                itr.seek(newRange, columnFamilies, includeColumnFamilies);
                nextCount = 0;
            }
            
            current = itr.hasTop() ? itr.getTopKey() : null;
        }
    }
    
    /**
     * Produce a pointer from the current key
     * 
     * @param current
     *            the current key
     * @return non-null ByteSequence
     */
    protected abstract ByteSequence parsePointer(Key current);
    
    /**
     * Produce the result for an apply()
     * 
     * @param current
     *            the initial Key from the iterator
     * @param pointer
     *            the pointer produced by calling parsePointer(current)
     * @return a result key
     */
    protected abstract Key getResult(Key current, ByteSequence pointer);
    
    /**
     * Test a Key against initial row and pointer values to determine if it should be skipped
     * 
     * @param next
     *            the key to test
     * @param row
     *            the initial row
     * @param pointer
     *            the initial pointer from parsePointer
     * @return true if the key should be skipped, false otherwise
     */
    protected abstract boolean skip(Key next, Text row, ByteSequence pointer);
    
    /**
     * get the Key to produce the next seek range. Key should be exclusive
     * 
     * @param current
     *            current key
     * @param pointer
     *            pointer produced from parsePointer
     * @return non-null Key that will be used to seek the current iterator exclusively
     */
    protected abstract Key getSeekStartKey(Key current, ByteSequence pointer);
}
