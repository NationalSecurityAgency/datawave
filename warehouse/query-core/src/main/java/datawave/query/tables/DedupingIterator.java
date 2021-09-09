package datawave.query.tables;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map.Entry;

class DedupingIterator implements Iterator<Entry<Key,Value>> {
    static final int BLOOM_EXPECTED_DEFAULT = 500000;
    static final double BLOOM_FPP_DEFAULT = 1e-15;
    
    private Iterator<Entry<Key,Value>> delegate;
    private Entry<Key,Value> next;
    private BloomFilter<byte[]> bloom = null;
    
    public DedupingIterator(Iterator<Entry<Key,Value>> iterator, int bloomFilterExpected, double bloomFilterFpp) {
        this.delegate = iterator;
        this.bloom = BloomFilter.create(new ByteFunnel(), bloomFilterExpected, bloomFilterFpp);
        getNext();
    }
    
    public DedupingIterator(Iterator<Entry<Key,Value>> iterator) {
        this(iterator, BLOOM_EXPECTED_DEFAULT, BLOOM_FPP_DEFAULT);
    }
    
    private void getNext() {
        next = null;
        while (next == null && delegate.hasNext()) {
            next = delegate.next();
            if (isDuplicate(next)) {
                next = null;
            }
        }
    }
    
    private byte[] getBytes(Entry<Key,Value> entry) {
        ByteSequence row = entry.getKey().getRowData();
        ByteSequence cf = entry.getKey().getColumnFamilyData();
        
        // only append the last 2 tokens (the datatype and uid)
        // we are expecting that they may be prefixed with a count (see sortedUIDs in the DefaultQueryPlanner / QueryIterator)
        int nullCount = 0;
        int index = -1;
        for (int i = 0; i < cf.length() && nullCount < 2; i++) {
            if (cf.byteAt(i) == 0) {
                nullCount++;
                if (index == -1) {
                    index = i;
                }
            }
        }
        int dataTypeOffset = index + 1;
        int offset = cf.offset() + dataTypeOffset;
        int length = cf.length() - dataTypeOffset;
        
        byte[] bytes = new byte[row.length() + length + 1];
        System.arraycopy(row.getBackingArray(), row.offset(), bytes, 0, row.length());
        System.arraycopy(cf.getBackingArray(), offset, bytes, row.length() + 1, length);
        return bytes;
    }
    
    @Override
    public boolean hasNext() {
        return next != null;
    }
    
    @Override
    public Entry<Key,Value> next() {
        Entry<Key,Value> nextReturn = next;
        if (next != null) {
            getNext();
        }
        return nextReturn;
    }
    
    @Override
    public void remove() {
        throw new UnsupportedOperationException("Remove not supported on DedupingIterator");
    }
    
    private boolean isDuplicate(Entry<Key,Value> entry) {
        byte[] bytes = getBytes(entry);
        if (bloom.mightContain(bytes)) {
            return true;
        }
        bloom.put(bytes);
        return false;
    }
    
    public static class ByteFunnel implements Funnel<byte[]>, Serializable {
        
        private static final long serialVersionUID = -2126172579955897986L;
        
        @Override
        public void funnel(byte[] from, PrimitiveSink into) {
            into.putBytes(from);
        }
        
    }
}
