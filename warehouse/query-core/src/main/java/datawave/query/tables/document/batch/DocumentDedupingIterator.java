package datawave.query.tables.document.batch;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import datawave.query.tables.serialization.SerializedDocumentIfc;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map.Entry;

class DocumentDedupingIterator implements Iterator<SerializedDocumentIfc> {
    static final int BLOOM_EXPECTED_DEFAULT = 500000;
    static final double BLOOM_FPP_DEFAULT = 1e-15;

    private Iterator<SerializedDocumentIfc> delegate;
    private SerializedDocumentIfc next;
    private BloomFilter<byte[]> bloom = null;

    public DocumentDedupingIterator(Iterator<SerializedDocumentIfc> iterator, int bloomFilterExpected, double bloomFilterFpp) {
        this.delegate = iterator;
        this.bloom = BloomFilter.create(new ByteFunnel(), bloomFilterExpected, bloomFilterFpp);
        getNext();
    }

    public DocumentDedupingIterator(Iterator<SerializedDocumentIfc> iterator) {
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
    

    
    @Override
    public boolean hasNext() {
        return next != null;
    }
    
    @Override
    public SerializedDocumentIfc next() {
        SerializedDocumentIfc nextReturn = next;
        if (next != null) {
            getNext();
        }
        return nextReturn;
    }
    
    @Override
    public void remove() {
        throw new UnsupportedOperationException("Remove not supported on DedupingIterator");
    }
    
    private boolean isDuplicate(SerializedDocumentIfc entry) {
        byte[] bytes = entry.getIdentifier();
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
