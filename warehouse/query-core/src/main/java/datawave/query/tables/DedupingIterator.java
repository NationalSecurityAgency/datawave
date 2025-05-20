package datawave.query.tables;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.accumulo.core.data.ByteSequence;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

import datawave.core.query.configuration.Result;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.iterator.profile.FinalDocumentTrackingIterator;

class DedupingIterator implements Iterator<Result> {
    static final int BLOOM_EXPECTED_DEFAULT = 500000;
    static final double BLOOM_FPP_DEFAULT = 1e-15;

    private Iterator<Result> delegate;
    private Result next;
    private byte[] nextBytes;
    private BloomFilter<byte[]> bloom = null;

    public DedupingIterator(Iterator<Result> iterator) {
        this(iterator, BLOOM_EXPECTED_DEFAULT, BLOOM_FPP_DEFAULT);
    }

    public DedupingIterator(Iterator<Result> iterator, BloomFilter<byte[]> bloom) {
        this(iterator, bloom, BLOOM_EXPECTED_DEFAULT, BLOOM_FPP_DEFAULT);
    }

    public DedupingIterator(Iterator<Result> iterator, int bloomFilterExpected, double bloomFilterFpp) {
        this(iterator, null, bloomFilterExpected, bloomFilterFpp);
    }

    public DedupingIterator(Iterator<Result> iterator, BloomFilter<byte[]> bloom, int bloomFilterExpected, double bloomFilterFpp) {
        this.delegate = iterator;
        if (bloom == null) {
            bloom = BloomFilter.create(new ByteFunnel(), bloomFilterExpected, bloomFilterFpp);
        }
        this.bloom = bloom;
        getNext();
    }

    public BloomFilter<byte[]> getBloom() {
        return bloom;
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

    private byte[] getBytes(Result entry) {
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
    public Result next() {
        Result nextReturn = next;
        if (next != null) {
            if (nextBytes != null) {
                // now that we are actually returning this result, update the bloom filter
                bloom.put(nextBytes);
                nextBytes = null;
            }
            getNext();
        }
        return nextReturn;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Remove not supported on DedupingIterator");
    }

    private boolean isDuplicate(Result entry) {
        // allow empty results to go through (required to track completion of ranges)
        if (entry.getKey() == null) {
            return false;
        }
        // allow all final documents through
        if (FinalDocumentTrackingIterator.isFinalDocumentKey(entry.getKey())) {
            return false;
        }
        nextBytes = getBytes(entry);
        if (bloom.mightContain(nextBytes)) {
            nextBytes = null;
            return true;
        }
        return false;
    }

    KryoDocumentDeserializer deserializer = new KryoDocumentDeserializer();

    public static class ByteFunnel implements Funnel<byte[]>, Serializable {

        private static final long serialVersionUID = -2126172579955897986L;

        @Override
        public void funnel(byte[] from, PrimitiveSink into) {
            into.putBytes(from);
        }

    }
}
