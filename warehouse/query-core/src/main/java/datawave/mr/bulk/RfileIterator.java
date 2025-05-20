package datawave.mr.bulk;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.google.common.collect.Maps;

public class RfileIterator implements Iterator<Entry<Key,Value>> {

    protected RecordIterator recordIter = null;

    public RfileIterator(RecordIterator iter) {
        recordIter = iter;
    }

    @Override
    public Entry<Key,Value> next() {
        Entry<Key,Value> entry = Maps.immutableEntry(recordIter.getTopKey(), recordIter.getTopValue());
        try {
            recordIter.next();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return entry;

    }

    @Override
    public boolean hasNext() {
        return recordIter.hasTop();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Can't remove from rfileIter");

    }

}
