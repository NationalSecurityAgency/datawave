package datawave.query.jexl.functions;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

import datawave.query.iterator.waitwindow.WaitWindowObserver;

/**
 * Key adjudicator, will take an accumulo key based entry whose value is specified by T.
 *
 * @param <T>
 *            type of the key adjudicator
 */
public class KeyAdjudicator<T> implements Iterator<Entry<Key,T>>, Function<Entry<Key,T>,Entry<Key,T>> {

    public static final Text COLUMN_QUALIFIER_SUFFIX = new Text("\uffff");

    private final Text colQualRef;
    private final Iterator<Entry<Key,T>> source;

    public KeyAdjudicator(Iterator<Entry<Key,T>> source, Text colQualRef) {
        this.colQualRef = colQualRef;
        this.source = source;
    }

    public KeyAdjudicator(Iterator<Entry<Key,T>> source) {
        this(source, COLUMN_QUALIFIER_SUFFIX);
    }

    public KeyAdjudicator(Text colQualRef) {
        this(null, colQualRef);
    }

    public KeyAdjudicator() {
        this(null, null);
    }

    @Override
    public Entry<Key,T> apply(Entry<Key,T> entry) {
        final Key entryKey = entry.getKey();
        // if the key has a YIELD_AT_BEGIN or YIELD_AT_END marker, then don't modify the key
        // because doing so will adversely affect the subsequent yield and re-seek position
        if (WaitWindowObserver.hasMarker(entryKey)) {
            return entry;
        } else {
            return Maps.immutableEntry(
                            new Key(entryKey.getRow(), entryKey.getColumnFamily(), colQualRef, entryKey.getColumnVisibility(), entryKey.getTimestamp()),
                            entry.getValue());
        }
    }

    @Override
    public boolean hasNext() {
        return source.hasNext();
    }

    @Override
    public Entry<Key,T> next() {
        Entry<Key,T> next = source.next();
        if (next != null) {
            next = apply(next);
        }
        return next;
    }

    @Override
    public void remove() {
        source.remove();
    }
}
