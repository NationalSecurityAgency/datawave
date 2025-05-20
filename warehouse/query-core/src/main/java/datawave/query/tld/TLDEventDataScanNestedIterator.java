package datawave.query.tld;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import com.google.common.base.Predicate;

import datawave.query.iterator.EventDataScanNestedIterator;

/**
 */
public class TLDEventDataScanNestedIterator extends EventDataScanNestedIterator {

    public TLDEventDataScanNestedIterator(SortedKeyValueIterator<Key,Value> source, Predicate<Key> dataTypeFilter) {
        super(source, dataTypeFilter);
    }

    @Override
    protected Key nextStartKey(Key key) {
        return TLD.getNextParentKey(key);
    }
}
