package datawave.query.tld;

import com.google.common.base.Predicate;
import datawave.query.iterator.EventDataScanNestedIterator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

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
