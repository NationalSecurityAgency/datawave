package nsa.datawave.query.rewrite.tld;

import com.google.common.base.Predicate;
import nsa.datawave.query.rewrite.iterator.EventDataScanNestedIterator;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.IOException;
import java.util.Collection;

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
