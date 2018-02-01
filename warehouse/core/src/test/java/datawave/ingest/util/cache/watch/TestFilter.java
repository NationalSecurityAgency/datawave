package datawave.ingest.util.cache.watch;

import datawave.iterators.filter.ageoff.AgeOffPeriod;
import datawave.iterators.filter.ageoff.FilterOptions;
import datawave.iterators.filter.ageoff.FilterRule;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public class TestFilter implements FilterRule {
    // public so that the tests can inspect the options
    public FilterOptions options;
    
    @Override
    public void init(FilterOptions options) {
        this.options = options;
    }
    
    @Override
    public boolean accept(SortedKeyValueIterator<Key,Value> iter) {
        return false;
    }
    
    @Override
    public FilterRule decorate(Object decoratedObject) {
        return null;
    }
    
    @Override
    public FilterRule deepCopy(AgeOffPeriod period) {
        return null;
    }
    
    @Override
    public FilterRule deepCopy(long scanStart) {
        return null;
    }
}
