package datawave.ingest.util.cache.watch;

import datawave.iterators.filter.ageoff.FieldAgeOffFilter;
import datawave.iterators.filter.ageoff.FilterOptions;
import org.apache.accumulo.core.iterators.IteratorEnvironment;

/**
 * Class to assist in the testing of FieldAgeOffFilters
 */
public class TestFieldFilter extends FieldAgeOffFilter {
    
    // public so that the tests can inspect the options
    public FilterOptions options;
    
    @Override
    public void init(FilterOptions options) {
        init(options, null);
    }
    
    @Override
    public void init(FilterOptions options, IteratorEnvironment iterEnv) {
        super.init(options, iterEnv);
        this.options = options;
    }
}
