package datawave.ingest.util.cache.watch;

import datawave.iterators.filter.ageoff.FieldAgeOffFilter;
import datawave.iterators.filter.ageoff.FilterOptions;

/**
 * Class to assist in the testing of FieldAgeOffFilters
 */
public class TestFieldFilter extends FieldAgeOffFilter {
    
    // public so that the tests can inspect the options
    public FilterOptions options;
    
    @Override
    public void init(FilterOptions options) {
        super.init(options);
        this.options = options;
    }
}
