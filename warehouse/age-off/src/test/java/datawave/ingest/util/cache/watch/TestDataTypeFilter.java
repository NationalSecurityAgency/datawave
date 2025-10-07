package datawave.ingest.util.cache.watch;

import org.apache.accumulo.core.iterators.IteratorEnvironment;

import datawave.iterators.filter.ageoff.DataTypeAgeOffFilter;
import datawave.iterators.filter.ageoff.FilterOptions;

/**
 * Class to assist in the testing of DataTypeAgeOffFilter
 */
public class TestDataTypeFilter extends DataTypeAgeOffFilter {

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
