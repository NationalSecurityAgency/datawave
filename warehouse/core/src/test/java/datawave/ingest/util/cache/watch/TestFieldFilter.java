package datawave.ingest.util.cache.watch;

import org.apache.accumulo.core.data.Key;

import datawave.iterators.filter.ageoff.FieldAgeOffFilter;
import datawave.iterators.filter.ageoff.FilterOptions;

/**
 * Class to assist in the testing of FieldAgeOffFilter
 */
public class TestFieldFilter extends FieldAgeOffFilter {

    // public so that the tests can inspect the options
    public FilterOptions options;
    
    @Override
    public void init(FilterOptions options) {
        super.init(options);
        this.options = options;
    }
    
    public static Key create(String data, long ts) {
        return new Key(data.getBytes(), ts);
    }
}
