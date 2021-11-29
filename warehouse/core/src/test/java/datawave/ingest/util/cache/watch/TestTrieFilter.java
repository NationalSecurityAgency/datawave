package datawave.ingest.util.cache.watch;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import datawave.iterators.filter.TokenizingFilterBase;
import datawave.iterators.filter.ageoff.FilterOptions;
import org.apache.accumulo.core.iterators.IteratorEnvironment;

/**
 * Class to assist in the testing of TokenizingFilters
 */
public class TestTrieFilter extends TokenizingFilterBase {
    private static final byte[] DELIM_BYTES = "/".getBytes();
    
    // public so that the tests can inspect the options
    public FilterOptions options;
    
    @Override
    public void init(FilterOptions options) {
        super.init(options);
        this.options = options;
    }
    
    @Override
    public void init(FilterOptions options, IteratorEnvironment iterEnv) {
        super.init(options, iterEnv);
        this.options = options;
    }
    
    @Override
    public byte[] getKeyField(Key k, Value V) {
        return k.getRow().getBytes();
    }
    
    @Override
    public byte[] getDelimiters(FilterOptions options) {
        return DELIM_BYTES;
    }
    
    public static Key create(String data, long ts) {
        return new Key(data.getBytes(), ts);
    }
}
