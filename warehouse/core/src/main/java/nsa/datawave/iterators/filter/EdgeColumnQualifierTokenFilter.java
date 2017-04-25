package nsa.datawave.iterators.filter;

import nsa.datawave.iterators.filter.ageoff.FilterOptions;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

public class EdgeColumnQualifierTokenFilter extends TokenizingFilterBase {
    
    private final static byte[] CV_DELIMITERS = "/".getBytes();
    
    @Override
    public byte[] getKeyField(Key k, Value V) {
        return k.getColumnQualifierData().getBackingArray();
    }
    
    @Override
    public byte[] getDelimiters(FilterOptions options) {
        return CV_DELIMITERS;
    }
}
