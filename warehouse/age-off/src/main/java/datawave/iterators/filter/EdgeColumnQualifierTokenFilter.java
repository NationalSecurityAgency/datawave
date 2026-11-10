package datawave.iterators.filter;

import java.nio.charset.StandardCharsets;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import datawave.iterators.filter.ageoff.FilterOptions;

public class EdgeColumnQualifierTokenFilter extends TokenizingFilterBase {

    private static final byte[] CV_DELIMITERS = "/".getBytes(StandardCharsets.UTF_8);

    @Override
    public byte[] getKeyField(Key k, Value V) {
        return k.getColumnQualifierData().getBackingArray();
    }

    @Override
    public byte[] getDelimiters(FilterOptions options) {
        return CV_DELIMITERS;
    }
}
