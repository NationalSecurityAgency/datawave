package datawave.metrics.iterators;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import datawave.metrics.keys.AnalyticEntryKey;
import datawave.metrics.keys.InvalidKeyException;

public class AnalyticDataTypeFilter extends DataTypeFilter {
    private AnalyticEntryKey aek = new AnalyticEntryKey();

    @Override
    public boolean accept(Key k, Value v) {
        try {
            aek.parse(k);
            return getTypes().contains(aek.getDataType());
        } catch (InvalidKeyException e) {
            return false;
        }
    }

}
