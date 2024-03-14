package datawave.iterators.filter;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

public class DateInColQualAgeOffFilter extends AgeOffFilterBase {

    @Override
    protected byte[] getDateBytes(Key k, Value v) {
        return k.getColumnQualifierData().getBackingArray();
    }

}
