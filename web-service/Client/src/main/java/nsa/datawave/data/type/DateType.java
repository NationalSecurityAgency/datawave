package nsa.datawave.data.type;

import java.util.Date;

import nsa.datawave.data.normalizer.Normalizer;

public class DateType extends BaseType<Date> {
    
    private static final long serialVersionUID = 936566410691643144L;
    
    public DateType() {
        super(Normalizer.DATE_NORMALIZER);
    }
    
    public DateType(String dateString) {
        super(Normalizer.DATE_NORMALIZER);
        super.setDelegate(normalizer.denormalize(dateString));
    }
    
    @Override
    public String getDelegateAsString() {
        // the normalized form of the date preserves milliseconds
        return normalizer.normalizeDelegateType(getDelegate());
    }
}
