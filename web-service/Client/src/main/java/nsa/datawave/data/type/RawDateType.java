package nsa.datawave.data.type;

import nsa.datawave.data.normalizer.Normalizer;

import java.util.Date;

public class RawDateType extends BaseType<String> {
    
    private static final long serialVersionUID = 936566410691643144L;
    
    public RawDateType() {
        super(Normalizer.RAW_DATE_NORMALIZER);
    }
    
    public RawDateType(String dateString) {
        super(Normalizer.RAW_DATE_NORMALIZER);
        super.setDelegate(normalizer.denormalize(dateString));
    }
}
