package datawave.data.type;

import datawave.data.normalizer.Normalizer;

public class RawDateType extends BaseType<String> {
    
    private static final long serialVersionUID = 936566410691643144L;
    private static final long STATIC_SIZE = PrecomputedSizes.STRING_STATIC_REF * 2 + Sizer.REFERENCE;
    
    public RawDateType() {
        super(Normalizer.RAW_DATE_NORMALIZER);
    }
    
    public RawDateType(String dateString) {
        super(Normalizer.RAW_DATE_NORMALIZER);
        super.setDelegate(normalizer.denormalize(dateString));
    }
    
    /**
     * Two String + normalizer reference
     * 
     * @return
     */
    @Override
    public long sizeInBytes() {
        return STATIC_SIZE + (2 * normalizedValue.length()) + (2 * delegate.length());
    }
}
