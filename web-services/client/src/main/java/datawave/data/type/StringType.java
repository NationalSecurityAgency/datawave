package datawave.data.type;

import datawave.data.normalizer.Normalizer;

public class StringType extends BaseType<String> {
    
    private static final long serialVersionUID = 8143572646109171126L;
    private static final long STATIC_SIZE = PrecomputedSizes.STRING_STATIC_REF * 2 + Sizer.REFERENCE;
    
    public StringType() {
        super(Normalizer.LC_NO_DIACRITICS_NORMALIZER);
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
