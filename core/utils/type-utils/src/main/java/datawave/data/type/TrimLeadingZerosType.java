package datawave.data.type;

import datawave.data.normalizer.Normalizer;

public class TrimLeadingZerosType extends BaseType<String> {
    
    private static final long serialVersionUID = -7425014359719165469L;
    private static final long STATIC_SIZE = PrecomputedSizes.STRING_STATIC_REF * 2 + Sizer.REFERENCE;
    
    public TrimLeadingZerosType() {
        super(Normalizer.TRIM_LEADING_ZEROS_NORMALIZER);
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
