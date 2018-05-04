package nsa.datawave.data.type;

import nsa.datawave.data.normalizer.Normalizer;

public class TrimLeadingZerosType extends BaseType<String> {
    
    private static final long serialVersionUID = -7425014359719165469L;
    
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
        return PrecomputedSizes.STRING_STATIC_REF * 2 + (2 * normalizedValue.length()) + (2 * delegate.length()) + Sizer.REFERENCE;
    }
}
