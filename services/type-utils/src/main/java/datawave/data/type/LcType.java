package datawave.data.type;

import datawave.data.normalizer.Normalizer;

public class LcType extends BaseType<String> {
    
    private static final long serialVersionUID = -5102714749195917406L;
    private static final long STATIC_SIZE = PrecomputedSizes.STRING_STATIC_REF * 2 + Sizer.REFERENCE;
    
    public LcType() {
        super(Normalizer.LC_NORMALIZER);
    }
    
    public LcType(String delegateString) {
        super(delegateString, Normalizer.LC_NORMALIZER);
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
