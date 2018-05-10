package datawave.data.type;

import datawave.data.normalizer.Normalizer;

public class LcNoDiacriticsType extends BaseType<String> {
    
    private static final long serialVersionUID = -6219894926244790742L;
    private static final long STATIC_SIZE = PrecomputedSizes.STRING_STATIC_REF * 2 + Sizer.REFERENCE;
    
    public LcNoDiacriticsType() {
        super(Normalizer.LC_NO_DIACRITICS_NORMALIZER);
    }
    
    public LcNoDiacriticsType(String delegateString) {
        super(delegateString, Normalizer.LC_NO_DIACRITICS_NORMALIZER);
    }
    
    /**
     * Two strings + normalizer reference
     * 
     * @return
     */
    @Override
    public long sizeInBytes() {
        return STATIC_SIZE + (2 * normalizedValue.length()) + (2 * delegate.length());
    }
}
