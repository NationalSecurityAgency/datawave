package nsa.datawave.data.type;

import nsa.datawave.data.normalizer.Normalizer;

public class HexStringType extends BaseType<String> {
    
    private static final long serialVersionUID = -3480716807342380164L;
    
    public HexStringType() {
        super(Normalizer.HEX_STRING_NORMALIZER);
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
