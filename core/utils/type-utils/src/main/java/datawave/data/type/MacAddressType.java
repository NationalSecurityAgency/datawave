package datawave.data.type;

import datawave.data.normalizer.Normalizer;

public class MacAddressType extends BaseType<String> {
    
    private static final long serialVersionUID = -6743560287574389073L;
    private static final long STATIC_SIZE = PrecomputedSizes.STRING_STATIC_REF * 2 + Sizer.REFERENCE;
    
    public MacAddressType() {
        super(Normalizer.MAC_ADDRESS_NORMALIZER);
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
