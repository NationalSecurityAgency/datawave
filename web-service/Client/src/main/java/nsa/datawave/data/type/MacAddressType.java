package nsa.datawave.data.type;

import nsa.datawave.data.normalizer.Normalizer;

public class MacAddressType extends BaseType<String> {
    
    private static final long serialVersionUID = -6743560287574389073L;
    
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
        return PrecomputedSizes.STRING_STATIC_REF * 2 + (2 * normalizedValue.length()) + (2 * delegate.length()) + Sizer.REFERENCE;
    }
}
