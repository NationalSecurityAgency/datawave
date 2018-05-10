package datawave.data.type;

import datawave.data.normalizer.Normalizer;

public class GeoType extends BaseType<String> {
    
    private static final long serialVersionUID = 8429780512238258642L;
    private static final long STATIC_SIZE = PrecomputedSizes.STRING_STATIC_REF * 2 + Sizer.REFERENCE;
    
    public GeoType() {
        super(Normalizer.GEO_NORMALIZER);
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
