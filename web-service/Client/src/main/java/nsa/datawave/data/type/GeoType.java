package nsa.datawave.data.type;

import nsa.datawave.data.normalizer.Normalizer;

public class GeoType extends BaseType<String> {
    
    private static final long serialVersionUID = 8429780512238258642L;
    
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
        return PrecomputedSizes.STRING_STATIC_REF * 2 + (2 * normalizedValue.length()) + (2 * delegate.length()) + Sizer.REFERENCE;
    }
}
