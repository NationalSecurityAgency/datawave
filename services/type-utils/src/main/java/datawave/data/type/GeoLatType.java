package datawave.data.type;

import datawave.data.normalizer.Normalizer;

public class GeoLatType extends BaseType<String> {
    
    private static final long serialVersionUID = -2775239290833908032L;
    private static final long STATIC_SIZE = PrecomputedSizes.STRING_STATIC_REF * 2 + Sizer.REFERENCE;
    
    public GeoLatType() {
        super(Normalizer.GEO_LAT_NORMALIZER);
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
