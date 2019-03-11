package datawave.data.type;

import datawave.data.normalizer.Normalizer;

public class GeoLonType extends BaseType<String> {
    
    private static final long serialVersionUID = 8912983433360105604L;
    private static final long STATIC_SIZE = PrecomputedSizes.STRING_STATIC_REF * 2 + Sizer.REFERENCE;
    
    public GeoLonType() {
        super(Normalizer.GEO_LON_NORMALIZER);
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
