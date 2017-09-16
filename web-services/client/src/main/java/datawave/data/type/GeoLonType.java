package datawave.data.type;

import datawave.data.normalizer.Normalizer;

public class GeoLonType extends BaseType<String> {
    
    private static final long serialVersionUID = 8912983433360105604L;
    
    public GeoLonType() {
        super(Normalizer.GEO_LON_NORMALIZER);
    }
    
}
