package nsa.datawave.data.type;

import nsa.datawave.data.normalizer.Normalizer;

public class GeoLatType extends BaseType<String> {
    
    private static final long serialVersionUID = -2775239290833908032L;
    
    public GeoLatType() {
        super(Normalizer.GEO_LAT_NORMALIZER);
    }
    
}
