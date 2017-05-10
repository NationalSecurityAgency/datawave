package datawave.data.normalizer;

import datawave.data.normalizer.GeoNormalizer.ParseException;
import datawave.data.type.util.NumericalEncoder;

import org.apache.log4j.Logger;

public class GeoLonNormalizer extends AbstractNormalizer<String> {
    
    private static final long serialVersionUID = 2026515023484372154L;
    private static final Logger log = Logger.getLogger(GeoLonNormalizer.class);
    private GeoNormalizer geoNormalizer = new GeoNormalizer();
    
    public String normalize(String fieldValue) {
        double val;
        try {
            val = geoNormalizer.parseLatOrLon(fieldValue);
        } catch (ParseException e) {
            throw new IllegalArgumentException(e);
        }
        if (val < -180.0 || val > 180.0) {
            throw new IllegalArgumentException("Longitude is outside of valid range [-180, 180]: " + val);
        }
        try {
            return NumericalEncoder.encode(Double.toString(val));
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to normalize value as a GeoLon: " + fieldValue);
        }
    }
    
    /**
     * We cannot support regex against numbers
     */
    
    public String normalizeRegex(String fieldRegex) {
        throw new IllegalArgumentException("Cannot normalize a regex against a numeric field");
    }
    
    @Override
    public String normalizeDelegateType(String delegateIn) {
        return normalize(delegateIn);
    }
    
    @Override
    public String denormalize(String in) {
        if (NumericalEncoder.isPossiblyEncoded(in)) {
            try {
                return NumericalEncoder.decode(in).toString();
            } catch (NumberFormatException e) {
                if (log.isTraceEnabled()) {
                    log.trace("Error decoding value.", e);
                }
            }
        }
        return in;
    }
    
}
