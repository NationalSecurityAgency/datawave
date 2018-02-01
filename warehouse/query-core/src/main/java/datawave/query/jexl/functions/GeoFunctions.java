package datawave.query.jexl.functions;

import datawave.data.normalizer.GeoNormalizer;
import datawave.data.normalizer.GeoNormalizer.GeoPoint;
import datawave.query.attributes.ValueTuple;

import org.apache.log4j.Logger;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * Provides functions for doing geo spacial queries, such as bounding boxes and circles of interest.
 * 
 * Function names are all lower case and separated by underscores to play nice with case insensitive queries.
 * 
 * NOTE: The JexlFunctionArgumentDescriptorFactory is implemented by GeoFunctionsDescriptor. This is kept as a separate class to reduce accumulo dependencies on
 * other jars.
 * 
 * 
 * 
 */
@JexlFunctions(descriptorFactory = "datawave.query.jexl.functions.GeoFunctionsDescriptor")
public class GeoFunctions {
    private static final Logger log = Logger.getLogger(GeoFunctions.class);
    
    public static final String GEO_FUNCTION_NAMESPACE = "geo";
    
    private static final LoadingCache<String,GeoPoint> unnormalizedPoints = CacheBuilder.newBuilder().concurrencyLevel(32).maximumSize(10 * 1024)
                    .build(new CacheLoader<String,GeoPoint>() {
                        
                        @Override
                        public GeoPoint load(String key) throws Exception {
                            GeoNormalizer gn = new GeoNormalizer();
                            return GeoPoint.decodeZRef(gn.normalize(key));
                        }
                        
                    });
    
    private static final LoadingCache<String,GeoPoint> normalizedPoints = CacheBuilder.newBuilder().concurrencyLevel(32).maximumSize(10 * 1024)
                    .build(new CacheLoader<String,GeoPoint>() {
                        
                        @Override
                        public GeoPoint load(String key) throws Exception {
                            return GeoPoint.decodeZRef(key);
                        }
                        
                    });
    
    /**
     * Tests if a LAT_LON value is within a bounding box defined by a lower left LAT_LON and an upper right LAT_LON. NOTE: Incoming value to test should already
     * be normalized.
     * 
     * @param field
     * @param lowerLeft
     * @param upperRight
     * @return
     */
    public static boolean within_bounding_box(Object field, String lowerLeft, String upperRight) {
        String fieldValue = ValueTuple.getNormalizedStringValue(field);
        GeoNormalizer geoNormalizer = new GeoNormalizer();
        try {
            GeoPoint ll = geoNormalizer.isNormalized(lowerLeft) ? normalizedPoints.get(lowerLeft) : unnormalizedPoints.get(lowerLeft);
            GeoPoint ur = geoNormalizer.isNormalized(upperRight) ? normalizedPoints.get(upperRight) : unnormalizedPoints.get(upperRight);
            GeoPoint pp = geoNormalizer.isNormalized(fieldValue) ? normalizedPoints.get(fieldValue) : unnormalizedPoints.get(fieldValue);
            boolean m = pp.within(ll, ur);
            if (log.isTraceEnabled())
                log.trace("Checking if " + fieldValue + " is within BB " + lowerLeft + ", " + upperRight + ": [" + m + "]");
            return m;
        } catch (Exception e) {
            if (log.isTraceEnabled())
                log.trace("Could not verify point " + fieldValue + " to be within BB " + lowerLeft + ", " + upperRight, e);
            return false;
        }
    }
    
    /**
     * Handle multiple values
     * 
     * @param values
     * @param lowerLeft
     * @param upperRight
     * @return
     */
    public static boolean within_bounding_box(Iterable<?> values, String lowerLeft, String upperRight) {
        for (Object fieldValue : values) {
            if (within_bounding_box(fieldValue, lowerLeft, upperRight)) {
                return true;
            }
        }
        return false;
    }
    
    public static boolean within_bounding_box(Object lonField, Object latField, double minLon, double minLat, double maxLon, double maxLat) {
        String lon = ValueTuple.getStringValue(lonField);
        String lat = ValueTuple.getStringValue(latField);
        try {
            double lonD = Double.parseDouble(lon), latD = Double.parseDouble(lat);
            return lonD >= minLon && lonD <= maxLon && latD >= minLat && latD <= maxLat;
        } catch (IllegalArgumentException e) { // NumberFormatException extends IAE and sometimes IAE is thrown
            if (log.isTraceEnabled())
                log.trace("Error parsing lat[" + lat + " or lon[" + lon + "]", e);
            return false;
        }
    }
    
    /**
     * Tests a given point is within the circle defined by a center LAT_LON is decimal degrees and a radius, also in decimal degrees. NOTE: Incoming value to
     * test should already be normalized.
     * 
     * @param field
     * @param center
     * @param radius
     * @return
     */
    public static boolean within_circle(Object field, String center, double radius) {
        String fieldValue = ValueTuple.getNormalizedStringValue(field);
        if (log.isTraceEnabled()) {
            log.trace("Checking if " + fieldValue + " is within circle[center=" + center + ", radius=" + radius + "]");
        }
        GeoNormalizer geoNormalizer = new GeoNormalizer();
        try {
            GeoPoint p = geoNormalizer.isNormalized(fieldValue) ? normalizedPoints.get(fieldValue) : unnormalizedPoints.get(fieldValue);
            GeoPoint c = geoNormalizer.isNormalized(center) ? normalizedPoints.get(center) : unnormalizedPoints.get(center);
            double y = Math.pow(p.getLatitude() - c.getLatitude(), 2);
            double x = Math.pow(p.getLongitude() - c.getLongitude(), 2);
            boolean m = (x + y) <= Math.pow(radius, 2);
            if (log.isTraceEnabled())
                log.trace("Checking if " + fieldValue + " is within circle[center=" + center + ", radius=" + radius + "]: [" + m + "]");
            return m;
        } catch (Exception e) {
            if (log.isTraceEnabled())
                log.trace("Could not check point in circle. P=" + fieldValue + ", C=" + center + ", R=" + radius, e);
            return false;
        }
    }
    
    /**
     * Handle multiple values
     * 
     * @param values
     * @param center
     * @param radius
     * @return
     */
    public static boolean within_circle(Iterable<?> values, String center, double radius) {
        for (Object fieldValue : values) {
            if (within_circle(fieldValue, center, radius)) {
                return true;
            }
        }
        return false;
    }
}
