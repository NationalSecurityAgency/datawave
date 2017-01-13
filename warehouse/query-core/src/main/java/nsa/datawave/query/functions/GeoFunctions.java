package nsa.datawave.query.functions;

import nsa.datawave.data.normalizer.GeoNormalizer.GeoPoint;

import org.apache.log4j.Logger;

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
@Deprecated
@JexlFunctions(descriptorFactory = "nsa.datawave.query.functions.GeoFunctionsDescriptor")
public class GeoFunctions {
    private static final Logger log = Logger.getLogger(GeoFunctions.class);
    
    /**
     * Tests if a LAT_LON value is within a bounding box defined by a lower left LAT_LON and an upper right LAT_LON. NOTE: Incoming value to test should already
     * be normalized.
     *
     * @param fieldValue
     * @param lowerLeft
     * @param upperRight
     * @return
     */
    public static boolean within_bounding_box(String fieldValue, String lowerLeft, String upperRight) {
        if (log.isTraceEnabled()) {
            log.trace("Checking if " + fieldValue + " is within BB " + lowerLeft + ", " + upperRight);
        }
        try {
            // given the function descriptor already mapped the lower and upper arguments to the appropriate field, they should have already been normalized
            GeoPoint ll = GeoPoint.decodeZRef(lowerLeft);
            GeoPoint ur = GeoPoint.decodeZRef(upperRight);
            GeoPoint pp = GeoPoint.decodeZRef(fieldValue);
            return pp.within(ll, ur);
        } catch (Exception e) {
            log.warn("Could not verify point " + fieldValue + " to be within BB " + lowerLeft + ", " + upperRight);
        }
        return false;
    }
    
    /**
     * Tests whether or not a point, (lon, lat), is with the bounding box specified by (minLon, minLat) and (maxLon, maxLat).
     *
     * @param lon
     * @param lat
     * @param minLon
     * @param minLat
     * @param maxLon
     * @param maxLat
     * @return
     */
    public static boolean within_bounding_box(double lon, double lat, double minLon, double minLat, double maxLon, double maxLat) {
        return (lon >= minLon && lon <= maxLon) && (lat >= minLat && lat <= maxLat);
    }
    
    public static boolean within_bounding_box(String lon, String lat, double minLon, double minLat, double maxLon, double maxLat) {
        return within_bounding_box(Double.parseDouble(lon), Double.parseDouble(lat), minLon, minLat, maxLon, maxLat);
    }
    
    public static boolean within_bounding_box(GeoPoint candidate, GeoPoint lowerLeft, GeoPoint upperRight) {
        return candidate.within(lowerLeft, upperRight);
    }
    
    /**
     * Tests a given point is within the circle defined by a center LAT_LON is decimal degrees and a radius, also in decimal degrees. NOTE: Incoming value to
     * test should already be normalized.
     *
     * @param fieldValue
     * @param center
     * @param radius
     * @return
     */
    public static boolean within_circle(String fieldValue, String center, double radius) {
        if (log.isTraceEnabled()) {
            log.trace("Checking if " + fieldValue + " is within circle[center=" + center + ", radius=" + radius + "]");
        }
        
        try {
            // given the function descriptor already mapped the center to the appropriate field, they should have already been normalized
            GeoPoint p = GeoPoint.decodeZRef(fieldValue);
            GeoPoint c = GeoPoint.decodeZRef(center);
            double y = Math.pow(p.getLatitude() - c.getLatitude(), 2);
            double x = Math.pow(p.getLongitude() - c.getLongitude(), 2);
            return (x + y) <= Math.pow(radius, 2);
        } catch (Exception e) {
            log.warn("Could not check point in circle. P=" + fieldValue + ", C=" + center + ", R=" + radius, e);
        }
        return false;
    }
}
