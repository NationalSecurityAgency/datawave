package datawave.query.jexl.functions;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import datawave.data.normalizer.GeoNormalizer;
import datawave.data.normalizer.GeoNormalizer.GeoPoint;
import datawave.data.normalizer.Normalizer;
import datawave.query.attributes.ValueTuple;
import datawave.query.collections.FunctionalSet;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

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
     *            the field
     * @param lowerLeft
     *            the lower left bound
     * @param upperRight
     *            the upper right bound
     * @return if we're in the box
     */
    public static boolean within_bounding_box(Object field, String lowerLeft, String upperRight) {
        if (field != null) {
            String fieldValue = ValueTuple.getNormalizedStringValue(field);
            try {
                GeoPoint ll = GeoNormalizer.isNormalized(lowerLeft) ? normalizedPoints.get(lowerLeft) : unnormalizedPoints.get(lowerLeft);
                GeoPoint ur = GeoNormalizer.isNormalized(upperRight) ? normalizedPoints.get(upperRight) : unnormalizedPoints.get(upperRight);
                Set<GeoPoint> pts = GeoFunctions.getGeoPointsFromFieldValue(field);
                boolean m = pts.stream().anyMatch(geoPt -> evaluate(geoPt, ll, ur));
                if (log.isTraceEnabled())
                    log.trace("Checking if " + fieldValue + " is within BB " + lowerLeft + ", " + upperRight + ": [" + m + "]");
                return m;
            } catch (Exception e) {
                if (log.isTraceEnabled())
                    log.trace("Could not verify point " + fieldValue + " to be within BB " + lowerLeft + ", " + upperRight, e);
                return false;
            }
        } else {
            return false;
        }
    }

    public static boolean evaluate(GeoPoint point, GeoPoint boundMin, GeoPoint boundMax) {
        // if the minLon is greater than maxLon, we have crossed the antimeridian and must split the bounds for evaluation
        // @formatter:off
        return (point.getLatitude() >= boundMin.getLatitude() && point.getLatitude() <= boundMax.getLatitude() &&
                ((boundMin.getLongitude() > boundMax.getLongitude() &&
                        ((point.getLongitude() >= boundMin.getLongitude() && point.getLongitude() <= 180.0) ||
                         (point.getLongitude() >= -180 && point.getLongitude() <= boundMax.getLongitude()))) ||
                 (boundMin.getLongitude() <= boundMax.getLongitude() &&
                         point.getLongitude() >= boundMin.getLongitude() && point.getLongitude() <= boundMax.getLongitude())));
        // @formatter:on
    }

    public static boolean within_bounding_box(Object lonField, Object latField, String minLon, String minLat, String maxLon, String maxLat) {
        return GeoFunctions.within_bounding_box(lonField, latField, Normalizer.NUMBER_NORMALIZER.denormalize(minLon).doubleValue(),
                        Normalizer.NUMBER_NORMALIZER.denormalize(minLat).doubleValue(), Normalizer.NUMBER_NORMALIZER.denormalize(maxLon).doubleValue(),
                        Normalizer.NUMBER_NORMALIZER.denormalize(maxLat).doubleValue());
    }

    public static boolean within_bounding_box(Object lonField, Object latField, double minLon, double minLat, double maxLon, double maxLat) {
        if (lonField != null && latField != null) {
            Set<Double> lonValues;
            Set<Double> latValues;
            try {
                lonValues = getDoublesFromFieldValue(lonField);
                latValues = getDoublesFromFieldValue(latField);
            } catch (IllegalArgumentException e) { // NumberFormatException extends IAE and sometimes IAE is thrown
                if (log.isTraceEnabled())
                    log.trace("Error parsing lat[" + latField + "] or lon[" + lonField + "]", e);
                return false;
            }

            // @formatter:off
            boolean lonMatch = lonValues.stream().anyMatch(lon ->
                (minLon > maxLon &&
                        ((lon >= minLon && lon <= 180.0) || (lon >= -180.0 && lon <= maxLon))) ||
                (minLon <= maxLon &&
                        lon >= minLon && lon <= maxLon));
            // @formatter:on

            boolean latMatch = latValues.stream().anyMatch(lat -> lat >= minLat && lat <= maxLat);

            return lonMatch && latMatch;
        } else {
            return false;
        }
    }

    /**
     * Tests a given point is within the circle defined by a center LAT_LON is decimal degrees and a radius, also in decimal degrees. NOTE: Incoming value to
     * test should already be normalized.
     *
     * @param field
     *            the field
     * @param center
     *            the center of the circle
     * @param radius
     *            radius of the circle
     * @return if we're within the circle
     */
    public static boolean within_circle(Object field, String center, double radius) {
        if (field != null) {
            String fieldValue = ValueTuple.getNormalizedStringValue(field);
            if (log.isTraceEnabled()) {
                log.trace("Checking if " + fieldValue + " is within circle[center=" + center + ", radius=" + radius + "]");
            }
            try {
                Set<GeoPoint> pts = GeoFunctions.getGeoPointsFromFieldValue(field);
                GeoPoint c = GeoNormalizer.isNormalized(center) ? normalizedPoints.get(center) : unnormalizedPoints.get(center);
                boolean m = pts.stream().anyMatch(
                                geoPt -> (Math.pow(geoPt.getLongitude() - c.getLongitude(), 2) + Math.pow(geoPt.getLatitude() - c.getLatitude(), 2)) <= Math
                                                .pow(radius, 2));
                if (log.isTraceEnabled())
                    log.trace("Checking if " + fieldValue + " is within circle[center=" + center + ", radius=" + radius + "]: [" + m + "]");
                return m;
            } catch (Exception e) {
                if (log.isTraceEnabled())
                    log.trace("Could not check point in circle. P=" + fieldValue + ", C=" + center + ", R=" + radius, e);
                return false;
            }
        } else {
            return false;
        }
    }

    private static Set<Double> getDoublesFromFieldValue(Object fieldValue) {
        if (fieldValue instanceof Number) {
            Set<Double> numbers = new HashSet<>();
            numbers.add(((Number) fieldValue).doubleValue());
            return numbers;
        } else if (fieldValue instanceof String) {
            Set<Double> numbers = new HashSet<>();
            numbers.add(Normalizer.NUMBER_NORMALIZER.denormalize((String) fieldValue).doubleValue());
            return numbers;
        } else if (fieldValue instanceof ValueTuple) {
            Set<Double> numbers = new HashSet<>();
            numbers.add(Normalizer.NUMBER_NORMALIZER.denormalize(ValueTuple.getStringValue(fieldValue)).doubleValue());
            return numbers;
        } else if (fieldValue instanceof FunctionalSet) {
            Set<Double> numbers = new HashSet<>();
            for (Object value : (FunctionalSet) fieldValue)
                numbers.addAll(getDoublesFromFieldValue(value));
            return numbers;
        }
        BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.UNPARSEABLE_JEXL_QUERY,
                        "Field Value:" + fieldValue + " cannot be recognized as a double");
        throw new IllegalArgumentException(qe);
    }

    private static Set<GeoPoint> getGeoPointsFromFieldValue(Object fieldValue) throws ExecutionException {
        if (fieldValue instanceof GeoPoint) {
            Set<GeoPoint> geoPoints = new HashSet<>();
            geoPoints.add((GeoPoint) fieldValue);
            return geoPoints;
        } else if (fieldValue instanceof String) {
            Set<GeoPoint> geoPoints = new HashSet<>();
            String stringValue = (String) fieldValue;
            geoPoints.add(GeoNormalizer.isNormalized(stringValue) ? normalizedPoints.get(stringValue) : unnormalizedPoints.get(stringValue));
            return geoPoints;
        } else if (fieldValue instanceof ValueTuple) {
            Set<GeoPoint> geoPoints = new HashSet<>();
            String normFieldVal = ValueTuple.getNormalizedStringValue(fieldValue);
            geoPoints.add(GeoNormalizer.isNormalized(normFieldVal) ? normalizedPoints.get(normFieldVal) : unnormalizedPoints.get(normFieldVal));
            return geoPoints;
        } else if (fieldValue instanceof FunctionalSet) {
            Set<GeoPoint> geoPoints = new HashSet<>();
            for (Object value : (FunctionalSet) fieldValue)
                geoPoints.addAll(getGeoPointsFromFieldValue(value));
            return geoPoints;
        }
        BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.UNPARSEABLE_JEXL_QUERY,
                        "Field Value: " + fieldValue + " cannot be recognized as a geo point");
        throw new IllegalArgumentException(qe);
    }
}
