package datawave.core.geo.utils;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.jexl3.parser.JexlNode;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.util.GeometricShapeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.core.geo.function.AbstractGeoFunctionDetails;
import datawave.core.geo.function.geo.NumericIndexGeoFunctionDetails;
import datawave.core.geo.function.geo.ZIndexGeoFunctionDetails;
import datawave.core.query.jexl.JexlASTHelper;
import datawave.data.normalizer.GeoNormalizer;

/**
 * This utility class contains a variety of methods which can be used to perform operations on Geo ranges.
 * <p>
 * These methods assume that a custom 10-base Z-index strategy is being used, with a maximum of 18 characters per index. No guarantees are made as to the
 * effectiveness or accuracy of these methods given any other configuration.
 */
public class GeoUtils {

    private static final Logger log = LoggerFactory.getLogger(GeoUtils.class);

    public static final int NUM_CIRCLE_POINTS = 60;
    public static final String GEO_NAMESPACE = "geo";
    public static final String WITHIN_BOUNDING_BOX = "within_bounding_box";
    public static final String WITHIN_CIRCLE = "within_circle";

    private static final GeoNormalizer geoNormalizer = new GeoNormalizer();

    /**
     * Setting the precision too high is unnecessary, and will result in occasional computational errors within the JTS library.
     */
    static final GeometryFactory gf = CommonGeoUtils.gf;

    /**
     * This is a convenience class used when optimizing ranges..
     */
    private static class RangeData {
        RangeData(long[] range, double difference) {
            this.range = range;
            this.difference = difference;
        }

        long[] range;
        double difference;
    }

    /**
     * Generates an optimized list of Geo index ranges. Throughout the range generation process, ranges which do not intersect the query geometry are thrown
     * out. The largest ranges are iteratively decomposed into smaller ranges, with non-intersecting sub-ranges thrown out. This process runs until the desired
     * number of ranges has been reached, or no further decomposition is possible.
     *
     * @param geometry
     *            the original query geometry
     * @param envelopes
     *            the envelopes representing the potentially disjoint geometries
     * @param config
     *            the geo query configuration
     * @param termsAndRanges
     *            the array where the terms and ranges will be saved
     */
    protected static void generateTermsAndRanges(Geometry geometry, List<Envelope> envelopes, GeoQueryConfig config, ArrayList<String[]> termsAndRanges) {
        List<long[]> allRanges = new ArrayList<>();

        // generate initial ranges from each envelope
        for (Envelope envelope : envelopes) {
            // @formatter:off
            allRanges.add(new long[]{
                    GeoUtils.latLonToPosition(envelope.getMinY(), envelope.getMinX()),
                    GeoUtils.latLonToPosition(envelope.getMaxY(), envelope.getMaxX())});
            // @formatter:on
        }

        // optionally decompose the ranges further to achieve a desired number of tight-fitting ranges
        if (config.isOptimizeGeoRanges()) {
            allRanges = generateOptimizedPositionRanges(geometry, allRanges, config);
        }

        // convert the position ranges to string ranges
        termsAndRanges.ensureCapacity(allRanges.size());
        for (long[] positionRange : allRanges) {
            termsAndRanges.add(new String[] {GeoUtils.positionToIndex(positionRange[0]), GeoUtils.positionToIndex(positionRange[1])});
        }
    }

    /**
     * This performs a sort of quad-tree decomposition on the given range. This algorithm breaks the given range down into its constituent parts. The bounds of
     * each range are of the same magnitude (i.e. ones, tens, thousands, etc...). The ranges returned by this method represent a collection of smaller,
     * constituent bounding boxes which are collectively topologically equal to the geometry represented by the original range.
     *
     * @param beginPosition
     *            the minimum numeric index position
     * @param endPosition
     *            the maximum numeric index position
     * @return a decomposed list of numeric index position ranges
     */
    public static List<long[]> decodePositionRange(long beginPosition, long endPosition) {
        LinkedList<long[]> rangesToDecode = new LinkedList<>();
        LinkedList<long[]> decodedRanges = new LinkedList<>();

        rangesToDecode.push(new long[] {beginPosition, endPosition});

        while (!rangesToDecode.isEmpty()) {
            long[] rangeToDecode = rangesToDecode.pop();
            long begin = rangeToDecode[0];
            long end = rangeToDecode[1];

            long diff = end - begin;
            long increment = (long) Math.pow(10, (long) Math.log10(diff));

            long alignedBegin = (long) Math.ceil((double) begin / increment) * increment;
            if (begin < alignedBegin) {
                rangesToDecode.push(new long[] {begin, alignedBegin});
            }

            long alignedEnd = (long) Math.floor((double) end / increment) * increment;
            if (alignedEnd < end) {
                rangesToDecode.push(new long[] {alignedEnd, end});
            }

            for (long start = alignedBegin; start < alignedEnd; start += increment) {
                decodedRanges.push(new long[] {start, start + increment});
            }
        }

        return decodedRanges;
    }

    /**
     * This method iteratively decomposes, and reduces the number of ranges to just those ranges required to cover the query geometry. The algorithm iteratively
     * decomposes the largest remaining range into its constituent ranges at the next lowest magnitude, removing any sub-ranges which no longer intersect with
     * the query geometry. This process continues until the number of desired ranges are reached, or until no more decomposition can be performed.
     *
     * @param geometry
     *            the query geometry
     * @param ranges
     *            the numeric index ranges
     * @param config
     *            the geo query config
     * @return an optimized list of numeric index ranges
     */
    public static List<long[]> generateOptimizedPositionRanges(Geometry geometry, List<long[]> ranges, GeoQueryConfig config) {
        TreeSet<RangeData> areaSortedRanges = new TreeSet<>((o1, o2) -> {
            // ranges which share the least overlap with the query geometry are sorted first
            int diff = Double.compare(o2.difference, o1.difference);
            if (diff == 0) {
                // next, sort by the length of the query range
                diff = Long.compare((o2.range[1] - o2.range[0]), (o1.range[1] - o1.range[0]));
                if (diff == 0) {
                    // finally, sort by the minimum bound of the range
                    diff = Long.compare(o1.range[0], o2.range[0]);
                }
            }
            return diff;
        });
        // these ranges are sorted by the minimum bound of the range so that we can
        // quickly merge contiguous segments into discrete ranges
        TreeSet<RangeData> positionSortedRanges = new TreeSet<>(Comparator.comparingDouble(o -> o.range[0]));

        // starting out, get the decomposed ranges, and add only the ranges which intersect with the query geometry
        for (long[] range : ranges) {
            for (long[] decomposedRange : GeoUtils.decodePositionRange(range[0], range[1])) {
                if (GeoUtils.positionRangeToGeometry(decomposedRange[0], decomposedRange[1]).intersects(geometry)) {
                    double difference = areaDifference(positionRangeToGeometry(decomposedRange[0], decomposedRange[1]), geometry);
                    RangeData rangeData = new RangeData(decomposedRange, difference);
                    areaSortedRanges.add(rangeData);
                    positionSortedRanges.add(rangeData);
                }
            }
        }

        // stop when we have the desired number of ranges
        while (countDiscreteRanges(positionSortedRanges) < config.getGeoMaxExpansion()) {
            RangeData biggestRange = areaSortedRanges.pollFirst();
            positionSortedRanges.remove(biggestRange);

            if (biggestRange != null) {

                // if the biggest range only has 1 index, there is no more work to do
                if ((biggestRange.range[1] - biggestRange.range[0]) == 1) {
                    areaSortedRanges.add(biggestRange);
                    positionSortedRanges.add(biggestRange);
                    break;
                }

                // break up the range, and only add the sub-ranges that intersect with the geometry
                List<long[]> decomposedPositionRanges = decomposePositionRange(biggestRange.range[0], biggestRange.range[1]);
                for (long[] range : decomposedPositionRanges) {
                    Geometry rangeGeometry = positionRangeToGeometry(range[0], range[1]);
                    if (rangeGeometry.intersects(geometry)) {
                        double difference = areaDifference(rangeGeometry, geometry);
                        RangeData rangeData = new RangeData(range, difference);
                        areaSortedRanges.add(rangeData);
                        positionSortedRanges.add(rangeData);
                    }
                }
            }
        }

        // return the discrete ranges after merging contiguous segments
        return mergeContiguousRanges(positionSortedRanges);
    }

    private static double areaDifference(Geometry geom1, Geometry geom2) {
        try {
            return geom1.getArea() - (geom1.intersection(geom2).getArea());
        } catch (Exception e) {
            log.warn("Unable to compute area difference between polygons. ", e);
            if (log.isTraceEnabled()) {
                log.trace("Geometries: [" + geom1.toText() + "], [" + geom2.toText() + "]");
            }
            return 0;
        }
    }

    /**
     * Decomposes a range into its constituent ranges at the next lowest order of magnitude.
     *
     * @param beginPosition
     *            the minimum numeric index position
     * @param endPosition
     *            the maximum numeric index position
     * @return a decomposed list of numeric index ranges
     */
    private static List<long[]> decomposePositionRange(long beginPosition, long endPosition) {
        List<long[]> decomposedRanges = new ArrayList<>();

        long increment = (endPosition - beginPosition) / 10L;
        if (increment >= 1) {
            for (long position = beginPosition; position < endPosition; position += increment) {
                decomposedRanges.add(new long[] {position, position + increment});
            }
        } else {
            decomposedRanges.add(new long[] {beginPosition, endPosition});
        }

        return decomposedRanges;
    }

    /**
     * Converts a lat/lon pair a geo index
     *
     * @param latitude
     *            the latitude
     * @param longitude
     *            the longitude
     * @return a geo index
     */
    public static String latLonToIndex(double latitude, double longitude) {
        double latShift = latitude + 90.0;
        double lonShift = longitude + 180.0;

        NumberFormat formatter = NumberFormat.getInstance();
        formatter.setMaximumIntegerDigits(3);
        formatter.setMinimumIntegerDigits(3);
        formatter.setMaximumFractionDigits(5);
        formatter.setMinimumFractionDigits(5);

        String latS = formatter.format(latShift);
        String lonS = formatter.format(lonShift);
        StringBuilder sb = new StringBuilder(latS.length() * 2);

        for (int i = 0; i < latS.length(); ++i) {
            sb.append(latS.charAt(i));
            sb.append(lonS.charAt(i));
        }

        return sb.toString();
    }

    /**
     * Converts a geo index to a numeric index position along the Z-order SFC
     *
     * @param index
     *            the geo index
     * @return a numeric index position
     */
    public static long indexToPosition(String index) {
        return Long.parseLong(index.replace(".", ""));
    }

    /**
     * Converts a numeric index position to a geo index
     *
     * @param position
     *            the numeric index position
     * @return a geo index
     */
    public static String positionToIndex(long position) {
        NumberFormat formatter = NumberFormat.getInstance();
        formatter.setMaximumIntegerDigits(16);
        formatter.setMinimumIntegerDigits(16);
        formatter.setGroupingUsed(false);

        String index = formatter.format(position);
        return index.substring(0, 6) + ".." + index.substring(6);
    }

    /**
     * Converts a lat/lon pair to a numeric index position
     *
     * @param latitude
     *            the latitude
     * @param longitude
     *            the longitude
     * @return a numeric index position
     */
    public static long latLonToPosition(double latitude, double longitude) {
        String index = latLonToIndex(latitude, longitude);
        return indexToPosition(index);
    }

    /**
     * Converts a numeric index position to a lat/lon pair
     *
     * @param position
     *            the numeric index position
     * @return a 2-element array with lat followed by lon
     */
    public static double[] positionToLatLon(long position) {
        return indexToLatLon(positionToIndex(position));
    }

    /**
     * Converts a numeric index position to a lat/lon pair
     *
     * @param index
     *            the geo index
     * @return a 2-element array with lat followed by lon
     */
    public static double[] indexToLatLon(String index) {
        StringBuilder latString = new StringBuilder(18);
        StringBuilder lonString = new StringBuilder(18);
        for (int i = 0; i < index.length(); i++) {
            if (i % 2 == 0) {
                latString.append(index.charAt(i));
            } else {
                lonString.append(index.charAt(i));
            }
        }

        return new double[] {Double.parseDouble(latString.toString()) - 90.0, Double.parseDouble(lonString.toString()) - 180.0};
    }

    /**
     * Converts a numeric index position to its equivalent bounding box geometry
     *
     * @param position
     *            the numeric index position
     * @return a bounding box geometry
     */
    public static Geometry termToGeometry(long position) {
        double[] latLon = positionToLatLon(position);

        double minLat = latLon[0];
        double maxLat = minLat + 0.00001;

        double minLon = latLon[1];
        double maxLon = minLon + 0.00001;

        Coordinate[] coords = new Coordinate[5];
        coords[0] = new Coordinate(minLon, minLat);
        coords[1] = new Coordinate(maxLon, minLat);
        coords[2] = new Coordinate(maxLon, maxLat);
        coords[3] = new Coordinate(minLon, maxLat);
        coords[4] = coords[0];

        return gf.createPolygon(coords);
    }

    /**
     * Converts a geo term to its equivalent bounding box geometry
     *
     * @param term
     *            the geo term
     * @return a bounding box geometry
     */
    public static Geometry termToGeometry(String term) {
        return termToGeometry(indexToPosition(term));
    }

    /**
     * Converts a geo index to its equivalent bounding box geometry
     *
     * @param index
     *            the geo index
     * @return a bounding box geometry
     */
    public static Geometry indexToGeometry(String index) {
        return termToGeometry(indexToPosition(index));
    }

    /**
     * Converts a numeric index range to its equivalent geometry.
     *
     * @param beginPosition
     *            the minimum numeric index position
     * @param endPosition
     *            the maximum numeric index position
     * @return a geometry which represents the range
     */
    public static Geometry positionRangeToGeometry(long beginPosition, long endPosition) {
        List<long[]> ranges = decodePositionRange(beginPosition, endPosition);

        List<Geometry> geometries = new ArrayList<>();
        for (long[] range : ranges) {
            List<Double> latitudes = new ArrayList<>();
            List<Double> longitudes = new ArrayList<>();

            Geometry minGeom = termToGeometry(range[0]);
            Arrays.stream(minGeom.getCoordinates()).forEach(c -> {
                longitudes.add(c.x);
                latitudes.add(c.y);
            });

            double minLat = Collections.min(latitudes);
            double minLon = Collections.min(longitudes);
            latitudes.clear();
            longitudes.clear();

            Geometry maxGeom = termToGeometry(range[1] - 1);
            Arrays.stream(maxGeom.getCoordinates()).forEach(c -> {
                longitudes.add(c.x);
                latitudes.add(c.y);
            });

            double maxLat = Collections.max(latitudes);
            double maxLon = Collections.max(longitudes);

            Coordinate[] coords = new Coordinate[5];
            coords[0] = new Coordinate(minLon, minLat);
            coords[1] = new Coordinate(maxLon, minLat);
            coords[2] = new Coordinate(maxLon, maxLat);
            coords[3] = new Coordinate(minLon, maxLat);
            coords[4] = coords[0];
            geometries.add(gf.createPolygon(coords));
        }

        Geometry geom = new GeometryCollection(geometries.toArray(new Geometry[0]), gf).union();
        if (geom instanceof MultiPolygon) {
            // try inflating and deflating to get a single, simple polygon
            double scale = 100000000.0;
            GeometryFactory newGf = new GeometryFactory(new PrecisionModel(scale));
            double buffer = 1.0 / scale;
            geom = gf.createGeometry(
                            new GeometryCollection(geometries.stream().map(x -> newGf.createGeometry(x).buffer(buffer)).toArray(Geometry[]::new), newGf).union()
                                            .buffer(-buffer));
        }
        return geom;
    }

    /**
     * Converts an index range to its equivalent geometry.
     *
     * @param beginIndex
     *            the minimum geohash index
     * @param endIndex
     *            the maximum geohash index
     * @return a geometry which represents the range
     */
    public static Geometry rangeToGeometry(String beginIndex, String endIndex) {
        return positionRangeToGeometry(indexToPosition(beginIndex), indexToPosition(endIndex));
    }

    /**
     * Counts the number of discrete (i.e. non-contiguous) ranges.
     *
     * @param positionSortedRanges
     *            the sorted set of ranges, sorted by each range's minimum bound
     * @return the number of discrete ranges in the set
     */
    private static long countDiscreteRanges(TreeSet<RangeData> positionSortedRanges) {
        long count = 0;

        long lastEnd = Long.MIN_VALUE;
        for (RangeData range : positionSortedRanges) {
            if (range.range[0] != lastEnd) {
                count++;
            }
            lastEnd = range.range[1];
        }

        return count;
    }

    /**
     * Merges the contiguous ranges in the set, and returns a list of discrete numeric index ranges.
     *
     * @param positionSortedRanges
     *            the ranges to merge
     * @return a list of discrete numeric index ranges
     */
    private static List<long[]> mergeContiguousRanges(TreeSet<RangeData> positionSortedRanges) {
        List<long[]> mergedRanges = new ArrayList<>();

        long lastBegin = Long.MIN_VALUE;
        long lastEnd = Long.MIN_VALUE;
        for (RangeData range : positionSortedRanges) {
            if (range.range[0] != lastEnd) {
                if (lastBegin != Long.MIN_VALUE && lastEnd != Long.MIN_VALUE) {
                    mergedRanges.add(new long[] {lastBegin, lastEnd});
                }

                lastBegin = range.range[0];
            }
            lastEnd = range.range[1];
        }

        if (lastBegin != Long.MIN_VALUE && lastEnd != Long.MIN_VALUE) {
            mergedRanges.add(new long[] {lastBegin, lastEnd});
        }

        return mergedRanges;
    }

    public static Polygon createCircle(double lon, double lat, double radius) {
        GeometricShapeFactory shapeFactory = new GeometricShapeFactory(gf);
        shapeFactory.setNumPoints(NUM_CIRCLE_POINTS);
        shapeFactory.setCentre(new Coordinate(lon, lat));
        shapeFactory.setSize(radius * 2);
        return shapeFactory.createCircle();
    }

    public static Polygon createRectangle(double minLon, double maxLon, double minLat, double maxLat) {
        List<Coordinate> coordinates = new ArrayList<>();
        coordinates.add(new CoordinateXY(minLon, minLat));
        coordinates.add(new CoordinateXY(maxLon, minLat));
        coordinates.add(new CoordinateXY(maxLon, maxLat));
        coordinates.add(new CoordinateXY(minLon, maxLat));
        coordinates.add(new CoordinateXY(minLon, minLat));
        return gf.createPolygon(coordinates.toArray(new Coordinate[0]));
    }

    public static AbstractGeoFunctionDetails parseGeoFunction(String name, List<JexlNode> args) {
        AbstractGeoFunctionDetails geoFunction = null;
        if (name.equals(WITHIN_BOUNDING_BOX)) {
            // three arguments is the form within_bounding_box(fieldName, lowerLeft, upperRight)
            if (args.size() == 3) {
                Set<String> fieldNames = JexlASTHelper.getIdentifierNames(args.get(0));
                double[] minLatLon = parseLatLon(String.valueOf(JexlASTHelper.getLiteralValue(args.get(1))));
                double[] maxLatLon = parseLatLon(String.valueOf(JexlASTHelper.getLiteralValue(args.get(2))));
                geoFunction = new ZIndexGeoFunctionDetails(WITHIN_BOUNDING_BOX, fieldNames, minLatLon[1], maxLatLon[1], minLatLon[0], maxLatLon[0]);
            }
            // six arguments is the form within_bounding_box(latFieldName, lonFieldName, minLon, minLat, maxLon, maxLat)
            else if (args.size() == 6) {
                try {
                    String lonFieldName = JexlASTHelper.getIdentifier(args.get(0));
                    String latFieldName = JexlASTHelper.getIdentifier(args.get(1));
                    double minLat = GeoNormalizer.parseLatOrLon(String.valueOf(JexlASTHelper.getLiteralValue(args.get(3))));
                    double maxLat = GeoNormalizer.parseLatOrLon(String.valueOf(JexlASTHelper.getLiteralValue(args.get(5))));
                    double minLon = GeoNormalizer.parseLatOrLon(String.valueOf(JexlASTHelper.getLiteralValue(args.get(2))));
                    double maxLon = GeoNormalizer.parseLatOrLon(String.valueOf(JexlASTHelper.getLiteralValue(args.get(4))));
                    geoFunction = new NumericIndexGeoFunctionDetails(WITHIN_BOUNDING_BOX, lonFieldName, latFieldName, minLon, maxLon, minLat, maxLat);
                } catch (GeoNormalizer.ParseException e) {
                    throw new IllegalArgumentException(e);
                }
            }
        } else if (name.equals(WITHIN_CIRCLE)) {
            // three arguments is the form within_circle(fieldName, center, radius)
            if (args.size() == 3) {
                try {
                    Set<String> fieldNames = JexlASTHelper.getIdentifierNames(args.get(0));
                    double[] centerLatLon = parseLatLon(String.valueOf(JexlASTHelper.getLiteralValue(args.get(1))));
                    double radius = GeoNormalizer.parseDouble(String.valueOf(JexlASTHelper.getLiteralValue(args.get(2))));
                    geoFunction = new ZIndexGeoFunctionDetails(WITHIN_CIRCLE, fieldNames, centerLatLon[1], centerLatLon[0], radius);
                } catch (GeoNormalizer.ParseException e) {
                    throw new IllegalArgumentException("Unable to parse radius " + JexlASTHelper.getLiteralValue(args.get(2)), e);
                }
            }
        }
        return geoFunction;
    }

    private static double[] parseLatLon(String latLonString) {
        try {
            return GeoNormalizer.isNormalized(latLonString) ? GeoNormalizer.GeoPoint.decodeZRef(latLonString).getLatLon()
                            : geoNormalizer.parseLatLon(latLonString);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to normalize value as a Geo: " + latLonString, e);
        }
    }

    /**
     * Generates and returns the geo query ranges for the given geometry
     *
     * @param geometry
     * @param config
     */
    public static List<String[]> generateTermsAndRanges(Geometry geometry, GeoQueryConfig config) {
        ArrayList<String[]> termsAndRanges = new ArrayList<>();
        List<Envelope> envelopes = CommonGeoUtils.getDisjointEnvelopes(geometry, config.getGeoMaxEnvelopes());
        if (!envelopes.isEmpty()) {
            GeoUtils.generateTermsAndRanges(geometry, envelopes, config, termsAndRanges);
        }
        return termsAndRanges;
    }
}
