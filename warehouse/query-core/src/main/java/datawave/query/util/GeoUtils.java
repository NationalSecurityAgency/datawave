package datawave.query.util;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;

import datawave.core.common.logging.ThreadConfigurableLogger;

/**
 * This utility class contains a variety of methods which can be used to perform operations on Geo ranges.
 * <p>
 * These methods assume that a custom 10-base Z-index strategy is being used, with a maximum of 18 characters per index. No guarantees are made as to the
 * effectiveness or accuracy of these methods given any other configuration.
 */
public class GeoUtils {

    private static final Logger log = ThreadConfigurableLogger.getLogger(GeoUtils.class);

    /**
     * Setting the precision too high is unnecessary, and will result in occasional computational errors within the JTS library.
     */
    static final GeometryFactory gf = new GeometryFactory(new PrecisionModel(1000000));

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
     * @param desiredRanges
     *            the desired number of ranges to generate
     * @return a list of optimized geo index ranges
     */
    public static List<String[]> generateOptimizedIndexRanges(Geometry geometry, int desiredRanges) {
        return generateOptimizedIndexRanges(geometry, Collections.singletonList(geometry.getEnvelopeInternal()), desiredRanges);
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
     * @param desiredRanges
     *            the desired number of ranges to generate
     * @return a list of optimized geo index ranges
     */
    public static List<String[]> generateOptimizedIndexRanges(Geometry geometry, List<Envelope> envelopes, int desiredRanges) {
        List<long[]> allRanges = new ArrayList<>();
        for (Envelope envelope : envelopes) {
            double minLon = envelope.getMinX();
            double maxLon = envelope.getMaxX();
            double minLat = envelope.getMinY();
            double maxLat = envelope.getMaxY();

            // is the lower left longitude greater than the upper right longitude?
            // if so, we have crossed the anti-meridian and should split
            if (minLon > maxLon) {
                allRanges.addAll(GeoUtils.decodePositionRange(GeoUtils.latLonToPosition(minLat, minLon), GeoUtils.latLonToPosition(maxLat, 180)));
                allRanges.addAll(GeoUtils.decodePositionRange(GeoUtils.latLonToPosition(minLat, -180), GeoUtils.latLonToPosition(maxLat, maxLon)));
            } else {
                allRanges.addAll(GeoUtils.decodePositionRange(GeoUtils.latLonToPosition(minLat, minLon), GeoUtils.latLonToPosition(maxLat, maxLon)));
            }
        }

        List<long[]> optimizedRanges = generateOptimizedPositionRanges(geometry, allRanges, desiredRanges);

        return optimizedRanges.stream().map(r -> new String[] {GeoUtils.positionToIndex(r[0]), GeoUtils.positionToIndex(r[1])}).collect(Collectors.toList());
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
     * @param desiredRanges
     *            the desired number of ranges to generate
     * @return an optimized list of numeric index ranges
     */
    public static List<long[]> generateOptimizedPositionRanges(Geometry geometry, List<long[]> ranges, int desiredRanges) {

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

        // starting out, add only the ranges which intersect with the query geometry
        for (long[] range : ranges) {
            if (GeoUtils.positionRangeToGeometry(range[0], range[1]).intersects(geometry)) {
                double difference = areaDifference(positionRangeToGeometry(range[0], range[1]), geometry);
                RangeData rangeData = new RangeData(range, difference);
                areaSortedRanges.add(rangeData);
                positionSortedRanges.add(rangeData);
            }
        }

        // stop when we have the desired number of ranges
        while (countDiscreteRanges(positionSortedRanges) < desiredRanges) {
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
        String index = positionToIndex(position);

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
    public static Geometry positionToGeometry(long position) {
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
     * Converts a geo index to its equivalent bounding box geometry
     *
     * @param index
     *            the geo index
     * @return a bounding box geometry
     */
    public static Geometry indexToGeometry(String index) {
        return positionToGeometry(indexToPosition(index));
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

            Geometry minGeom = positionToGeometry(range[0]);
            Arrays.stream(minGeom.getCoordinates()).forEach(c -> {
                longitudes.add(c.x);
                latitudes.add(c.y);
            });

            double minLat = Collections.min(latitudes);
            double minLon = Collections.min(longitudes);
            latitudes.clear();
            longitudes.clear();

            Geometry maxGeom = positionToGeometry(range[1] - 1);
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

        return new GeometryCollection(geometries.toArray(new Geometry[0]), gf).union();
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
}
