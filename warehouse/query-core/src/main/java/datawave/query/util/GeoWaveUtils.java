package datawave.query.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;

import datawave.data.normalizer.GeometryNormalizer;

/**
 * This utility class contains a variety of methods which can be used to perform operations on GeoWave ranges.
 *
 * These methods assume that a full incremental tiered index strategy is being used, with a maximum of 31 bits per dimension, and using the Hilbert
 * Space-Filling Curve. No guarantees are made as to the effectiveness or accuracy of these methods given any other configuration.
 */
public class GeoWaveUtils {

    /**
     * This is a convenience class used within decomposeRange.
     */
    private static class TierMinMax {
        public int tier;
        public long min;
        public long max;

        public TierMinMax(int tier, long min, long max) {
            this.tier = tier;
            this.min = min;
            this.max = max;
        }
    }

    /**
     * Ensures that the byte buffer is the right size, and has been cleared.
     *
     * @param longBuffer
     *            the byte buffer
     * @return the provided buffer
     */
    private static ByteBuffer initLongBuffer(ByteBuffer longBuffer) {
        longBuffer = (longBuffer != null && longBuffer.array().length == Long.BYTES) ? longBuffer : ByteBuffer.allocate(Long.BYTES);
        longBuffer.clear();

        return longBuffer;
    }

    /**
     * Optimizes the list of byte array ranges needed to query the desired area. Portions of each range which do not intersect the original query polygon will
     * be pruned out.
     *
     * @param queryGeometry
     *            the original query geometry used to create the list of byte array ranges
     * @param byteArrayRanges
     *            the original byte array ranges generated for the query geometry
     * @param rangeSplitThreshold
     *            used to determine the minimum number of segments to break a range into - higher values will take longer to compute, but will yield tighter
     *            ranges
     * @param maxRangeOverlap
     *            the maximum amount of overlap a range is allowed to have compared to the envelope of the query geometry - expressed as a double between 0 and
     *            1.
     * @return a list of optimized byte array ranges
     */
    public static List<ByteArrayRange> optimizeByteArrayRanges(Geometry queryGeometry, List<ByteArrayRange> byteArrayRanges, int rangeSplitThreshold,
                    double maxRangeOverlap) {
        return optimizeByteArrayRanges(queryGeometry, byteArrayRanges, rangeSplitThreshold, maxRangeOverlap, null);
    }

    /**
     * Optimizes the list of byte array ranges needed to query the desired area. Portions of each range which do not intersect the original query polygon will
     * be pruned out.
     *
     * @param queryGeometry
     *            the original query geometry used to create the list of byte array ranges
     * @param byteArrayRanges
     *            the original byte array ranges generated for the query geometry
     * @param rangeSplitThreshold
     *            used to determine the minimum number of segments to break a range into - higher values will take longer to compute, but will yield tighter
     *            ranges
     * @param maxRangeOverlap
     *            the maximum amount of overlap a range is allowed to have compared to the envelope of the query geometry - expressed as a double between 0 and
     *            1.
     * @param longBuffer
     *            a reusable byte buffer of Long.BYTES length
     * @return a list of optimized byte array ranges
     */
    public static List<ByteArrayRange> optimizeByteArrayRanges(Geometry queryGeometry, List<ByteArrayRange> byteArrayRanges, int rangeSplitThreshold,
                    double maxRangeOverlap, ByteBuffer longBuffer) {
        longBuffer = initLongBuffer(longBuffer);

        List<ByteArrayRange> optimizedRanges = new ArrayList<>();
        for (ByteArrayRange byteArrayRange : byteArrayRanges) {
            if (!Arrays.equals(byteArrayRange.getStart(), byteArrayRange.getEnd())) {
                optimizedRanges.addAll(optimizeByteArrayRange(queryGeometry, byteArrayRange, rangeSplitThreshold, maxRangeOverlap, longBuffer));
            } else {
                optimizedRanges.add(byteArrayRange);
            }
        }
        return optimizedRanges;
    }

    /**
     * Optimizes the list of byte array ranges needed to query the desired area. Portions of each range which do not intersect the original query polygon will
     * be pruned out.
     *
     * @param queryGeometry
     *            the original query geometry used to create the list of byte array ranges
     * @param byteArrayRange
     *            a byte array range representing a portion of the query geometry
     * @param rangeSplitThreshold
     *            used to determine the minimum number of segments to break a range into - higher values will take longer to compute, but will yield tighter
     *            ranges
     * @param maxRangeOverlap
     *            the maximum amount of overlap a range is allowed to have compared to the envelope of the query geometry - expressed as a double between 0 and
     *            1.
     * @return a list of optimized byte array ranges
     */
    public static List<ByteArrayRange> optimizeByteArrayRange(Geometry queryGeometry, ByteArrayRange byteArrayRange, int rangeSplitThreshold,
                    double maxRangeOverlap) {
        return optimizeByteArrayRange(queryGeometry, byteArrayRange, rangeSplitThreshold, maxRangeOverlap, null);
    }

    /**
     * Optimizes the list of byte array ranges needed to query the desired area. Portions of each range which do not intersect the original query polygon will
     * be pruned out.
     *
     * @param queryGeometry
     *            the original query geometry used to create the list of byte array ranges
     * @param byteArrayRange
     *            a byte array range representing a portion of the query geometry
     * @param rangeSplitThreshold
     *            used to determine the minimum number of segments to break a range into - higher values will take longer to compute, but will yield tighter
     *            ranges
     * @param maxRangeOverlap
     *            the maximum amount of overlap a range is allowed to have compared to the envelope of the query geometry - expressed as a double between 0 and
     *            1.
     * @param longBuffer
     *            a reusable byte buffer of Long.BYTES length
     * @return a list of optimized byte array ranges
     */
    public static List<ByteArrayRange> optimizeByteArrayRange(Geometry queryGeometry, ByteArrayRange byteArrayRange, int rangeSplitThreshold,
                    double maxRangeOverlap, ByteBuffer longBuffer) {
        GeometryFactory gf = new GeometryFactory();
        List<ByteArrayRange> byteArrayRanges = new ArrayList<>();

        int tier = decodeTier(byteArrayRange.getStart());
        if (tier == 0) {
            byteArrayRanges.add(byteArrayRange);
        } else {
            longBuffer = initLongBuffer(longBuffer);

            long min = decodePosition(byteArrayRange.getStart(), longBuffer);
            long max = decodePosition(byteArrayRange.getEnd(), longBuffer);
            long range = max - min + 1;

            // It's too expensive to check every geohash in the range to see if it
            // intersects with the original query geometry, so we will attempt to project
            // this range to an equivalent range at a lower granularity tier to minimize
            // the number of geohashes we need to check. By doing this, we can adjust
            // the level of granularity used to prune our ranges.
            // This is controlled by modifying the chunks per range. Higher chunks per
            // range will achieve greater pruning, but will be more expensive to compute,
            // and will introduce more query ranges (which has performance implications
            // as well).
            for (int curTier = 0; curTier <= tier; curTier++) {
                long scale = (long) Math.pow(2.0, 2.0 * (tier - curTier));

                if (range >= scale) {
                    long scaledMin = (long) Math.ceil((double) min / scale);
                    long scaledMax = max / scale;

                    if ((scaledMax - scaledMin + 1) >= rangeSplitThreshold) {
                        boolean simplifiedRanges = false;
                        long subRangeMin = scaledMin * scale;
                        long subRangeMax = Long.MIN_VALUE;

                        for (long scaledPos = scaledMin; scaledPos <= scaledMax; scaledPos++) {
                            long nextSubRangeMax = (scaledPos * scale + scale - 1);

                            if (nextSubRangeMax <= max) {
                                simplifiedRanges = true;
                                subRangeMax = nextSubRangeMax;

                                // make sure that this condensed hash is within the bounds of the map
                                byte[] scaledId = createByteArray(curTier, scaledPos, longBuffer);
                                MultiDimensionalNumericData scaledBounds = GeometryNormalizer.getGeometryIndexStrategy().getRangeForId(null, scaledId);

                                // make sure that the scaled id is within the bounds of the map
                                // note: all cells for tiers 0 and 1 are within the bounds of the map
                                if (curTier <= 1 || inBounds(scaledBounds)) {

                                    Geometry scaledGeom = boundsToGeometry(gf, scaledBounds);

                                    // make sure that the scaled geometry intersects the original query geometry
                                    if (scaledGeom.intersects(queryGeometry)) {
                                        byteArrayRanges.add(createByteArrayRange(tier, scaledPos * scale, scaledPos * scale + scale - 1, longBuffer));
                                    }
                                }
                            } else {
                                break;
                            }
                        }

                        if (simplifiedRanges) {
                            if (min < subRangeMin && rangeToGeometry(tier, min, subRangeMin - 1).intersects(queryGeometry)) {
                                byteArrayRanges.add(createByteArrayRange(tier, min, subRangeMin - 1, longBuffer));
                            }

                            if (max > subRangeMax && rangeToGeometry(tier, subRangeMax + 1, max).intersects(queryGeometry)) {
                                byteArrayRanges.add(createByteArrayRange(tier, subRangeMax + 1, max, longBuffer));
                            }
                            break;
                        }
                    }
                }
            }

            if (byteArrayRanges.isEmpty()) {
                if (rangeToGeometry(tier, min, max).intersects(queryGeometry))
                    byteArrayRanges.add(byteArrayRange);
            } else {
                if (byteArrayRanges.size() > 1)
                    byteArrayRanges = mergeContiguousRanges(byteArrayRanges, longBuffer);
                if (!byteArrayRanges.isEmpty())
                    byteArrayRanges = splitLargeRanges(byteArrayRanges, queryGeometry, maxRangeOverlap, longBuffer);
            }
        }

        return byteArrayRanges;
    }

    /**
     * Creates a bounding box geometry from MultiDimensionalNumericData. Bounds with latitudes greater than 90 and less than -90 will be appropriately clamped.
     *
     * @param gf
     *            the geometry factory to use
     * @param bounds
     *            the bounds of the box
     * @return a bounding box geometry
     */
    private static Geometry boundsToGeometry(GeometryFactory gf, MultiDimensionalNumericData bounds) {
        // @formatter:off
        return gf.toGeometry(
                new Envelope(
                        bounds.getMinValuesPerDimension()[0],
                        bounds.getMaxValuesPerDimension()[0],
                        Math.max(-90, bounds.getMinValuesPerDimension()[1]),
                        Math.min(90, bounds.getMaxValuesPerDimension()[1])));
        // @formatter:on
    }

    /**
     * Merges contiguous ranges in the list - assumes that the list of ranges is already sorted
     *
     * @param byteArrayRanges
     *            the sorted list of ranges to merge
     * @return a list of merged ranges
     */
    public static List<ByteArrayRange> mergeContiguousRanges(List<ByteArrayRange> byteArrayRanges) {
        return mergeContiguousRanges(byteArrayRanges, null);
    }

    /**
     * Merges contiguous ranges in the list - assumes that the list of ranges is already sorted
     *
     * @param byteArrayRanges
     *            the sorted list of ranges to merge
     * @param longBuffer
     *            a reusable byte buffer of Long.BYTES length
     * @return a list of merged ranges
     */
    public static List<ByteArrayRange> mergeContiguousRanges(List<ByteArrayRange> byteArrayRanges, ByteBuffer longBuffer) {
        longBuffer = initLongBuffer(longBuffer);

        List<ByteArrayRange> mergedByteArrayRanges = new ArrayList<>(byteArrayRanges.size());
        ByteArrayRange currentRange = null;

        for (ByteArrayRange range : byteArrayRanges) {
            if (currentRange == null) {
                currentRange = range;
            } else {
                long currentMax = decodePosition(currentRange.getEnd(), longBuffer);
                long nextMin = decodePosition(range.getStart(), longBuffer);

                if ((currentMax + 1) == nextMin) {
                    currentRange = new ByteArrayRange(currentRange.getStart(), range.getEnd(), false);
                } else {
                    mergedByteArrayRanges.add(currentRange);
                    currentRange = range;
                }
            }
        }

        if (currentRange != null) {
            mergedByteArrayRanges.add(currentRange);
        }

        return mergedByteArrayRanges;
    }

    /**
     * Splits ranges whose area overlaps more than maxRangeOverlap of the area of the queryGeometry envelope.
     *
     * @param byteArrayRanges
     *            the list of ranges to split
     * @param queryGeometry
     *            the original query geometry
     * @param maxRangeOverlap
     *            the maximum percentage overlap allowed for a range compared to the envelope of the original query geometry
     * @return a list of ranges, each of which overlaps less than maxRangeOverlap of the original query geometry
     */
    public static List<ByteArrayRange> splitLargeRanges(List<ByteArrayRange> byteArrayRanges, Geometry queryGeometry, double maxRangeOverlap) {
        return splitLargeRanges(byteArrayRanges, queryGeometry, maxRangeOverlap, null);
    }

    /**
     * Splits ranges whose area overlaps more than maxRangeOverlap of the area of the queryGeometry envelope.
     *
     * @param byteArrayRanges
     *            the list of ranges to split
     * @param queryGeometry
     *            the original query geometry
     * @param maxRangeOverlap
     *            the maximum percentage overlap allowed for a range compared to the envelope of the original query geometry
     * @param longBuffer
     *            a reusable byte buffer of Long.BYTES length
     * @return a list of ranges, each of which overlaps less than maxRangeOverlap of the original query geometry
     */
    public static List<ByteArrayRange> splitLargeRanges(List<ByteArrayRange> byteArrayRanges, Geometry queryGeometry, double maxRangeOverlap,
                    ByteBuffer longBuffer) {
        longBuffer = initLongBuffer(longBuffer);

        List<ByteArrayRange> splitByteArrayRanges = new ArrayList<>();

        for (ByteArrayRange range : byteArrayRanges) {
            int tier = decodeTier(range.getStart());
            long min = decodePosition(range.getStart(), longBuffer);
            long max = decodePosition(range.getEnd(), longBuffer);

            Geometry rangeGeometry = rangeToGeometry(tier, min, max);
            if (rangeGeometry.getArea() > maxRangeOverlap * queryGeometry.getEnvelope().getArea()) {
                int numSubRanges = (int) (rangeGeometry.getArea() / (maxRangeOverlap * queryGeometry.getEnvelope().getArea())) + 1;
                long offset = (max - min) / numSubRanges;

                for (int i = 0; i < numSubRanges; i++) {
                    long subMax = ((i + 1) == numSubRanges) ? max : min + (i + 1) * offset - 1;
                    splitByteArrayRanges.add(createByteArrayRange(tier, min + i * offset, subMax, longBuffer));
                }
            } else {
                splitByteArrayRanges.add(range);
            }
        }
        return splitByteArrayRanges;
    }

    /**
     * Extracts the tier from the GeoWave geohash
     *
     * @param geohash
     *            a geohash string
     * @return the tier from the GeoWave geohash
     */
    public static int decodeTier(String geohash) {
        return Integer.parseInt(geohash.substring(0, 2), 16);
    }

    /**
     * Extracts the tier from the byte array
     *
     * @param byteArray
     *            a byte array
     * @return the tier from the byte array
     */
    public static int decodeTier(byte[] byteArray) {
        return byteArray[0];
    }

    /**
     * Extracts the position from the GeoWave geohash
     *
     * @param geohash
     *            a geohash string
     * @return the position from the GeoWave geohash
     */
    public static long decodePosition(String geohash) {
        return geohash.equals("00") ? 0L : Long.parseLong(geohash.substring(2), 16);
    }

    /**
     * Extracts the position from the byte array
     *
     * @param byteArray
     *            a byte array
     * @return position from the byte array
     */
    public static long decodePosition(byte[] byteArray) {
        return decodePosition(byteArray, null);
    }

    /**
     * Extracts the position from the byte array
     *
     * @param byteArray
     *            a byte array
     * @param longBuffer
     *            a reusable byte buffer of Long.BYTES length
     * @return the position from the byte array
     */
    public static long decodePosition(byte[] byteArray, ByteBuffer longBuffer) {
        if (byteArray[0] != (byte) 0) {
            longBuffer = initLongBuffer(longBuffer);

            for (int i = 0; i < (Long.BYTES - (byteArray.length - 1)); i++)
                longBuffer.put((byte) 0);

            longBuffer.put(byteArray, 1, byteArray.length - 1);
            return longBuffer.getLong(0);
        } else {
            return 0L;
        }
    }

    /**
     * Determines the number of hex characters needed to represent a position at a given tier. This excludes the byte reserved for the tier identifier.
     *
     * @param tier
     *            the given tier
     * @return number of hex characters needed to represent a position
     */
    public static int hexCharsPerTier(int tier) {
        String hexString = String.format("%X", ((long) Math.pow(2.0, tier) - 1));
        if (Long.parseLong(hexString, 16) == 0)
            return 0;
        else
            return hexString.length() * 2;
    }

    /**
     * Creates a byte array from the given GeoWave geohash
     *
     * @param geohash
     *            a geowave geohash
     * @return a byte array
     */
    public static byte[] createByteArray(String geohash) {
        return createByteArray(geohash, null);
    }

    /**
     * Creates a byte array from the given tier and position
     *
     * @param tier
     *            a tier
     * @param position
     *            a position
     * @return a byte array
     */
    public static byte[] createByteArray(int tier, long position) {
        return createByteArray(tier, position, null);
    }

    /**
     * Creates a byte array from the given GeoWave geohash
     *
     * @param geohash
     *            a geohash string
     * @param longBuffer
     *            a reusable byte buffer of Long.BYTES length
     * @return a byte array
     */
    public static byte[] createByteArray(String geohash, ByteBuffer longBuffer) {
        return createByteArray(decodeTier(geohash), decodePosition(geohash), longBuffer);
    }

    /**
     * Creates a byte array from the given tier and position
     *
     * @param tier
     *            a tier
     * @param position
     *            a position
     * @param longBuffer
     *            a reusable byte buffer of Long.BYTES length
     * @return a byte array
     */
    public static byte[] createByteArray(int tier, long position, ByteBuffer longBuffer) {
        if (tier != 0) {
            longBuffer = initLongBuffer(longBuffer);

            ByteBuffer buffer = ByteBuffer.allocate(hexCharsPerTier(tier) / 2 + 1);
            buffer.put((byte) tier);
            longBuffer.putLong(position);
            buffer.put(longBuffer.array(), longBuffer.capacity() - buffer.remaining(), buffer.remaining());

            return buffer.array();
        } else {
            return new byte[] {0};
        }
    }

    /**
     * Creates a ByteArrayRange from the given start and end GeoWave geohashes
     *
     * @param startGeohash
     *            a start geohash
     * @param endGeohash
     *            an end geohash
     * @return a ByteArrayRange
     */
    public static ByteArrayRange createByteArrayRange(String startGeohash, String endGeohash) {
        return createByteArrayRange(startGeohash, endGeohash, null);
    }

    /**
     * Creates a ByteArrayRange from the given tier, and min and max positions
     *
     * @param tier
     *            a tier
     * @param min
     *            min position
     * @param max
     *            max position
     * @return a ByteArrayRange
     */
    public static ByteArrayRange createByteArrayRange(int tier, long min, long max) {
        return createByteArrayRange(tier, min, max, null);
    }

    /**
     * Creates a ByteArrayRange from the given start and end GeoWave geohashes
     *
     * @param startGeohash
     *            a start geohash
     * @param endGeohash
     *            an end geohash
     * @param longBuffer
     *            a reusable byte buffer of Long.BYTES length
     * @return a ByteArrayRange
     */
    public static ByteArrayRange createByteArrayRange(String startGeohash, String endGeohash, ByteBuffer longBuffer) {
        return createByteArrayRange(decodeTier(startGeohash), decodePosition(startGeohash), decodePosition(endGeohash), longBuffer);
    }

    /**
     * Creates a ByteArrayRange from the given tier, and min and max positions
     *
     * @param tier
     *            a given tier
     * @param min
     *            min position
     * @param max
     *            max position
     * @param longBuffer
     *            a reusable byte buffer of Long.BYTES length
     * @return a ByteArrayRange
     */
    public static ByteArrayRange createByteArrayRange(int tier, long min, long max, ByteBuffer longBuffer) {
        longBuffer = initLongBuffer(longBuffer);

        return new ByteArrayRange(createByteArray(tier, min, longBuffer), createByteArray(tier, max, longBuffer), min == max);
    }

    /**
     * Determines whether the given bounds are within the bounds of the map.
     *
     * @param bounds
     *            the given bounds
     * @return whether the given bounds are within the bounds of the map
     */
    private static boolean inBounds(MultiDimensionalNumericData bounds) {
        // @formatter:off
        return bounds.getMinValuesPerDimension()[0] >= -180 && bounds.getMinValuesPerDimension()[0] <= 180 &&
                bounds.getMaxValuesPerDimension()[0] >= -180 && bounds.getMaxValuesPerDimension()[0] <= 180 &&
                bounds.getMinValuesPerDimension()[1] >= -90 && bounds.getMinValuesPerDimension()[1] <= 90 &&
                bounds.getMaxValuesPerDimension()[1] >= -90 && bounds.getMaxValuesPerDimension()[1] <= 90;
        // @formatter:on
    }

    /**
     * Given a GeoWave geohash position, this will generate a Geometry which represents that position.
     *
     * @param geohash
     *            geohash position
     * @return Geometry which represents that position
     */
    public static Geometry positionToGeometry(String geohash) {
        return positionToGeometry(geohash, null);
    }

    /**
     * Given a position at a given tier, this will generate a Geometry which represents that position.
     *
     * @param tier
     *            a tier
     * @param position
     *            a position
     * @return a Geometry
     */
    public static Geometry positionToGeometry(int tier, long position) {
        return positionToGeometry(tier, position, null);
    }

    /**
     * Given a GeoWave geohash position, this will generate a Geometry which represents that position.
     *
     * @param geohash
     *            a GeoWave geohash position
     * @param longBuffer
     *            a reusable byte buffer of Long.BYTES length
     * @return a Geometry
     */
    public static Geometry positionToGeometry(String geohash, ByteBuffer longBuffer) {
        longBuffer = initLongBuffer(longBuffer);

        return positionToGeometry(createByteArray(geohash, longBuffer));
    }

    /**
     * Given a position at a given tier, this will generate a Geometry which represents that position.
     *
     * @param tier
     *            a tier
     * @param position
     *            a position
     * @param longBuffer
     *            a reusable byte buffer of Long.BYTES length
     * @return a Geometry
     */
    public static Geometry positionToGeometry(int tier, long position, ByteBuffer longBuffer) {
        longBuffer = initLongBuffer(longBuffer);

        return positionToGeometry(createByteArray(tier, position, longBuffer));
    }

    /**
     * Given a byte array, this will generate a Geometry which represents that position.
     *
     * @param byteArray
     *            a byte array
     * @return a Geometry
     */
    public static Geometry positionToGeometry(byte[] byteArray) {
        MultiDimensionalNumericData bounds = GeometryNormalizer.getGeometryIndexStrategy().getRangeForId(null, byteArray);
        return boundsToGeometry(new GeometryFactory(), bounds);
    }

    /**
     * Given a range defined by the start and end geohashes, this will generate a Geometry which represents that range.
     *
     * @param startGeohash
     *            a start geohash
     * @param endGeohash
     *            an end geohash
     * @return a Geometry
     */
    public static Geometry rangeToGeometry(String startGeohash, String endGeohash) {
        return rangeToGeometry(startGeohash, endGeohash, null);
    }

    /**
     * Given a range defined by the start and end geohashes, this will generate a Geometry which represents that range.
     *
     * @param byteArrayRange
     *            a range
     * @return a Geometry
     */
    public static Geometry rangeToGeometry(ByteArrayRange byteArrayRange) {
        return rangeToGeometry(byteArrayRange, null);
    }

    /**
     * Given a range at a given tier, this will generate a Geometry which represents that range.
     *
     * @param tier
     *            a tier
     * @param start
     *            the start
     * @param end
     *            the end
     * @return a Geometry
     */
    public static Geometry rangeToGeometry(int tier, long start, long end) {
        return rangeToGeometry(tier, start, end, null);
    }

    /**
     * Given a range defined by the start and end geohashes, this will generate a Geometry which represents that range.
     *
     * @param startGeohash
     *            a start geohash
     * @param endGeohash
     *            an end geohash
     * @param longBuffer
     *            a reusable byte buffer of Long.BYTES length
     * @return a Geometry
     */
    public static Geometry rangeToGeometry(String startGeohash, String endGeohash, ByteBuffer longBuffer) {
        return rangeToGeometry(decodeTier(startGeohash), decodePosition(startGeohash), decodePosition(endGeohash), longBuffer);
    }

    /**
     * Given a range defined by byteArrayRange, this will generate a Geometry which represents that range.
     *
     * @param byteArrayRange
     *            a range defined by byteArrayRange
     * @param longBuffer
     *            a reusable byte buffer of Long.BYTES length
     * @return a Geometry
     */
    public static Geometry rangeToGeometry(ByteArrayRange byteArrayRange, ByteBuffer longBuffer) {
        return rangeToGeometry(decodeTier(byteArrayRange.getStart()), decodePosition(byteArrayRange.getStart()), decodePosition(byteArrayRange.getEnd()),
                        longBuffer);
    }

    /**
     * Given a range at a given tier, this will generate a Geometry which represents that range.
     *
     * @param tier
     *            a given tier
     * @param start
     *            a start
     * @param end
     *            a end
     * @param longBuffer
     *            a reusable byte buffer of Long.BYTES length
     * @return a Geometry
     */
    public static Geometry rangeToGeometry(int tier, long start, long end, ByteBuffer longBuffer) {
        longBuffer = initLongBuffer(longBuffer);

        GeometryFactory gf = new GeometryFactory();

        List<byte[]> byteArrays = decomposeRange(tier, start, end, longBuffer);

        List<Geometry> geometries = new ArrayList<>(byteArrays.size());
        for (byte[] byteArray : byteArrays) {
            MultiDimensionalNumericData bounds = GeometryNormalizer.getGeometryIndexStrategy().getRangeForId(null, byteArray);

            if (decodeTier(byteArray) <= 1 || inBounds(bounds)) {
                geometries.add(boundsToGeometry(gf, bounds));
            }
        }

        // union can introduce holes in the final geometry due to the level of precision used to
        // represent the geometries, so we need to ensure that we are only keeping the exterior ring
        Geometry unionedGeometry = new GeometryCollection(geometries.toArray(new Geometry[0]), gf).union();
        if (unionedGeometry instanceof Polygon && ((Polygon) unionedGeometry).getNumInteriorRing() > 0) {
            return gf.createPolygon(((Polygon) unionedGeometry).getExteriorRing().getCoordinates());
        } else {
            return unionedGeometry;
        }
    }

    /**
     * This performs a sort of quad-tree decomposition on the given range. This algorithm searches for subranges within the original range which can be
     * represented in a simplified fashion at a lower granularity tier. The resulting list of byte arrays will consist of an equivalent set of ids, spread out
     * across multiple tiers, which is topologically equivalent to the footprint of the original range.
     *
     * @param startGeohash
     *            a start geohash
     * @param endGeohash
     *            an end geohash
     * @return equivalent set of ids
     */
    public static List<byte[]> decomposeRange(String startGeohash, String endGeohash) {
        return decomposeRange(startGeohash, endGeohash, null);
    }

    /**
     * This performs a sort of quad-tree decomposition on the given range. This algorithm searches for subranges within the original range which can be
     * represented in a simplified fashion at a lower granularity tier. The resulting list of byte arrays will consist of an equivalent set of ids, spread out
     * across multiple tiers, which is topologically equivalent to the footprint of the original range.
     *
     * @param tier
     *            a tier
     * @param start
     *            the start
     * @param end
     *            the end
     * @return equivalent set of ids
     */
    public static List<byte[]> decomposeRange(int tier, long start, long end) {
        return decomposeRange(tier, start, end, null);
    }

    /**
     * This performs a sort of quad-tree decomposition on the given range. This algorithm searches for subranges within the original range which can be
     * represented in a simplified fashion at a lower granularity tier. The resulting list of byte arrays will consist of an equivalent set of ids, spread out
     * across multiple tiers, which is topologically equivalent to the footprint of the original range.
     *
     * @param startGeohash
     *            a start geohash
     * @param endGeohash
     *            an end geohash
     * @param longBuffer
     *            a reusable byte buffer of Long.BYTES length
     * @return equivalent set of ids
     */
    public static List<byte[]> decomposeRange(String startGeohash, String endGeohash, ByteBuffer longBuffer) {
        return decomposeRange(decodeTier(startGeohash), decodePosition(startGeohash), decodePosition(endGeohash), longBuffer);
    }

    /**
     * This performs a sort of quad-tree decomposition on the given range. This algorithm searches for subranges within the original range which can be
     * represented in a simplified fashion at a lower granularity tier. The resulting list of byte arrays will consist of an equivalent set of ids, spread out
     * across multiple tiers, which is topologically equivalent to the footprint of the original range.
     *
     * @param tier
     *            a tier
     * @param start
     *            the start
     * @param end
     *            the end
     * @param longBuffer
     *            a reusable byte buffer of Long.BYTES length
     * @return equivalent set of ids
     */
    public static List<byte[]> decomposeRange(int tier, long start, long end, ByteBuffer longBuffer) {
        longBuffer = initLongBuffer(longBuffer);

        List<byte[]> byteArrays = new ArrayList<>();

        LinkedList<TierMinMax> queue = new LinkedList<>();
        queue.push(new TierMinMax(0, start, end));

        while (!queue.isEmpty()) {
            TierMinMax tierMinMax = queue.pop();
            long range = tierMinMax.max - tierMinMax.min + 1;

            while (tierMinMax.tier <= tier) {
                long scale = (long) Math.pow(2.0, 2.0 * (tier - tierMinMax.tier));

                if (range >= scale) {
                    long scaledMin = (long) Math.ceil((double) tierMinMax.min / scale);
                    long scaledMax = tierMinMax.max / scale;

                    boolean simplifiedRanges = false;
                    long subRangeMin = scaledMin * scale;
                    long subRangeMax = Long.MIN_VALUE;

                    for (long scaledPos = scaledMin; scaledPos <= scaledMax; scaledPos++) {
                        long nextSubRangeMax = (scaledPos * scale + scale - 1);

                        if (nextSubRangeMax <= tierMinMax.max) {
                            simplifiedRanges = true;
                            subRangeMax = nextSubRangeMax;

                            byteArrays.add(createByteArray(tierMinMax.tier, scaledPos, longBuffer));
                        } else {
                            break;
                        }
                    }

                    if (simplifiedRanges) {
                        if (tierMinMax.min < subRangeMin) {
                            queue.push(new TierMinMax(tierMinMax.tier + 1, tierMinMax.min, subRangeMin - 1));
                        }

                        if (subRangeMax < tierMinMax.max) {
                            queue.push(new TierMinMax(tierMinMax.tier + 1, subRangeMax + 1, tierMinMax.max));
                        }

                        break;
                    }
                }

                tierMinMax.tier++;
            }
        }
        return byteArrays;
    }
}
