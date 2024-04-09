package datawave.core.geo.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.WKTReader;

import datawave.data.normalizer.GeometryNormalizer;
import datawave.data.normalizer.PointNormalizer;

public class GeoWaveUtilsTest {

    // add tests that showcase geo fields used in geowave functions
    // add tests that showcase geowave fields used in geo functions

    @Test
    public void optimizeSinglePointTest() throws Exception {
        String wktString = "POINT(12.34567890123 9.87654321098)";
        Geometry geom = new WKTReader().read(wktString);

        List<ByteArrayRange> byteArrayRanges = new ArrayList<>();
        for (MultiDimensionalNumericData range : GeometryUtils.basicConstraintsFromEnvelope(geom.getEnvelopeInternal())
                        .getIndexConstraints(PointNormalizer.getPointIndex())) {
            byteArrayRanges.addAll(PointNormalizer.getPointIndexStrategy().getQueryRanges(range, 32).getCompositeQueryRanges());
        }

        List<ByteArrayRange> optimizedByteArrayRanges = GeoWaveUtils.optimizeByteArrayRanges(geom, byteArrayRanges, 16, 0.25);

        assertEquals(1, optimizedByteArrayRanges.size());
    }

    @Test
    public void optimizeByteArrayRangesTest() throws Exception {
        String wktString = "POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))";
        Geometry geom = new WKTReader().read(wktString);

        List<ByteArrayRange> byteArrayRanges = new ArrayList<>();
        for (MultiDimensionalNumericData range : GeometryUtils.basicConstraintsFromEnvelope(geom.getEnvelopeInternal())
                        .getIndexConstraints(GeometryNormalizer.getGeometryIndex())) {
            byteArrayRanges.addAll(GeometryNormalizer.getGeometryIndexStrategy().getQueryRanges(range, 8).getCompositeQueryRanges());
        }

        // count the number of cells included in the original ranges
        long numUnoptimizedIndices = byteArrayRanges.stream()
                        .map(range -> (GeoWaveUtils.decodePosition(range.getEnd()) - GeoWaveUtils.decodePosition(range.getStart()) + 1)).reduce(0L, Long::sum);

        List<ByteArrayRange> optimizedByteArrayRanges = GeoWaveUtils.optimizeByteArrayRanges(geom, byteArrayRanges, 16, 0.25);

        // count the number of cells included in the optimized ranges
        long numOptimizedIndices = optimizedByteArrayRanges.stream()
                        .map(range -> (GeoWaveUtils.decodePosition(range.getEnd()) - GeoWaveUtils.decodePosition(range.getStart()) + 1)).reduce(0L, Long::sum);

        assertTrue(numOptimizedIndices < numUnoptimizedIndices);

        // check each tier to ensure that it covers the original geometry
        Map<Integer,List<ByteArrayRange>> rangesByTier = optimizedByteArrayRanges.stream()
                        .collect(Collectors.groupingBy(x -> GeoWaveUtils.decodeTier(x.getStart())));
        for (Map.Entry<Integer,List<ByteArrayRange>> ranges : rangesByTier.entrySet()) {
            // union can introduce holes in the final geometry due to the level of precision used to
            // represent the geometries, so we need to ensure that we are only keeping the exterior ring
            Geometry tierGeom = new GeometryFactory()
                            .createPolygon(((Polygon) ranges.getValue().stream().map(GeoWaveUtils::rangeToGeometry).reduce(Geometry::union).get())
                                            .getExteriorRing().getCoordinates());
            assertTrue(tierGeom.covers(geom));
        }
    }

    @Test
    public void optimizeByteArrayRangeTest() throws Exception {
        String wktString = "POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))";
        Geometry geom = new WKTReader().read(wktString);

        ByteArrayRange byteArrayRange = GeoWaveUtils.createByteArrayRange("1f0aa02a0000000000", "1f0aa0d5ffffffffff");

        // count the number of cells included in the original ranges
        long numUnoptimizedIndices = GeoWaveUtils.decodePosition(byteArrayRange.getEnd()) - GeoWaveUtils.decodePosition(byteArrayRange.getStart()) + 1;

        List<ByteArrayRange> optimizedByteArrayRanges = GeoWaveUtils.optimizeByteArrayRange(geom, byteArrayRange, 16, 0.25);

        // count the number of cells included in the optimized ranges
        long numOptimizedIndices = optimizedByteArrayRanges.stream()
                        .map(range -> (GeoWaveUtils.decodePosition(range.getEnd()) - GeoWaveUtils.decodePosition(range.getStart()) + 1)).reduce(0L, Long::sum);

        assertTrue(numOptimizedIndices < numUnoptimizedIndices);

        Geometry tierGeom = GeoWaveUtils.rangeToGeometry(byteArrayRange);

        // union can introduce holes in the final geometry due to the level of precision used to
        // represent the geometries, so we need to ensure that we are only keeping the exterior ring
        Geometry optimizedTierGeom = new GeometryFactory()
                        .createPolygon(((Polygon) optimizedByteArrayRanges.stream().map(GeoWaveUtils::rangeToGeometry).reduce(Geometry::union).get())
                                        .getExteriorRing().getCoordinates());

        // ensure that the optimized range is covered by the unoptimized range
        assertTrue(tierGeom.covers(optimizedTierGeom));

        // ensure that the area of the optimized range is smaller than the unoptimized range
        assertTrue(tierGeom.getArea() > optimizedTierGeom.getArea());
    }

    @Test
    public void mergeContiguousRangesTest() {
        ByteArrayRange fullRange = GeoWaveUtils.createByteArrayRange("1f0aa02a0000000000", "1f0aa0d5ffffffffff");

        List<ByteArrayRange> byteArrayRanges = new ArrayList<>();
        byteArrayRanges.add(GeoWaveUtils.createByteArrayRange("1f0aa02a0000000000", "1f0aa0800000000000"));
        byteArrayRanges.add(GeoWaveUtils.createByteArrayRange("1f0aa0800000000001", "1f0aa0d5ffffffffff"));

        List<ByteArrayRange> mergedRanges = GeoWaveUtils.mergeContiguousRanges(byteArrayRanges);
        assertEquals(1, mergedRanges.size());

        assertEquals(fullRange, mergedRanges.get(0));
    }

    @Test
    public void splitLargeRangesTest() throws Exception {
        String wktString = "POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90))";
        Geometry geom = new WKTReader().read(wktString);

        List<ByteArrayRange> byteArrayRanges = new ArrayList<>();
        byteArrayRanges.add(GeoWaveUtils.createByteArrayRange("0100", "0103"));

        List<ByteArrayRange> splitByteArrayRanges = GeoWaveUtils.splitLargeRanges(byteArrayRanges, geom, .75);

        assertEquals(2, splitByteArrayRanges.size());
    }

    @Test
    public void decodeTierTest() {
        // String
        assertEquals(1, GeoWaveUtils.decodeTier("0101"));

        // ByteArrayId
        assertEquals(31, GeoWaveUtils.decodeTier(GeoWaveUtils.createByteArray(31, 0)));
    }

    @Test
    public void decodePositionTest() {
        // String
        assertEquals(3, GeoWaveUtils.decodePosition("0103"));

        // ByteArrayId
        assertEquals(123456, GeoWaveUtils.decodePosition(GeoWaveUtils.createByteArray(31, 123456)));
    }

    @Test
    public void createByteArrayIdTest() {
        // String
        ByteArray byteArrayId = new ByteArray(GeoWaveUtils.createByteArray("0102"));
        assertEquals("0102", byteArrayId.getHexString().replace(" ", ""));

        // int, long
        byteArrayId = new ByteArray(GeoWaveUtils.createByteArray(2, 4));
        assertEquals("0204", byteArrayId.getHexString().replace(" ", ""));
    }

    @Test
    public void createByteArrayRangeTest() {
        // String, String
        ByteArrayRange byteArrayRange = GeoWaveUtils.createByteArrayRange("0100", "0103");
        assertEquals("0100", new ByteArray(byteArrayRange.getStart()).getHexString().replace(" ", ""));
        assertEquals("0103", new ByteArray(byteArrayRange.getEnd()).getHexString().replace(" ", ""));

        // int, long, long
        byteArrayRange = GeoWaveUtils.createByteArrayRange(4, 0, 20);
        assertEquals("0400", new ByteArray(byteArrayRange.getStart()).getHexString().replace(" ", ""));
        assertEquals("0414", new ByteArray(byteArrayRange.getEnd()).getHexString().replace(" ", ""));
    }

    @Test
    public void positionToGeometryTest() throws Exception {
        // String
        Geometry geom = GeoWaveUtils.termToGeometry("00");
        Geometry expectedGeom = new WKTReader().read("POLYGON((-180 -90, -180 90, 180 90, 180 -90, -180 -90))");
        assertEquals(expectedGeom.toText(), geom.toText());

        // int, long
        geom = GeoWaveUtils.positionToGeometry(3, 32);
        expectedGeom = new WKTReader().read("POLYGON ((0 0, 0 45, 45 45, 45 0, 0 0))");
        assertEquals(expectedGeom.toText(), geom.toText());
    }

    @Test
    public void rangeToGeometryTest() throws Exception {
        // String, String
        Geometry geom = GeoWaveUtils.rangeToGeometry("0100", "0101");
        Geometry expectedGeom = new WKTReader().read("POLYGON((-180 -90, -180 0, -180 90, 0 90, 0 0, 0 -90, -180 -90))");
        assertEquals(expectedGeom.toText(), geom.toText());

        // int, long, long
        geom = GeoWaveUtils.rangeToGeometry(2, 0, 7);
        expectedGeom = new WKTReader().read("POLYGON((-180 -90, -180 0, -180 90, 0 90, 0 0, 0 -90, -180 -90))");
        assertEquals(expectedGeom.toText(), geom.toText());
    }

    @Test
    public void decomposeRangeTest() {
        // String, String
        List<ByteArray> byteArrayIds = GeoWaveUtils.decomposeRange("0200", "0207").stream().map(ByteArray::new).collect(Collectors.toList());
        Collections.sort(byteArrayIds);
        assertEquals(2, byteArrayIds.size());
        assertEquals(1, GeoWaveUtils.decodeTier(byteArrayIds.get(0).getBytes()));
        assertEquals(0, GeoWaveUtils.decodePosition(byteArrayIds.get(0).getBytes()));
        assertEquals(1, GeoWaveUtils.decodeTier(byteArrayIds.get(1).getBytes()));
        assertEquals(1, GeoWaveUtils.decodePosition(byteArrayIds.get(1).getBytes()));

        // int, long, long
        byteArrayIds = GeoWaveUtils.decomposeRange("0200", "020b").stream().map(ByteArray::new).collect(Collectors.toList());
        Collections.sort(byteArrayIds);
        assertEquals(3, byteArrayIds.size());
        assertEquals(1, GeoWaveUtils.decodeTier(byteArrayIds.get(0).getBytes()));
        assertEquals(0, GeoWaveUtils.decodePosition(byteArrayIds.get(0).getBytes()));
        assertEquals(1, GeoWaveUtils.decodeTier(byteArrayIds.get(1).getBytes()));
        assertEquals(1, GeoWaveUtils.decodePosition(byteArrayIds.get(1).getBytes()));
        assertEquals(1, GeoWaveUtils.decodeTier(byteArrayIds.get(2).getBytes()));
        assertEquals(2, GeoWaveUtils.decodePosition(byteArrayIds.get(2).getBytes()));
    }
}
