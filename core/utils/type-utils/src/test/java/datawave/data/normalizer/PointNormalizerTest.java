package datawave.data.normalizer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.WKTWriter;

import com.google.common.collect.Lists;

public class PointNormalizerTest {
    
    private PointNormalizer pointNormalizer = null;
    private GeometryNormalizer geometryNormalizer = null;
    
    @BeforeEach
    public void setup() {
        pointNormalizer = new PointNormalizer();
        geometryNormalizer = new GeometryNormalizer();
    }
    
    @Test
    public void testPoint() {
        Geometry point = new GeometryFactory().createPoint(new Coordinate(10, 10));
        List<String> insertionIds = new ArrayList<>(pointNormalizer.expand(new WKTWriter().write(point)));
        assertEquals(1, insertionIds.size());
        assertEquals("1f200a80a80a80a80a", insertionIds.get(0));
        
        // make sure the insertion id matches the geo normalizer
        List<String> geoInsertionIds = new ArrayList<>(geometryNormalizer.expand(new WKTWriter().write(point)));
        assertEquals(1, geoInsertionIds.size());
        assertEquals(insertionIds.get(0), geoInsertionIds.get(0));
    }
    
    @Test
    public void testLine() {
        Geometry line = new GeometryFactory().createLineString(new Coordinate[] {new Coordinate(-10, -10), new Coordinate(0, 0), new Coordinate(10, 20)});
        assertThrows(ClassCastException.class, () -> pointNormalizer.expand(new WKTWriter().write(line)));
    }
    
    @Test
    public void testPolygon() {
        Geometry polygon = new GeometryFactory().createPolygon(new Coordinate[] {new Coordinate(-10, -10), new Coordinate(10, -10), new Coordinate(10, 10),
                new Coordinate(-10, 10), new Coordinate(-10, -10)});
        assertThrows(ClassCastException.class, () -> pointNormalizer.expand(new WKTWriter().write(polygon)));
    }
    
    @Test
    public void testWKTPoint() {
        Geometry geom = AbstractGeometryNormalizer.parseGeometry("POINT(10 20)");
        assertEquals(10.0, geom.getGeometryN(0).getCoordinate().x, 0.0);
        assertEquals(20.0, geom.getGeometryN(0).getCoordinate().y, 0.0);
        
        List<String> insertionIds = new ArrayList<>(pointNormalizer.expand(new WKTWriter().write(geom)));
        assertEquals(1, insertionIds.size());
        assertEquals("1f20306ba4306ba430", insertionIds.get(0));
        
        // make sure the insertion id matches the geo normalizer
        List<String> geoInsertionIds = new ArrayList<>(geometryNormalizer.expand(new WKTWriter().write(geom.getCentroid())));
        assertEquals(1, geoInsertionIds.size());
        assertEquals(insertionIds.get(0), geoInsertionIds.get(0));
    }
    
    @Test
    public void testWKTPointz() {
        Geometry geom = AbstractGeometryNormalizer.parseGeometry("POINT Z(10 20 30)");
        assertEquals(10.0, geom.getGeometryN(0).getCoordinate().x, 0.0);
        assertEquals(20.0, geom.getGeometryN(0).getCoordinate().y, 0.0);
        assertEquals(30.0, geom.getGeometryN(0).getCoordinate().z, 0.0);
        
        List<String> insertionIds = new ArrayList<>(pointNormalizer.expand(new WKTWriter().write(geom)));
        assertEquals(1, insertionIds.size());
        assertEquals("1f20306ba4306ba430", insertionIds.get(0));
        
        // make sure the insertion id matches the geo normalizer
        List<String> geoInsertionIds = new ArrayList<>(geometryNormalizer.expand(new WKTWriter().write(geom.getCentroid())));
        assertEquals(1, geoInsertionIds.size());
        assertEquals(insertionIds.get(0), geoInsertionIds.get(0));
    }
    
    @Test
    public void testQueryRanges() throws Exception {
        Geometry polygon = new GeometryFactory().createPolygon(new Coordinate[] {new Coordinate(-10, -10), new Coordinate(10, -10), new Coordinate(10, 10),
                new Coordinate(-10, 10), new Coordinate(-10, -10)});
        
        List<ByteArrayRange> allRanges = new ArrayList<>();
        for (MultiDimensionalNumericData range : GeometryUtils.basicConstraintsFromEnvelope(polygon.getEnvelopeInternal())
                        .getIndexConstraints(PointNormalizer.getPointIndex())) {
            allRanges.addAll(Lists.reverse(PointNormalizer.getPointIndexStrategy().getQueryRanges(range).getCompositeQueryRanges()));
        }
        
        assertEquals(171, allRanges.size());
        
        StringBuilder result = new StringBuilder();
        for (ByteArrayRange range : allRanges) {
            result.append(Hex.encodeHexString(range.getStart()));
            result.append(Hex.encodeHexString(range.getEnd()));
        }
        
        String expected = IOUtils.toString(this.getClass().getClassLoader().getResourceAsStream("datawave/data/normalizer/pointRanges.txt"), "UTF8");
        
        assertEquals(expected, result.toString());
    }
    
    @Test
    public void testPointQueryRangesMatchGeoQueryRanges() {
        Geometry polygon = new GeometryFactory().createPolygon(new Coordinate[] {new Coordinate(-10, -10), new Coordinate(10, -10), new Coordinate(10, 10),
                new Coordinate(-10, 10), new Coordinate(-10, -10)});
        
        List<ByteArrayRange> allPointRanges = new ArrayList<>();
        for (MultiDimensionalNumericData range : GeometryUtils.basicConstraintsFromEnvelope(polygon.getEnvelopeInternal())
                        .getIndexConstraints(PointNormalizer.getPointIndex())) {
            allPointRanges.addAll(Lists.reverse(PointNormalizer.getPointIndexStrategy().getQueryRanges(range).getCompositeQueryRanges()));
        }
        
        assertEquals(171, allPointRanges.size());
        
        StringBuilder pointResult = new StringBuilder();
        for (ByteArrayRange range : allPointRanges) {
            pointResult.append(Hex.encodeHexString(range.getStart()));
            pointResult.append(Hex.encodeHexString(range.getEnd()));
        }
        
        List<ByteArrayRange> allGeoRanges = new ArrayList<>();
        for (MultiDimensionalNumericData range : GeometryUtils.basicConstraintsFromEnvelope(polygon.getEnvelopeInternal())
                        .getIndexConstraints(GeometryNormalizer.getGeometryIndex())) {
            allGeoRanges.addAll(Lists.reverse(GeometryNormalizer.getGeometryIndexStrategy().getQueryRanges(range).getCompositeQueryRanges()));
        }
        
        assertEquals(3746, allGeoRanges.size());
        
        int numPointRanges = 0;
        StringBuilder geoResult = new StringBuilder();
        for (ByteArrayRange range : allGeoRanges) {
            String start = Hex.encodeHexString(range.getStart());
            String end = Hex.encodeHexString(range.getEnd());
            if (start.startsWith("1f") && end.startsWith("1f")) {
                geoResult.append(start);
                geoResult.append(end);
                numPointRanges++;
            }
        }
        
        assertEquals(171, numPointRanges);
        assertEquals(geoResult.toString(), pointResult.toString());
    }
}
