package datawave.data.normalizer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Collections;
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

public class GeometryNormalizerTest {
    
    private GeometryNormalizer normalizer = null;
    
    @BeforeEach
    public void setup() {
        normalizer = new GeometryNormalizer();
    }
    
    @Test
    public void testPoint() {
        Geometry point = new GeometryFactory().createPoint(new Coordinate(10, 10));
        List<String> insertionIds = new ArrayList<>(normalizer.expand(new WKTWriter().write(point)));
        assertEquals(1, insertionIds.size());
        assertEquals("1f200a80a80a80a80a", insertionIds.get(0));
    }
    
    @Test
    public void testLine() {
        Geometry line = new GeometryFactory().createLineString(new Coordinate[] {new Coordinate(-10, -10), new Coordinate(0, 0), new Coordinate(10, 20)});
        List<String> insertionIds = new ArrayList<>(normalizer.expand(new WKTWriter().write(line)));
        Collections.sort(insertionIds);
        assertEquals(4, insertionIds.size());
        assertEquals("042a", insertionIds.get(0));
        assertEquals("047f", insertionIds.get(1));
        assertEquals("0480", insertionIds.get(2));
        assertEquals("04d5", insertionIds.get(3));
    }
    
    @Test
    public void testPolygon() {
        Geometry polygon = new GeometryFactory().createPolygon(new Coordinate[] {new Coordinate(-10, -10), new Coordinate(10, -10), new Coordinate(10, 10),
                new Coordinate(-10, 10), new Coordinate(-10, -10)});
        List<String> insertionIds = new ArrayList<>(normalizer.expand(new WKTWriter().write(polygon)));
        assertEquals(4, insertionIds.size());
        assertEquals("0500aa", insertionIds.get(0));
        assertEquals("0501ff", insertionIds.get(1));
        assertEquals("050200", insertionIds.get(2));
        assertEquals("050355", insertionIds.get(3));
    }
    
    @Test
    public void testWKTPoint() {
        Geometry geom = AbstractGeometryNormalizer.parseGeometry("POINT(10 20)");
        assertEquals(10.0, geom.getGeometryN(0).getCoordinate().x, 0.0);
        assertEquals(20.0, geom.getGeometryN(0).getCoordinate().y, 0.0);
        
        List<String> insertionIds = new ArrayList<>(normalizer.expand(new WKTWriter().write(geom)));
        assertEquals(1, insertionIds.size());
        assertEquals("1f20306ba4306ba430", insertionIds.get(0));
    }
    
    @Test
    public void testWKTPointz() {
        Geometry geom = AbstractGeometryNormalizer.parseGeometry("POINT Z(10 20 30)");
        assertEquals(10.0, geom.getGeometryN(0).getCoordinate().x, 0.0);
        assertEquals(20.0, geom.getGeometryN(0).getCoordinate().y, 0.0);
        assertEquals(30.0, geom.getGeometryN(0).getCoordinate().z, 0.0);
        
        List<String> insertionIds = new ArrayList<>(normalizer.expand(new WKTWriter().write(geom)));
        assertEquals(1, insertionIds.size());
        assertEquals("1f20306ba4306ba430", insertionIds.get(0));
    }
    
    @Test
    public void testQueryRanges() throws Exception {
        Geometry polygon = new GeometryFactory().createPolygon(new Coordinate[] {new Coordinate(-10, -10), new Coordinate(10, -10), new Coordinate(10, 10),
                new Coordinate(-10, 10), new Coordinate(-10, -10)});
        
        List<ByteArrayRange> allRanges = new ArrayList<>();
        for (MultiDimensionalNumericData range : GeometryUtils.basicConstraintsFromEnvelope(polygon.getEnvelopeInternal())
                        .getIndexConstraints(GeometryNormalizer.getGeometryIndex())) {
            allRanges.addAll(Lists.reverse(GeometryNormalizer.getGeometryIndexStrategy().getQueryRanges(range).getCompositeQueryRanges()));
        }
        
        assertEquals(3746, allRanges.size());
        
        StringBuffer result = new StringBuffer();
        for (ByteArrayRange range : allRanges) {
            result.append(Hex.encodeHexString(range.getStart()));
            result.append(Hex.encodeHexString(range.getEnd()));
        }
        
        String expected = IOUtils.toString(this.getClass().getClassLoader().getResourceAsStream("datawave/data/normalizer/geoRanges.txt"), "UTF8");
        
        assertEquals(expected, result.toString());
    }
    
    @Test
    public void testHash() {
        String[] validHashes = new String[] {"00", "0100", "020d", "031b", "04df", "05031e", "0604ff", "0713ff", "08c7fe", "09023fff", "0a04ffff", "0b0dffff",
                "0c8fffff", "0d01c00000", "0e0b000000", "0f0dfffffe", "1037ffffff", "11023fffffff", "1208ffffffff", "131c00000000", "1437ffffffff",
                "15023fffffffff", "16070000000000", "1723ffffffffff", "188fffffffffff", "19013fffffffffff", "1a08ffffffffffff", "1b1c000000000000",
                "1c4fffffffffffff", "1d01c0000000000000", "1e0700000000000000", "1f0dffffffffffffff"};
        String[] invalidHashes = new String[] {"0", "0001", "01", "1fffffffffffffffff", "200dffffffffffffff", "1c4fffffffffffffff"};
        for (String hash : validHashes) {
            assertEquals(hash, normalizer.normalize(hash));
        }
        for (String hash : invalidHashes) {
            try {
                normalizer.normalize(hash);
                fail("Should have failed to normalize " + hash);
            } catch (Exception e) {
                // this is expected
            }
        }
    }
}
