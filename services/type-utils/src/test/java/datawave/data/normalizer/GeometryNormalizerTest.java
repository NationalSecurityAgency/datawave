package datawave.data.normalizer;

import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.WKTWriter;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class GeometryNormalizerTest {
    
    private GeometryNormalizer normalizer = null;
    
    @Before
    public void setup() {
        normalizer = new GeometryNormalizer();
    }
    
    @Test
    public void testPoint() {
        Geometry point = new GeometryFactory().createPoint(new Coordinate(10, 10));
        List<String> insertionIds = new ArrayList<String>(normalizer.expand(new WKTWriter().write(point)));
        assertEquals(1, insertionIds.size());
        assertEquals("1f200a80a80a80a80a", insertionIds.get(0));
    }
    
    @Test
    public void testLine() {
        Geometry line = new GeometryFactory().createLineString(new Coordinate[] {new Coordinate(-10, -10), new Coordinate(0, 0), new Coordinate(10, 20)});
        List<String> insertionIds = new ArrayList<String>(normalizer.expand(new WKTWriter().write(line)));
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
        List<String> insertionIds = new ArrayList<String>(normalizer.expand(new WKTWriter().write(polygon)));
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
        
        List<String> insertionIds = new ArrayList<String>(normalizer.expand(new WKTWriter().write(geom)));
        assertEquals(1, insertionIds.size());
        assertEquals("1f20306ba4306ba430", insertionIds.get(0));
    }
    
    @Test
    public void testWKTPointz() {
        Geometry geom = AbstractGeometryNormalizer.parseGeometry("POINT Z(10 20 30)");
        assertEquals(10.0, geom.getGeometryN(0).getCoordinate().x, 0.0);
        assertEquals(20.0, geom.getGeometryN(0).getCoordinate().y, 0.0);
        assertEquals(30.0, geom.getGeometryN(0).getCoordinate().z, 0.0);
        
        List<String> insertionIds = new ArrayList<String>(normalizer.expand(new WKTWriter().write(geom)));
        assertEquals(1, insertionIds.size());
        assertEquals("1f20306ba4306ba430", insertionIds.get(0));
    }
    
    @Test
    public void testQueryRanges() throws Exception {
        Geometry polygon = new GeometryFactory().createPolygon(new Coordinate[] {new Coordinate(-10, -10), new Coordinate(10, -10), new Coordinate(10, 10),
                new Coordinate(-10, 10), new Coordinate(-10, -10)});
        
        List<ByteArrayRange> allRanges = new ArrayList<ByteArrayRange>();
        for (MultiDimensionalNumericData range : GeometryUtils.basicConstraintsFromEnvelope(polygon.getEnvelopeInternal())
                        .getIndexConstraints(GeometryNormalizer.indexStrategy)) {
            allRanges.addAll(Lists.reverse(GeometryNormalizer.indexStrategy.getQueryRanges(range)));
        }
        
        assertEquals(3746, allRanges.size());
        
        StringBuffer result = new StringBuffer();
        for (ByteArrayRange range : allRanges) {
            result.append(Hex.encodeHexString(range.getStart().getBytes()));
            result.append(Hex.encodeHexString(range.getEnd().getBytes()));
        }
        
        String expected = IOUtils.toString(this.getClass().getClassLoader().getResourceAsStream("datawave/data/normalizer/geoRanges.txt"), "UTF8");
        
        assertEquals(expected, result.toString());
    }
}
