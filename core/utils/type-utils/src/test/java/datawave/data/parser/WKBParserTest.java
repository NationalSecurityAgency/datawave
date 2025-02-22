package datawave.data.parser;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;

import com.google.common.io.BaseEncoding;

public class WKBParserTest {
    
    @Test
    public void testParsePoint() throws Exception {
        Geometry geom = new WKTReader().read("POINT(10 20)");
        
        String base64EncodedWkb = BaseEncoding.base64().encode(new WKBWriter().write(geom));
        
        WKBParser wkbParser = new WKBParser();
        
        Geometry parsedGeom = wkbParser.parseGeometry(base64EncodedWkb);
        
        assertTrue(geom.equals(parsedGeom));
    }
    
    @Test
    public void testParseLine() throws Exception {
        Geometry geom = new WKTReader().read("LINESTRING (30 10, 10 30, 40 40)");
        
        String base64EncodedWkb = BaseEncoding.base64().encode(new WKBWriter().write(geom));
        
        WKBParser wkbParser = new WKBParser();
        
        Geometry parsedGeom = wkbParser.parseGeometry(base64EncodedWkb);
        
        assertTrue(geom.equals(parsedGeom));
    }
    
    @Test
    public void testParsePolygon() throws Exception {
        Geometry geom = new WKTReader().read("POLYGON((10 10, 20 20, 30 10, 10 10))");
        
        String base64EncodedWkb = BaseEncoding.base64().encode(new WKBWriter().write(geom));
        
        WKBParser wkbParser = new WKBParser();
        
        Geometry parsedGeom = wkbParser.parseGeometry(base64EncodedWkb);
        
        assertTrue(geom.equals(parsedGeom));
    }
}
