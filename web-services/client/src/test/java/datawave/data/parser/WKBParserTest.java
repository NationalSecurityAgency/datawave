package datawave.data.parser;

import com.google.common.io.BaseEncoding;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.junit.Assert;
import org.junit.Test;

public class WKBParserTest {
    
    @Test
    public void testParsePoint() throws Exception {
        Geometry geom = new WKTReader().read("POINT(10 20)");
        
        String base64EncodedWkb = BaseEncoding.base64().encode(new WKBWriter().write(geom));
        
        WKBParser wkbParser = new WKBParser();
        
        Geometry parsedGeom = wkbParser.parseGeometry(base64EncodedWkb);
        
        Assert.assertTrue(geom.equals(parsedGeom));
    }
    
    @Test
    public void testParseLine() throws Exception {
        Geometry geom = new WKTReader().read("LINESTRING (30 10, 10 30, 40 40)");
        
        String base64EncodedWkb = BaseEncoding.base64().encode(new WKBWriter().write(geom));
        
        WKBParser wkbParser = new WKBParser();
        
        Geometry parsedGeom = wkbParser.parseGeometry(base64EncodedWkb);
        
        Assert.assertTrue(geom.equals(parsedGeom));
    }
    
    @Test
    public void testParsePolygon() throws Exception {
        Geometry geom = new WKTReader().read("POLYGON((10 10, 20 20, 30 10, 10 10))");
        
        String base64EncodedWkb = BaseEncoding.base64().encode(new WKBWriter().write(geom));
        
        WKBParser wkbParser = new WKBParser();
        
        Geometry parsedGeom = wkbParser.parseGeometry(base64EncodedWkb);
        
        Assert.assertTrue(geom.equals(parsedGeom));
    }
}
