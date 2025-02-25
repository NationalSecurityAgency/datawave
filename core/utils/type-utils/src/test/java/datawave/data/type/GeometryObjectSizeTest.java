package datawave.data.type;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.io.WKTReader;

import datawave.data.type.util.Geometry;
import datawave.data.type.util.Point;
import datawave.webservice.query.data.ObjectSizeOf;

public class GeometryObjectSizeTest {
    
    private final static double THRESHOLD = 0.1;
    
    @Test
    public void pointTest() throws Exception {
        PointType pointType = new PointType();
        pointType.setDelegate(new Point((org.locationtech.jts.geom.Point) new WKTReader().read("POINT(0 0)")));
        
        long estimatedSizeInBytes = pointType.sizeInBytes();
        long actualSizeInBytes = sizeInBytes(pointType);
        
        assertTrue(Math.abs(actualSizeInBytes - estimatedSizeInBytes) / (double) actualSizeInBytes <= THRESHOLD);
    }
    
    @Test
    public void multiPointTest() throws Exception {
        GeometryType geometryType = new GeometryType();
        geometryType.setDelegate(new Geometry(new WKTReader().read("MULTIPOINT(0 0, 1 1, 2 2, 3 3, 4 4, 5 5, 6 6, 7 7, 8 8, 9 9, 10 10)")));
        
        long estimatedSizeInBytes = geometryType.sizeInBytes();
        long actualSizeInBytes = sizeInBytes(geometryType);
        
        assertTrue(Math.abs(actualSizeInBytes - estimatedSizeInBytes) / (double) actualSizeInBytes <= THRESHOLD);
    }
    
    @Test
    public void polygonTest() throws Exception {
        GeometryType geometryType = new GeometryType();
        geometryType.setDelegate(new Geometry(
                        new WKTReader().read("POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90), (-45 -45, 45 -45, 45 45, -45 45, -45 -45))")));
        
        long estimatedSizeInBytes = geometryType.sizeInBytes();
        long actualSizeInBytes = sizeInBytes(geometryType);
        
        assertTrue(Math.abs(actualSizeInBytes - estimatedSizeInBytes) / (double) actualSizeInBytes <= THRESHOLD);
    }
    
    @Test
    public void multiPolygonTest() throws Exception {
        GeometryType geometryType = new GeometryType();
        geometryType.setDelegate(new Geometry(new WKTReader().read(
                        "MULTIPOLYGON(((-180 -90, 180 -90, 180 90, -180 90, -180 -90), (-45 -45, 45 -45, 45 45, -45 45, -45 -45)), ((-60 -60, 60 -60, 60 60, -60 60, -60 -60)))")));
        
        long estimatedSizeInBytes = geometryType.sizeInBytes();
        long actualSizeInBytes = sizeInBytes(geometryType);
        
        assertTrue(Math.abs(actualSizeInBytes - estimatedSizeInBytes) / (double) actualSizeInBytes <= THRESHOLD);
    }
    
    @Test
    public void lineStringTest() throws Exception {
        GeometryType geometryType = new GeometryType();
        geometryType.setDelegate(new Geometry(new WKTReader().read("LINESTRING(-110 -80, -45 -76, -10 -5, 30 10, 40 50, 35 30, 170 85)")));
        
        long estimatedSizeInBytes = geometryType.sizeInBytes();
        long actualSizeInBytes = sizeInBytes(geometryType);
        
        assertTrue(Math.abs(actualSizeInBytes - estimatedSizeInBytes) / (double) actualSizeInBytes <= THRESHOLD);
    }
    
    @Test
    public void multiLineStringTest() throws Exception {
        GeometryType geometryType = new GeometryType();
        geometryType.setDelegate(new Geometry(new WKTReader().read(
                        "MULTILINESTRING((-110 -80, -45 -76, -10 -5, 30 10, 40 50, 35 30, 170 85), (0 1, 0 2, 0 3, 0 4, 0 5, 0 6, 0 7, 1 8, 1 9, 1 10))")));
        
        long estimatedSizeInBytes = geometryType.sizeInBytes();
        long actualSizeInBytes = sizeInBytes(geometryType);
        
        assertTrue(Math.abs(actualSizeInBytes - estimatedSizeInBytes) / (double) actualSizeInBytes <= THRESHOLD);
    }
    
    @Test
    public void geometryCollectionTest() throws Exception {
        GeometryType geometryType = new GeometryType();
        geometryType.setDelegate(new Geometry(new WKTReader().read(
                        "GEOMETRYCOLLECTION(POINT(0 0), MULTIPOINT(0 0, 1 1, 2 2, 3 3, 4 4, 5 5, 6 6, 7 7, 8 8, 9 9, 10 10), POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90), (-45 -45, 45 -45, 45 45, -45 45, -45 -45)), MULTIPOLYGON(((-180 -90, 180 -90, 180 90, -180 90, -180 -90), (-45 -45, 45 -45, 45 45, -45 45, -45 -45)), ((-60 -60, 60 -60, 60 60, -60 60, -60 -60))), LINESTRING(-110 -80, -45 -76, -10 -5, 30 10, 40 50, 35 30, 170 85), MULTILINESTRING((-110 -80, -45 -76, -10 -5, 30 10, 40 50, 35 30, 170 85), (0 1, 0 2, 0 3, 0 4, 0 5, 0 6, 0 7, 1 8, 1 9, 1 10)), GEOMETRYCOLLECTION(POINT(0 0), MULTIPOINT(0 0, 1 1, 2 2, 3 3, 4 4, 5 5, 6 6, 7 7, 8 8, 9 9, 10 10), POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90), (-45 -45, 45 -45, 45 45, -45 45, -45 -45)), MULTIPOLYGON(((-180 -90, 180 -90, 180 90, -180 90, -180 -90), (-45 -45, 45 -45, 45 45, -45 45, -45 -45)), ((-60 -60, 60 -60, 60 60, -60 60, -60 -60))), LINESTRING(-110 -80, -45 -76, -10 -5, 30 10, 40 50, 35 30, 170 85), MULTILINESTRING((-110 -80, -45 -76, -10 -5, 30 10, 40 50, 35 30, 170 85), (0 1, 0 2, 0 3, 0 4, 0 5, 0 6, 0 7, 1 8, 1 9, 1 10))))")));
        
        long estimatedSizeInBytes = geometryType.sizeInBytes();
        long actualSizeInBytes = sizeInBytes(geometryType);
        
        assertTrue(Math.abs(actualSizeInBytes - estimatedSizeInBytes) / (double) actualSizeInBytes <= THRESHOLD);
    }
    
    private static long sizeInBytes(BaseType baseType) {
        long size = 0;
        if (baseType instanceof OneToManyNormalizerType) {
            List<String> values = ((OneToManyNormalizerType<?>) baseType).getNormalizedValues();
            size += values.stream().map(String::length).map(length -> 2 * length + ObjectSizeOf.Sizer.REFERENCE).reduce(Integer::sum).orElse(0);
        }
        size += ObjectSizeOf.PrecomputedSizes.STRING_STATIC_REF + ObjectSizeOf.Sizer.REFERENCE + ObjectSizeOf.Sizer.REFERENCE
                        + (2 * baseType.getNormalizedValue().length()) + ObjectSizeOf.Sizer.getObjectSize(baseType.getDelegate());
        return size;
    }
}
