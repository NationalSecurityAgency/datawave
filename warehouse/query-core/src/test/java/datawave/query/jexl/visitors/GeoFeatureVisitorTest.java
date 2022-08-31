package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import datawave.webservice.query.map.QueryGeometry;
import org.apache.commons.jexl2.parser.JexlNode;
import org.junit.Test;

import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GeoFeatureVisitorTest {
    private static final String GEO_FIELD_QUERY = "(((GEO_FIELD >= '0208' && GEO_FIELD <= '020d') || GEO_FIELD == '0202') && (geowave:intersects(GEO_FIELD, 'POLYGON((10 10, -10 10, -10 -10, 10 -10, 10 10 ))')))";
    private static final String GEOM_FIELD_QUERY = "(((GEOM_FIELD > '0500aa' && GEOM_FIELD < '0501ff') || GEOM_FIELD == '050355') && (geowave:intersects(GEOM_FIELD, 'POLYGON((-90 -90, 90 -90, 90 90, -90 90, -90 -90))')))";
    private static final String POINT_FIELD_QUERY = "((POINT_FIELD == '1faaaaaaaaaaaaaaa') && (geowave:intersects(POINT_FIELD, 'POINT(0 0)')))";
    private static final String EQ_FIELD_QUERY = "(NAME == 'Tony Stark' || NAME == 'Iron Man')";
    private static final String ER_FIELD_QUERY = "(filter:includeRegex(NAME,'Thor.*?'))";
    private static final String ALL_GEO_FIELDS_QUERY = "((" + GEO_FIELD_QUERY + " || " + GEOM_FIELD_QUERY + ")" + " && " + POINT_FIELD_QUERY + ")";
    private static final String ALL_OTHER_FIELDS_QUERY = "(" + EQ_FIELD_QUERY + " || " + ER_FIELD_QUERY + ")";
    private static final String ALL_COMBINED_FIELDS_QUERY = "(((" + GEO_FIELD_QUERY + " || " + GEOM_FIELD_QUERY + ")" + " && " + POINT_FIELD_QUERY + ") || "
                    + "(" + EQ_FIELD_QUERY + " || " + ER_FIELD_QUERY + "))";
    
    @Test
    public void testGeoQueryOnly() throws Exception {
        Set<QueryGeometry> geoFeatures = new LinkedHashSet<>();
        
        JexlNode queryNode = JexlASTHelper.parseAndFlattenJexlQuery(GEO_FIELD_QUERY);
        geoFeatures.addAll(GeoFeatureVisitor.getGeoFeatures(queryNode));
        
        assertEquals(1, geoFeatures.size());
        assertTrue(geoFeatures.contains(new QueryGeometry("geowave:intersects(GEO_FIELD, 'POLYGON((10 10, -10 10, -10 -10, 10 -10, 10 10 ))')",
                        "{\"type\":\"Polygon\",\"coordinates\":[[[10,10],[-10,10],[-10,-10],[10,-10],[10,10]]]}")));
    }
    
    @Test
    public void testGeomQueryOnly() throws Exception {
        Set<QueryGeometry> geoFeatures = new LinkedHashSet<>();
        
        JexlNode queryNode = JexlASTHelper.parseAndFlattenJexlQuery(GEOM_FIELD_QUERY);
        geoFeatures.addAll(GeoFeatureVisitor.getGeoFeatures(queryNode));
        
        assertEquals(1, geoFeatures.size());
        assertTrue(geoFeatures.contains(new QueryGeometry("geowave:intersects(GEOM_FIELD, 'POLYGON((-90 -90, 90 -90, 90 90, -90 90, -90 -90))')",
                        "{\"type\":\"Polygon\",\"coordinates\":[[[-90,-90],[90,-90],[90,90],[-90,90],[-90,-90]]]}")));
    }
    
    @Test
    public void testPointQueryMixed() throws Exception {
        Set<QueryGeometry> geoFeatures = new LinkedHashSet<>();
        
        JexlNode queryNode = JexlASTHelper.parseAndFlattenJexlQuery(POINT_FIELD_QUERY);
        geoFeatures.addAll(GeoFeatureVisitor.getGeoFeatures(queryNode));
        
        assertEquals(1, geoFeatures.size());
        assertTrue(geoFeatures.contains(new QueryGeometry("geowave:intersects(POINT_FIELD, 'POINT(0 0)')", "{\"type\":\"Point\",\"coordinates\":[0.0,0.0]}")));
    }
    
    @Test
    public void testGeoQueriesOnlyMixed() throws Exception {
        Set<QueryGeometry> geoFeatures = new LinkedHashSet<>();
        
        JexlNode queryNode = JexlASTHelper.parseAndFlattenJexlQuery(ALL_GEO_FIELDS_QUERY);
        geoFeatures.addAll(GeoFeatureVisitor.getGeoFeatures(queryNode));
        
        assertEquals(3, geoFeatures.size());
        assertTrue(geoFeatures.contains(new QueryGeometry("geowave:intersects(GEO_FIELD, 'POLYGON((10 10, -10 10, -10 -10, 10 -10, 10 10 ))')",
                        "{\"type\":\"Polygon\",\"coordinates\":[[[10,10],[-10,10],[-10,-10],[10,-10],[10,10]]]}")));
        assertTrue(geoFeatures.contains(new QueryGeometry("geowave:intersects(GEOM_FIELD, 'POLYGON((-90 -90, 90 -90, 90 90, -90 90, -90 -90))')",
                        "{\"type\":\"Polygon\",\"coordinates\":[[[-90,-90],[90,-90],[90,90],[-90,90],[-90,-90]]]}")));
        assertTrue(geoFeatures.contains(new QueryGeometry("geowave:intersects(POINT_FIELD, 'POINT(0 0)')", "{\"type\":\"Point\",\"coordinates\":[0.0,0.0]}")));
    }
    
    @Test
    public void testNoGeoQueries() throws Exception {
        Set<QueryGeometry> geoFeatures = new LinkedHashSet<>();
        
        JexlNode queryNode = JexlASTHelper.parseAndFlattenJexlQuery(ALL_OTHER_FIELDS_QUERY);
        geoFeatures.addAll(GeoFeatureVisitor.getGeoFeatures(queryNode));
        
        assertEquals(0, geoFeatures.size());
    }
    
    @Test
    public void testMultipleMixedQueryTypes() throws Exception {
        Set<QueryGeometry> geoFeatures = new LinkedHashSet<>();
        
        JexlNode queryNode = JexlASTHelper.parseAndFlattenJexlQuery(ALL_COMBINED_FIELDS_QUERY);
        geoFeatures.addAll(GeoFeatureVisitor.getGeoFeatures(queryNode));
        
        assertEquals(3, geoFeatures.size());
        assertTrue(geoFeatures.contains(new QueryGeometry("geowave:intersects(GEO_FIELD, 'POLYGON((10 10, -10 10, -10 -10, 10 -10, 10 10 ))')",
                        "{\"type\":\"Polygon\",\"coordinates\":[[[10,10],[-10,10],[-10,-10],[10,-10],[10,10]]]}")));
        assertTrue(geoFeatures.contains(new QueryGeometry("geowave:intersects(GEOM_FIELD, 'POLYGON((-90 -90, 90 -90, 90 90, -90 90, -90 -90))')",
                        "{\"type\":\"Polygon\",\"coordinates\":[[[-90,-90],[90,-90],[90,90],[-90,90],[-90,-90]]]}")));
        assertTrue(geoFeatures.contains(new QueryGeometry("geowave:intersects(POINT_FIELD, 'POINT(0 0)')", "{\"type\":\"Point\",\"coordinates\":[0.0,0.0]}")));
    }
}
