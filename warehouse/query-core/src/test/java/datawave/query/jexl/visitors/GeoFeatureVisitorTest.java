package datawave.query.jexl.visitors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.commons.jexl3.parser.JexlNode;
import org.junit.Test;

import datawave.query.jexl.JexlASTHelper;
import datawave.webservice.query.map.QueryGeometry;

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
    private static final String GEO_WITHIN_BOUNDING_BOX_QUERY = "geo:within_bounding_box(GEO_FIELD, '40_170', '50_-170')";
    private static final String GEO_WITHIN_CIRCLE_QUERY = "geo:within_circle(FIELD_1 || FIELD_2, '0_0', '10')";
    private static final String LAT_LON_GEO_QUERY = "geo:within_bounding_box(LON_FIELD, LAT_FIELD, 0, 0, 10, 10)";

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

    @Test
    public void testGeoWithinBoundingBox() throws Exception {
        Set<QueryGeometry> geoFeatures = new LinkedHashSet<>();

        JexlNode queryNode = JexlASTHelper.parseAndFlattenJexlQuery(GEO_WITHIN_BOUNDING_BOX_QUERY);
        geoFeatures.addAll(GeoFeatureVisitor.getGeoFeatures(queryNode));

        assertEquals(1, geoFeatures.size());
        assertTrue(geoFeatures.contains(new QueryGeometry("geo:within_bounding_box(GEO_FIELD, '40_170', '50_-170')",
                        "{\"type\":\"GeometryCollection\",\"geometries\":[{\"type\":\"Polygon\",\"coordinates\":[[[170,40],[180,40],[180,50],[170,50],[170,40]]]},{\"type\":\"Polygon\",\"coordinates\":[[[-180,40],[-170,40],[-170,50],[-180,50],[-180,40]]]}]}")));
    }

    @Test
    public void testGeoWithinCircle() throws Exception {
        Set<QueryGeometry> geoFeatures = new LinkedHashSet<>();

        JexlNode queryNode = JexlASTHelper.parseAndFlattenJexlQuery(GEO_WITHIN_CIRCLE_QUERY);
        geoFeatures.addAll(GeoFeatureVisitor.getGeoFeatures(queryNode));

        assertEquals(1, geoFeatures.size());
        assertTrue(geoFeatures.contains(new QueryGeometry("geo:within_circle((FIELD_1 || FIELD_2), '0_0', '10')",
                        "{\"type\":\"Polygon\",\"coordinates\":[[[10,0.0],[9.9452,1.0453],[9.7815,2.0791],[9.5106,3.0902],[9.1355,4.0674],[8.6603,5],[8.0902,5.8779],[7.4314,6.6913],[6.6913,7.4314],[5.8779,8.0902],[5,8.6603],[4.0674,9.1355],[3.0902,9.5106],[2.0791,9.7815],[1.0453,9.9452],[6.0E-16,10],[-1.0453,9.9452],[-2.0791,9.7815],[-3.0902,9.5106],[-4.0674,9.1355],[-5,8.6603],[-5.8779,8.0902],[-6.6913,7.4314],[-7.4314,6.6913],[-8.0902,5.8779],[-8.6603,5],[-9.1355,4.0674],[-9.5106,3.0902],[-9.7815,2.0791],[-9.9452,1.0453],[-10,1.2E-15],[-9.9452,-1.0453],[-9.7815,-2.0791],[-9.5106,-3.0902],[-9.1355,-4.0674],[-8.6603,-5],[-8.0902,-5.8779],[-7.4314,-6.6913],[-6.6913,-7.4314],[-5.8779,-8.0902],[-5,-8.6603],[-4.0674,-9.1355],[-3.0902,-9.5106],[-2.0791,-9.7815],[-1.0453,-9.9452],[-1.8E-15,-10],[1.0453,-9.9452],[2.0791,-9.7815],[3.0902,-9.5106],[4.0674,-9.1355],[5,-8.6603],[5.8779,-8.0902],[6.6913,-7.4314],[7.4314,-6.6913],[8.0902,-5.8779],[8.6603,-5],[9.1355,-4.0674],[9.5106,-3.0902],[9.7815,-2.0791],[9.9452,-1.0453],[10,0.0]]]}")));
    }

    @Test
    public void testSeparateLatLon() throws Exception {
        Set<QueryGeometry> geoFeatures = new LinkedHashSet<>();

        JexlNode queryNode = JexlASTHelper.parseAndFlattenJexlQuery(LAT_LON_GEO_QUERY);
        geoFeatures.addAll(GeoFeatureVisitor.getGeoFeatures(queryNode));

        assertEquals(1, geoFeatures.size());
        assertTrue(geoFeatures.contains(new QueryGeometry("geo:within_bounding_box(LON_FIELD, LAT_FIELD, 0, 0, 10, 10)",
                        "{\"type\":\"Polygon\",\"coordinates\":[[[0.0,0.0],[10,0.0],[10,10],[0.0,10],[0.0,0.0]]]}")));

    }
}
