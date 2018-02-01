package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.GeoWaveQueryInfoVisitor.GeoWaveQueryInfo;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GeoWaveQueryInfoVisitorTest {
    
    private static final String GEO_FIELD_QUERY = "(((GEO_FIELD >= '0208' && GEO_FIELD <= '020d') || GEO_FIELD == '0202') && (geowave:intersects(GEO_FIELD, 'POLYGON((10 10, -10 10, -10 -10, 10 -10, 10 10 ))')))";
    private static final String GEOM_FIELD_QUERY = "(((GEOM_FIELD > '0500aa' && GEOM_FIELD < '0501ff') || GEOM_FIELD == '050355') && (geowave:intersects(GEOM_FIELD, 'POLYGON((-90 -90, 90 -90, 90 90, -90 90, -90 -90))')))";
    private static final String POINT_FIELD_QUERY = "((POINT_FIELD == '1faaaaaaaaaaaaaaa') && (geowave:intersects(POINT_FIELD, 'POINT(0 0)')))";
    private static final String OTHER_FIELD_QUERY = "(NAME == 'Tony Stark' || NAME == 'Iron Man')";
    private static final String COMBINED_FIELDS_QUERY = "(((" + GEO_FIELD_QUERY + " || " + GEOM_FIELD_QUERY + ")" + " && " + POINT_FIELD_QUERY + ") || "
                    + OTHER_FIELD_QUERY + ")";
    
    @Test
    public void testGeoField() throws Exception {
        ASTJexlScript queryTree = JexlASTHelper.parseJexlQuery(COMBINED_FIELDS_QUERY);
        
        GeoWaveQueryInfo queryInfo = new GeoWaveQueryInfoVisitor(Arrays.asList("GEO_FIELD")).parseGeoWaveQueryInfo(queryTree);
        assertEquals(2, queryInfo.getMinTier());
        assertEquals(2, queryInfo.getMaxTier());
    }
    
    @Test
    public void testGeomField() throws Exception {
        ASTJexlScript queryTree = JexlASTHelper.parseJexlQuery(COMBINED_FIELDS_QUERY);
        
        GeoWaveQueryInfo queryInfo = new GeoWaveQueryInfoVisitor(Arrays.asList("GEOM_FIELD")).parseGeoWaveQueryInfo(queryTree);
        assertEquals(5, queryInfo.getMinTier());
        assertEquals(5, queryInfo.getMaxTier());
    }
    
    @Test
    public void testPointField() throws Exception {
        ASTJexlScript queryTree = JexlASTHelper.parseJexlQuery(COMBINED_FIELDS_QUERY);
        
        GeoWaveQueryInfo queryInfo = new GeoWaveQueryInfoVisitor(Arrays.asList("POINT_FIELD")).parseGeoWaveQueryInfo(queryTree);
        assertEquals(31, queryInfo.getMinTier());
        assertEquals(31, queryInfo.getMaxTier());
    }
    
    @Test(expected = NumberFormatException.class)
    public void testOtherField() throws Exception {
        ASTJexlScript queryTree = JexlASTHelper.parseJexlQuery(COMBINED_FIELDS_QUERY);
        
        new GeoWaveQueryInfoVisitor(Arrays.asList("NAME")).parseGeoWaveQueryInfo(queryTree);
    }
    
    @Test
    public void testCombinedFields() throws Exception {
        ASTJexlScript queryTree = JexlASTHelper.parseJexlQuery(COMBINED_FIELDS_QUERY);
        
        GeoWaveQueryInfo queryInfo = new GeoWaveQueryInfoVisitor(Arrays.asList("GEO_FIELD", "GEOM_FIELD", "POINT_FIELD")).parseGeoWaveQueryInfo(queryTree);
        assertEquals(2, queryInfo.getMinTier());
        assertEquals(31, queryInfo.getMaxTier());
    }
    
    @Test
    public void testQueryInfo() {
        GeoWaveQueryInfo qi44 = new GeoWaveQueryInfo(4, 4);
        assertEquals(0, qi44.compareTo(qi44));
        
        GeoWaveQueryInfo qi45 = new GeoWaveQueryInfo(4, 5);
        assertTrue(qi44.compareTo(qi45) > 0);
        
        GeoWaveQueryInfo qi55 = new GeoWaveQueryInfo(5, 5);
        assertTrue(qi44.compareTo(qi55) > 0);
        assertTrue(qi45.compareTo(qi55) > 0);
    }
}
