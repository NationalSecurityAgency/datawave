package datawave.query.jexl.functions;

import datawave.data.normalizer.AbstractGeometryNormalizer;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.util.MockMetadataHelper;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.Envelope;
import org.powermock.reflect.Whitebox;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class GeoWaveFunctionsDescriptorTest {
    
    private static List<String> expandedWkt;
    
    @BeforeClass
    public static void readExpandedWkt() {
        ClassLoader classLoader = GeoWaveFunctionsDescriptor.class.getClassLoader();
        expandedWkt = new BufferedReader(new InputStreamReader(classLoader.getResourceAsStream("datawave/query/jexl/functions/expandedWkt.txt"))).lines()
                        .collect(Collectors.toList());
    }
    
    @Test
    public void testAntiMeridianSpanningMultipolygon() throws Exception {
        String query = "geowave:intersects(GEO_FIELD, 'MULTIPOLYGON(((160 60, 180 60, 180 70, 160 70, 160 60)), ((-180 70, -180 60, -175 60, -175 70, -180 70)))')";
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        
        // DEFAULT, maxEnvelopes = 4
        String defaultExpandedQuery = convertFunctionToIndexQuery(query, config);
        Assert.assertEquals(expandedWkt.get(0), defaultExpandedQuery);
        
        // maxEnvelopes = 1
        config.setGeoWaveMaxEnvelopes(1);
        String oneEnvelopeExpandedQuery = convertFunctionToIndexQuery(query, config);
        Assert.assertEquals(expandedWkt.get(1), oneEnvelopeExpandedQuery);
        
        // maxEnvelopes = 2
        config.setGeoWaveMaxEnvelopes(2);
        String twoEnvelopesExpandedQuery = convertFunctionToIndexQuery(query, config);
        Assert.assertEquals(expandedWkt.get(2), twoEnvelopesExpandedQuery);
        
        // Test the the default number of envelopes produces a different expanded query than the single envelope expansion
        Assert.assertNotEquals(defaultExpandedQuery, oneEnvelopeExpandedQuery);
        
        // Test that the default number of envelopes produces the same expanded query as the two envelope expansion
        Assert.assertEquals(defaultExpandedQuery, twoEnvelopesExpandedQuery);
    }
    
    @Test
    public void testMultipolygonIntersection() throws Exception {
        // @formatter:off
        String wkt = "MULTIPOLYGON(" +
                "((160 60, 180 60, 180 70, 160 70, 160 60)), " +      // GROUP 1
                "((-180 70, -180 60, -175 60, -175 70, -180 70)), " + // GROUP 2
                "((155 20, 165 20, 165 65, 155 65, 155 20)), " +      // GROUP 1
                "((-105 68, -80 68, -80 88, -105 88, -105 68)), " +   // GROUP 2
                "((100 20, 165 20, 165 40, 100 40, 100 20)), " +      // GROUP 1
                "((-180 68, -100 68, -100 70, -180 70, -180 68)), " + // GROUP 2
                "((-5 -5, 5 -5, 5 5, -5 5, -5 -5)))";                 // GROUP 3
        // @formatter:on
        
        List<Envelope> envelopes = (List<Envelope>) Whitebox.invokeMethod(GeoWaveFunctionsDescriptor.class, "getSeparateEnvelopes",
                        AbstractGeometryNormalizer.parseGeometry(wkt), 4);
        
        Assert.assertEquals(3, envelopes.size());
        
        List<Envelope> expectedEnvelopes = new ArrayList<>();
        expectedEnvelopes.add(new Envelope(100, 180, 20, 70));
        expectedEnvelopes.add(new Envelope(-180, -80, 60, 88));
        expectedEnvelopes.add(new Envelope(-5, 5, -5, 5));
        
        Iterator<Envelope> expectedIter = expectedEnvelopes.iterator();
        while (expectedIter.hasNext()) {
            boolean foundMatch = false;
            for (Envelope envelope : envelopes) {
                if (envelope.equals(expectedIter.next())) {
                    foundMatch = true;
                    expectedIter.remove();
                }
            }
            Assert.assertTrue(foundMatch);
        }
    }
    
    public static String convertFunctionToIndexQuery(String queryStr, ShardQueryConfiguration config) throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryStr);
        ASTFunctionNode func = find(script);
        JexlArgumentDescriptor desc = new GeoWaveFunctionsDescriptor().getArgumentDescriptor(func);
        MockMetadataHelper helper = new MockMetadataHelper();
        helper.addField("GEO_FIELD", "datawave.data.type.GeometryType");
        
        JexlNode indexQuery = desc.getIndexQuery(config, helper, null, null);
        return JexlStringBuildingVisitor.buildQuery(indexQuery);
    }
    
    /**
     * Traverse the node graph to find the first function node in the iteration and returns it. Null is returned if no function node is found.
     *
     * @param root
     * @return
     */
    public static ASTFunctionNode find(JexlNode root) {
        if (root instanceof ASTFunctionNode) {
            return (ASTFunctionNode) root;
        } else {
            final int nChildren = root.jjtGetNumChildren();
            for (int child = 0; child < nChildren; ++child) {
                JexlNode subtreeSearch = find(root.jjtGetChild(child));
                if (subtreeSearch != null) {
                    return (ASTFunctionNode) subtreeSearch;
                }
            }
        }
        return null;
    }
}
