package datawave.query.jexl.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.Envelope;

import datawave.data.normalizer.AbstractGeometryNormalizer;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.LiteralRange;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MockMetadataHelper;

public class GeoWaveFunctionsDescriptorTest {

    private static List<String> expandedWkt;

    @BeforeAll
    public static void readExpandedWkt() {
        ClassLoader classLoader = GeoWaveFunctionsDescriptor.class.getClassLoader();

        try (InputStream is = classLoader.getResourceAsStream("datawave/query/jexl/functions/expandedWkt.txt")) {
            assertNotNull(is);
            expandedWkt = new BufferedReader(new InputStreamReader(is)).lines().collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testAntiMeridianSpanningMultipolygon() throws Exception {
        String query = "geowave:intersects(GEO_FIELD, 'MULTIPOLYGON(((160 60, 180 60, 180 70, 160 70, 160 60)), ((-180 70, -180 60, -175 60, -175 70, -180 70)))')";

        ShardQueryConfiguration config = new ShardQueryConfiguration();

        MockMetadataHelper helper = new MockMetadataHelper();
        helper.addField("GEO_FIELD", "datawave.data.type.GeometryType");

        // DEFAULT, maxEnvelopes = 4
        String defaultExpandedQuery = convertFunctionToIndexQuery(query, config, helper);
        assertEquals(expandedWkt.get(0), defaultExpandedQuery);

        // maxEnvelopes = 1
        config.setGeoWaveMaxEnvelopes(1);
        String oneEnvelopeExpandedQuery = convertFunctionToIndexQuery(query, config, helper);
        assertEquals(expandedWkt.get(1), oneEnvelopeExpandedQuery);

        // maxEnvelopes = 2
        config.setGeoWaveMaxEnvelopes(2);
        String twoEnvelopesExpandedQuery = convertFunctionToIndexQuery(query, config, helper);
        assertEquals(expandedWkt.get(2), twoEnvelopesExpandedQuery);

        // Test the default number of envelopes produces a different expanded query than the single envelope expansion
        assertNotEquals(defaultExpandedQuery, oneEnvelopeExpandedQuery);

        // Test that the default number of envelopes produces the same expanded query as the two envelope expansion
        assertEquals(defaultExpandedQuery, twoEnvelopesExpandedQuery);
    }

    @Test
    public void testOptimizedRangesCanBuildAccumuloRanges() throws Exception {
        String query = "geowave:intersects(GEO_FIELD, 'POLYGON((0 12.34, 0.01 12.34, 0.01 22.34, 0 22.34, 0 12.34))')";
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        MockMetadataHelper helper = new MockMetadataHelper();
        helper.addField("GEO_FIELD", "datawave.data.type.GeometryType");
        JexlArgumentDescriptor descriptor = new GeoWaveFunctionsDescriptor().getArgumentDescriptor(find(JexlASTHelper.parseJexlQuery(query)));

        JexlNode indexQuery = descriptor.getIndexQuery(config, helper, null, null);

        assertTrue(buildAccumuloRanges(indexQuery) > 0);
    }

    @ParameterizedTest
    @ValueSource(strings = {"datawave.data.type.GeometryType", "datawave.data.type.PointType"})
    public void testThinPolygonUsesConfiguredSplitLimit(String fieldType) throws Exception {
        String query = "geowave:intersects(GEO_FIELD, 'POLYGON((0 12.34, 0.00000000001 12.34, 0.00000000001 22.34, 0 22.34, 0 12.34))')";
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setGeometryMaxExpansion(2);
        config.setPointMaxExpansion(3);
        MockMetadataHelper helper = new MockMetadataHelper();
        helper.addField("GEO_FIELD", fieldType);
        JexlArgumentDescriptor descriptor = new GeoWaveFunctionsDescriptor().getArgumentDescriptor(find(JexlASTHelper.parseJexlQuery(query)));

        JexlNode indexQuery = descriptor.getIndexQuery(config, helper, null, null);

        int rangeCount = buildAccumuloRanges(indexQuery);
        assertTrue(rangeCount > 0);
        // Pruning examines fewer than 4 * threshold + 1 cells per input range at each tier.
        int maxExpansion = fieldType.endsWith("GeometryType") ? config.getGeometryMaxExpansion() : config.getPointMaxExpansion();
        assertTrue(rangeCount <= 32 * maxExpansion * (4 * config.getGeoWaveRangeSplitThreshold() + 1) * maxExpansion);
    }

    private int buildAccumuloRanges(JexlNode node) {
        LiteralRange<?> range = JexlASTHelper.findRange().getRange(node);
        if (range != null) {
            new Range(range.getLower().toString(), range.getUpper().toString());
            return 1;
        }
        int count = 0;
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            count += buildAccumuloRanges(node.jjtGetChild(i));
        }
        return count;
    }

    @Test
    public void testMultipolygonIntersection() {
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

        List<Envelope> envelopes = GeoWaveFunctionsDescriptor.getSeparateEnvelopes(AbstractGeometryNormalizer.parseGeometry(wkt), 4);
        assertEquals(3, envelopes.size());

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
            assertTrue(foundMatch);
        }
    }

    public static String convertFunctionToIndexQuery(String queryStr, ShardQueryConfiguration config, MetadataHelper metadataHelper) throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryStr);
        ASTFunctionNode func = find(script);
        JexlArgumentDescriptor desc = new GeoWaveFunctionsDescriptor().getArgumentDescriptor(func);

        JexlNode indexQuery = desc.getIndexQuery(config, metadataHelper, null, null);
        String result = JexlStringBuildingVisitor.buildQuery(indexQuery);
        // a union is wrapped with reference expressions when building the query. Because the index query is merged
        // into a union the reference expressions need to be removed to ensure a flattened tree
        return result.substring(1, result.length() - 1);
    }

    /**
     * Traverse the node graph to find the first function node in the iteration and returns it. Null is returned if no function node is found.
     *
     * @param root
     *            root node
     * @return an AST function node
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
