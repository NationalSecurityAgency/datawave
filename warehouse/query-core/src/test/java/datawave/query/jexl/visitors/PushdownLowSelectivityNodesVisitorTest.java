package datawave.query.jexl.visitors;

import datawave.data.MetadataCardinalityCounts;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.MockMetadataHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class PushdownLowSelectivityNodesVisitorTest {
    
    private ShardQueryConfiguration config;
    private MockMetadataHelper helper;
    
    @BeforeEach
    public void beforeEach() {
        config = new ShardQueryConfiguration();
        config.setMinSelectivity(0.002);
        
        helper = new MockMetadataHelper();
        helper.setTermCounts(buildTermCounts());
    }
    
    private Map<String,Map<String,MetadataCardinalityCounts>> buildTermCounts() {
        
        List<String> fields = Collections.singletonList("FOO");
        List<String> values = Arrays.asList("a", "b", "c");
        
        Map<String,Map<String,MetadataCardinalityCounts>> counts = new HashMap<>();
        int count = 1;
        for (String field : fields) {
            Map<String,MetadataCardinalityCounts> map = new HashMap<>();
            counts.put(field, map);
            for (String value : values) {
                long valueCount = count++;
                long fieldValueCount = count;
                long uniqueFieldValueCount = fields.size();
                long totalFieldValueCount = 1000;
                long totalUniqueFieldValueCount = 49;
                long totalUniqueFieldCount = 7;
                MetadataCardinalityCounts cardinality = new MetadataCardinalityCounts(field, value, valueCount, fieldValueCount, uniqueFieldValueCount,
                                totalFieldValueCount, totalUniqueFieldValueCount, totalUniqueFieldCount);
                map.put(value, cardinality);
            }
        }
        return counts;
    }
    
    @Test
    public void testPushdownLeftTermInNestedUnion() {
        String query = "FOO == 'a' && (FOO == 'b' || UNINDEXED == 'c')";
        String expected = "FOO == 'a' && (((_Delayed_ = true) && (FOO == 'b')) || UNINDEXED == 'c')";
        test(query, expected);
    }
    
    @Test
    public void testPushdownRightTermInNestedUnion() {
        String query = "FOO == 'a' && (UNINDEXED == 'c' || FOO == 'b')";
        String expected = "FOO == 'a' && (UNINDEXED == 'c' || ((_Delayed_ = true) && (FOO == 'b')))";
        test(query, expected);
    }
    
    @Test
    public void testPushdownAnchorTerm() {
        String query = "FOO == 'b' && (FOO == 'a' || UNINDEXED == 'c')";
        String expected = "((_Delayed_ = true) && (FOO == 'b')) && (FOO == 'a' || UNINDEXED == 'c')";
        test(query, expected);
    }
    
    private void test(String query, String expected) {
        test(query, expected, config);
    }
    
    private void test(String query, String expected, ShardQueryConfiguration config) {
        try {
            ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
            ASTJexlScript pushed = PushdownLowSelectivityNodesVisitor.pushdownLowSelectiveTerms(script, config, helper);
            
            ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(expected);
            //  @formatter:off
            assertTrue(
                    TreeEqualityVisitor.isEqual(expectedScript, pushed), "Expected: " + expected + "\nBut was: " + JexlStringBuildingVisitor.buildQueryWithoutParse(pushed));
            //  @formatter:on
        } catch (Exception e) {
            fail("Error running test: " + e.getMessage());
        }
    }
}
