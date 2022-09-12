package datawave.query.jexl.visitors;

import datawave.data.MetadataCardinalityCounts;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.MockMetadataHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class PushdownLowSelectivityNodesVisitorTest {
    private MockMetadataHelper helper = null;
    private ShardQueryConfiguration config = null;
    
    @Before
    public void setup() throws Exception {
        helper = new MockMetadataHelper();
        Map<String,Map<String,MetadataCardinalityCounts>> selectivities = new HashMap<>();
        int count = 1;
        for (String field : Arrays.asList(new String[] {"FOO", "BAR", "FOOBAR", "BARFOO", "BARFOO2", "BARFOO3", "BARFOO4"})) {
            Map<String,MetadataCardinalityCounts> values = new HashMap<>();
            selectivities.put(field, values);
            for (String value : Arrays.asList(new String[] {"jsub", "ca1", "ca11", "ca2", "ca3", "ca4", "ca5"})) {
                long valueCount = count++;
                long fieldValueCount = count;
                long uniqueFieldValueCount = 7;
                long totalFieldValueCount = 1000;
                long totalUniqueFieldValueCount = 49;
                long totalUniqueFieldCount = 7;
                MetadataCardinalityCounts counts = new MetadataCardinalityCounts(field, value, valueCount, fieldValueCount, uniqueFieldValueCount,
                                totalFieldValueCount, totalUniqueFieldValueCount, totalUniqueFieldCount);
                values.put(value, counts);
            }
        }
        helper.setTermCounts(selectivities);
        config = new ShardQueryConfiguration();
        config.setMinSelectivity(0.002);
    }
    
    /**
     * check for pushdown in indexed equalities
     *
     * @throws Exception
     */
    @Test
    public void testDelayEquality1() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO == 'jsub' && (FOO == 'ca1' || UNINDEXED == 'ca1')");
        
        String result = JexlStringBuildingVisitor.buildQuery(PushdownLowSelectivityNodesVisitor.pushdownLowSelectiveTerms(script, config, helper));
        Assert.assertEquals("FOO == 'jsub' && (((_Delayed_ = true) && (FOO == 'ca1')) || UNINDEXED == 'ca1')", result);
    }
    
    /**
     * check for pushdown in indexed equalities
     *
     * @throws Exception
     */
    @Test
    public void testDelayEquality2() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO == 'jsub' && (FOO == 'ca11' || UNINDEXED == 'ca11')");
        
        String result = JexlStringBuildingVisitor.buildQuery(PushdownLowSelectivityNodesVisitor.pushdownLowSelectiveTerms(script, config, helper));
        Assert.assertEquals("FOO == 'jsub' && (((_Delayed_ = true) && (FOO == 'ca11')) || UNINDEXED == 'ca11')", result);
    }
    
    /**
     * check for pushdown in indexed equalities
     *
     * @throws Exception
     */
    @Test
    public void testDelayEquality3() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO == 'jsub' && (FOO == 'ca2' || UNINDEXED == 'ca2')");
        
        String result = JexlStringBuildingVisitor.buildQuery(PushdownLowSelectivityNodesVisitor.pushdownLowSelectiveTerms(script, config, helper));
        Assert.assertEquals("FOO == 'jsub' && (((_Delayed_ = true) && (FOO == 'ca2')) || UNINDEXED == 'ca2')", result);
    }
    
    /**
     * check for skipped pushdown in indexed equalities
     *
     * @throws Exception
     */
    @Test
    public void testDelayEquality4() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO == 'jsub' && (FOO == 'ca21' || UNINDEXED == 'ca21')");
        
        String result = JexlStringBuildingVisitor.buildQuery(PushdownLowSelectivityNodesVisitor.pushdownLowSelectiveTerms(script, config, helper));
        Assert.assertEquals("FOO == 'jsub' && (FOO == 'ca21' || UNINDEXED == 'ca21')", result);
    }
    
    /**
     * check for skipped pushdown in indexed equalities, outside of min selectivity range
     *
     * @throws Exception
     */
    @Test
    public void testDelayEquality5() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO == 'jsub' && (FOO == 'ca1' || UNINDEXED == 'ca1')");
        
        config.setMinSelectivity(0.01);
        
        String result = JexlStringBuildingVisitor.buildQuery(PushdownLowSelectivityNodesVisitor.pushdownLowSelectiveTerms(script, config, helper));
        Assert.assertEquals("FOO == 'jsub' && (FOO == 'ca1' || UNINDEXED == 'ca1')", result);
    }
    
    /**
     * check for pushdown in indexed equalities, mixed
     *
     * @throws Exception
     */
    @Test
    public void testDelayMultipleHoles() throws Exception {
        ASTJexlScript script = JexlASTHelper
                        .parseJexlQuery("FOO == 'jsub' && (FOO == 'ca1' || FOO == 'ca2' || FOO == 'ca3' || FOO == 'ca4' || UNINDEXED == 'ca1')");
        
        String result = JexlStringBuildingVisitor.buildQuery(PushdownLowSelectivityNodesVisitor.pushdownLowSelectiveTerms(script, config, helper));
        Assert.assertEquals(
                        "FOO == 'jsub' && (((_Delayed_ = true) && (FOO == 'ca1')) || ((_Delayed_ = true) && (FOO == 'ca2')) || ((_Delayed_ = true) && (FOO == 'ca3')) || ((_Delayed_ = true) && (FOO == 'ca4')) || UNINDEXED == 'ca1')",
                        result);
    }
    
}
