package datawave.query.jexl.visitors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.data.type.NoOpType;
import datawave.data.type.Type;
import datawave.ingest.mapreduce.handler.dateindex.DateIndexUtil;
import datawave.query.config.IndexValueHole;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.MockMetadataHelper;

public class PushdownMissingIndexRangeNodesVisitorTest {
    private static final Logger log = Logger.getLogger(PushdownMissingIndexRangeNodesVisitorTest.class);

    private MockMetadataHelper helper = null;
    private ShardQueryConfiguration config = null;

    @Before
    public void setup() throws Exception {
        helper = new MockMetadataHelper();
        helper.addDataTypes(Arrays.asList(new String[] {"DATATYPE"}));
        helper.addFields(Arrays.asList(new String[] {"FOO", "BAR", "FOOBAR", "BARFOO", "BARFOO2", "BARFOO3", "BARFOO4", "UNINDEXED"}));
        helper.setIndexedFields(new HashSet<String>(Arrays.asList(new String[] {"FOO", "BAR", "FOOBAR", "BARFOO", "BARFOO2", "BARFOO3", "BARFOO4"})));
        config = new ShardQueryConfiguration();
        config.setDatatypeFilter(new HashSet<String>(Arrays.asList(new String[] {"DATATYPE"})));
        Multimap<String,Type<?>> indexedFieldsDatatypes = HashMultimap.create();
        indexedFieldsDatatypes.put("FOO", new NoOpType());
        indexedFieldsDatatypes.put("BAR", new NoOpType());
        indexedFieldsDatatypes.put("FOOBAR", new NoOpType());
        indexedFieldsDatatypes.put("BARFOO", new NoOpType());
        indexedFieldsDatatypes.put("BARFOO2", new NoOpType());
        indexedFieldsDatatypes.put("BARFOO3", new NoOpType());
        indexedFieldsDatatypes.put("BARFOO4", new NoOpType());
        config.setQueryFieldsDatatypes(indexedFieldsDatatypes);
        config.setDatatypeFilter(new HashSet<String>(Arrays.asList(new String[] {"DATATYPE"})));
        config.setBeginDate(DateIndexUtil.getBeginDate("20100101"));
        config.setEndDate(DateIndexUtil.getEndDate("20100101"));
    }

    @Test
    public void testDelayEquality1() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO == 'jsub' && (FOO == 'ca1' || UNINDEXED == 'ca1')");
        List<IndexValueHole> holes = new ArrayList<>();
        holes.add(new IndexValueHole(new String[] {"20100101", "20100102"}, new String[] {"ca1", "ca2"}));
        config.setIndexValueHoles(holes);

        String result = JexlStringBuildingVisitor.buildQuery(PushdownMissingIndexRangeNodesVisitor.pushdownPredicates(script, config, helper));
        Assert.assertEquals("FOO == 'jsub' && (((_Hole_ = true) && (FOO == 'ca1')) || UNINDEXED == 'ca1')", result);
    }

    @Test
    public void testDelayEquality2() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO == 'jsub' && (FOO == 'ca11' || UNINDEXED == 'ca11')");
        List<IndexValueHole> holes = new ArrayList<>();
        holes.add(new IndexValueHole(new String[] {"20100101", "20100102"}, new String[] {"ca1", "ca2"}));
        config.setIndexValueHoles(holes);

        String result = JexlStringBuildingVisitor.buildQuery(PushdownMissingIndexRangeNodesVisitor.pushdownPredicates(script, config, helper));
        Assert.assertEquals("FOO == 'jsub' && (((_Hole_ = true) && (FOO == 'ca11')) || UNINDEXED == 'ca11')", result);
    }

    @Test
    public void testDelayEquality3() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO == 'jsub' && (FOO == 'ca2' || UNINDEXED == 'ca2')");
        List<IndexValueHole> holes = new ArrayList<>();
        holes.add(new IndexValueHole(new String[] {"20100101", "20100102"}, new String[] {"ca1", "ca2"}));
        config.setIndexValueHoles(holes);

        String result = JexlStringBuildingVisitor.buildQuery(PushdownMissingIndexRangeNodesVisitor.pushdownPredicates(script, config, helper));
        Assert.assertEquals("FOO == 'jsub' && (((_Hole_ = true) && (FOO == 'ca2')) || UNINDEXED == 'ca2')", result);
    }

    @Test
    public void testDelayEquality4() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO == 'jsub' && (FOO == 'ca21' || UNINDEXED == 'ca21')");
        List<IndexValueHole> holes = new ArrayList<>();
        holes.add(new IndexValueHole(new String[] {"20100101", "20100102"}, new String[] {"ca1", "ca2"}));
        config.setIndexValueHoles(holes);

        String result = JexlStringBuildingVisitor.buildQuery(PushdownMissingIndexRangeNodesVisitor.pushdownPredicates(script, config, helper));
        Assert.assertEquals("FOO == 'jsub' && (FOO == 'ca21' || UNINDEXED == 'ca21')", result);
    }

    @Test
    public void testDelayEquality5() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO == 'jsub' && (FOO == 'ca1' || UNINDEXED == 'ca1')");
        List<IndexValueHole> holes = new ArrayList<>();
        holes.add(new IndexValueHole(new String[] {"20100102", "20100103"}, new String[] {"ca1", "ca2"}));
        config.setIndexValueHoles(holes);

        String result = JexlStringBuildingVisitor.buildQuery(PushdownMissingIndexRangeNodesVisitor.pushdownPredicates(script, config, helper));
        Assert.assertEquals("FOO == 'jsub' && (FOO == 'ca1' || UNINDEXED == 'ca1')", result);
    }

    @Test
    public void testDelayRegex1() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO == 'jsub' && (FOO =~ 'ca.*' || UNINDEXED =~ 'ca.*')");
        List<IndexValueHole> holes = new ArrayList<>();
        holes.add(new IndexValueHole(new String[] {"20100101", "20100102"}, new String[] {"ca1", "ca2"}));
        config.setIndexValueHoles(holes);

        String result = JexlStringBuildingVisitor.buildQuery(PushdownMissingIndexRangeNodesVisitor.pushdownPredicates(script, config, helper));
        Assert.assertEquals("FOO == 'jsub' && (((_Hole_ = true) && (FOO =~ 'ca.*')) || UNINDEXED =~ 'ca.*')", result);
    }

    @Test
    public void testDelayRegex2() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO == 'jsub' && (FOO =~ 'ca1.*' || UNINDEXED =~ 'ca1.*')");
        List<IndexValueHole> holes = new ArrayList<>();
        holes.add(new IndexValueHole(new String[] {"20100101", "20100102"}, new String[] {"ca1", "ca2"}));
        config.setIndexValueHoles(holes);

        String result = JexlStringBuildingVisitor.buildQuery(PushdownMissingIndexRangeNodesVisitor.pushdownPredicates(script, config, helper));
        Assert.assertEquals("FOO == 'jsub' && (((_Hole_ = true) && (FOO =~ 'ca1.*')) || UNINDEXED =~ 'ca1.*')", result);
    }

    @Test
    public void testDelayRegex3() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO == 'jsub' && (FOO =~ 'ca11.*' || UNINDEXED =~ 'ca11.*')");
        List<IndexValueHole> holes = new ArrayList<>();
        holes.add(new IndexValueHole(new String[] {"20100101", "20100102"}, new String[] {"ca1", "ca2"}));
        config.setIndexValueHoles(holes);

        String result = JexlStringBuildingVisitor.buildQuery(PushdownMissingIndexRangeNodesVisitor.pushdownPredicates(script, config, helper));
        Assert.assertEquals("FOO == 'jsub' && (((_Hole_ = true) && (FOO =~ 'ca11.*')) || UNINDEXED =~ 'ca11.*')", result);
    }

    @Test
    public void testDelayRegex4() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO == 'jsub' && (FOO =~ 'ca2.*' || UNINDEXED =~ 'ca2.*')");
        List<IndexValueHole> holes = new ArrayList<>();
        holes.add(new IndexValueHole(new String[] {"20100101", "20100102"}, new String[] {"ca1", "ca2"}));
        config.setIndexValueHoles(holes);

        String result = JexlStringBuildingVisitor.buildQuery(PushdownMissingIndexRangeNodesVisitor.pushdownPredicates(script, config, helper));
        Assert.assertEquals("FOO == 'jsub' && (((_Hole_ = true) && (FOO =~ 'ca2.*')) || UNINDEXED =~ 'ca2.*')", result);
    }

    @Test
    public void testDelayRegex5() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO == 'jsub' && (FOO =~ 'ca21.*' || UNINDEXED =~ 'ca21.*')");
        List<IndexValueHole> holes = new ArrayList<>();
        holes.add(new IndexValueHole(new String[] {"20100101", "20100102"}, new String[] {"ca1", "ca2"}));
        config.setIndexValueHoles(holes);

        String result = JexlStringBuildingVisitor.buildQuery(PushdownMissingIndexRangeNodesVisitor.pushdownPredicates(script, config, helper));
        Assert.assertEquals("FOO == 'jsub' && (FOO =~ 'ca21.*' || UNINDEXED =~ 'ca21.*')", result);
    }

    @Test
    public void testDelayRegex6() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO == 'jsub' && (FOO =~ 'ca.*' || UNINDEXED =~ 'ca.*')");
        List<IndexValueHole> holes = new ArrayList<>();
        holes.add(new IndexValueHole(new String[] {"20100101", "20100102"}, new String[] {"ca1", "ca2"}));
        config.setIndexValueHoles(holes);

        String result = JexlStringBuildingVisitor.buildQuery(PushdownMissingIndexRangeNodesVisitor.pushdownPredicates(script, config, helper));
        Assert.assertEquals("FOO == 'jsub' && (((_Hole_ = true) && (FOO =~ 'ca.*')) || UNINDEXED =~ 'ca.*')", result);
    }

    @Test
    public void testDelayRange1() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(
                        "FOO == 'jsub' && ((_Bounded_ = true) && (FOO >= 'ca' && FOO <= 'caz')) && ((_Bounded_ = true) && (UNINDEXED >= 'ca' && UNINDEXED <= 'caz'))");
        List<IndexValueHole> holes = new ArrayList<>();
        holes.add(new IndexValueHole(new String[] {"20100101", "20100102"}, new String[] {"ca1", "ca2"}));
        config.setIndexValueHoles(holes);

        String result = JexlStringBuildingVisitor.buildQuery(PushdownMissingIndexRangeNodesVisitor.pushdownPredicates(script, config, helper));
        Assert.assertEquals(
                        "FOO == 'jsub' && ((_Hole_ = true) && ((_Bounded_ = true) && (FOO >= 'ca' && FOO <= 'caz'))) && ((_Bounded_ = true) && (UNINDEXED >= 'ca' && UNINDEXED <= 'caz'))",
                        result);
    }

    @Test
    public void testDelayRange2() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(
                        "FOO == 'jsub' && ((_Bounded_ = true) && (FOO >= 'ca1' && FOO <= 'ca11')) && ((_Bounded_ = true) && (UNINDEXED >= 'ca1' && UNINDEXED <= 'ca11'))");
        List<IndexValueHole> holes = new ArrayList<>();
        holes.add(new IndexValueHole(new String[] {"20100101", "20100102"}, new String[] {"ca1", "ca2"}));
        config.setIndexValueHoles(holes);

        String result = JexlStringBuildingVisitor.buildQuery(PushdownMissingIndexRangeNodesVisitor.pushdownPredicates(script, config, helper));
        Assert.assertEquals(
                        "FOO == 'jsub' && ((_Hole_ = true) && ((_Bounded_ = true) && (FOO >= 'ca1' && FOO <= 'ca11'))) && ((_Bounded_ = true) && (UNINDEXED >= 'ca1' && UNINDEXED <= 'ca11'))",
                        result);
    }

    @Test
    public void testDelayRange3() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(
                        "FOO == 'jsub' && ((_Bounded_ = true) && (FOO >= 'ca11' && FOO <= 'ca111')) && ((_Bounded_ = true) && (UNINDEXED >= 'ca11' && UNINDEXED <= 'ca111'))");
        List<IndexValueHole> holes = new ArrayList<>();
        holes.add(new IndexValueHole(new String[] {"20100101", "20100102"}, new String[] {"ca1", "ca2"}));
        config.setIndexValueHoles(holes);

        String result = JexlStringBuildingVisitor.buildQuery(PushdownMissingIndexRangeNodesVisitor.pushdownPredicates(script, config, helper));
        Assert.assertEquals(
                        "FOO == 'jsub' && ((_Hole_ = true) && ((_Bounded_ = true) && (FOO >= 'ca11' && FOO <= 'ca111'))) && ((_Bounded_ = true) && (UNINDEXED >= 'ca11' && UNINDEXED <= 'ca111'))",
                        result);
    }

    @Test
    public void testDelayRange4() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(
                        "FOO == 'jsub' && ((_Bounded_ = true) && (FOO >= 'ca2' && FOO <= 'ca21')) && ((_Bounded_ = true) && (UNINDEXED >= 'ca2' && UNINDEXED <= 'ca21'))");
        List<IndexValueHole> holes = new ArrayList<>();
        holes.add(new IndexValueHole(new String[] {"20100101", "20100102"}, new String[] {"ca1", "ca2"}));
        config.setIndexValueHoles(holes);

        String result = JexlStringBuildingVisitor.buildQuery(PushdownMissingIndexRangeNodesVisitor.pushdownPredicates(script, config, helper));
        Assert.assertEquals(
                        "FOO == 'jsub' && ((_Hole_ = true) && ((_Bounded_ = true) && (FOO >= 'ca2' && FOO <= 'ca21'))) && ((_Bounded_ = true) && (UNINDEXED >= 'ca2' && UNINDEXED <= 'ca21'))",
                        result);
    }

    @Test
    public void testDelayRange5() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(
                        "FOO == 'jsub' && ((_Bounded_ = true) && (FOO >= 'ca21' && FOO <= 'ca211')) && ((_Bounded_ = true) && (UNINDEXED >= 'ca21' && UNINDEXED <= 'ca211'))");
        List<IndexValueHole> holes = new ArrayList<>();
        holes.add(new IndexValueHole(new String[] {"20100101", "20100102"}, new String[] {"ca1", "ca2"}));
        config.setIndexValueHoles(holes);

        String result = JexlStringBuildingVisitor.buildQuery(PushdownMissingIndexRangeNodesVisitor.pushdownPredicates(script, config, helper));
        Assert.assertEquals(
                        "FOO == 'jsub' && ((_Bounded_ = true) && (FOO >= 'ca21' && FOO <= 'ca211')) && ((_Bounded_ = true) && (UNINDEXED >= 'ca21' && UNINDEXED <= 'ca211'))",
                        result);
    }

    @Test
    public void testDelayRange6() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(
                        "FOO == 'jsub' && ((_Bounded_ = true) && (FOO >= 'ca' && FOO <= 'caz')) && ((_Bounded_ = true) && (UNINDEXED >= 'ca' && UNINDEXED <= 'caz'))");
        List<IndexValueHole> holes = new ArrayList<>();
        holes.add(new IndexValueHole(new String[] {"20100102", "20100103"}, new String[] {"ca1", "ca2"}));
        config.setIndexValueHoles(holes);

        String result = JexlStringBuildingVisitor.buildQuery(PushdownMissingIndexRangeNodesVisitor.pushdownPredicates(script, config, helper));
        Assert.assertEquals(
                        "FOO == 'jsub' && ((_Bounded_ = true) && (FOO >= 'ca' && FOO <= 'caz')) && ((_Bounded_ = true) && (UNINDEXED >= 'ca' && UNINDEXED <= 'caz'))",
                        result);
    }

    @Test
    public void testSkipMarkers() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO == 'jsub' && (((_Value_ = true) && (FOO =~ 'ca.*')) || UNINDEXED =~ 'ca.*')");
        List<IndexValueHole> holes = new ArrayList<>();
        holes.add(new IndexValueHole(new String[] {"20100101", "20100102"}, new String[] {"ca1", "ca2"}));
        config.setIndexValueHoles(holes);

        String result = JexlStringBuildingVisitor.buildQuery(PushdownMissingIndexRangeNodesVisitor.pushdownPredicates(script, config, helper));
        Assert.assertEquals("FOO == 'jsub' && (((_Value_ = true) && (FOO =~ 'ca.*')) || UNINDEXED =~ 'ca.*')", result);
    }

    @Test
    public void testDelayMultipleHoles() throws Exception {
        ASTJexlScript script = JexlASTHelper
                        .parseJexlQuery("FOO == 'jsub' && (FOO == 'ca1' || FOO == 'ca2' || FOO == 'ca3' || FOO == 'ca4' || UNINDEXED == 'ca1')");

        List<IndexValueHole> holes = new ArrayList<>();
        holes.add(new IndexValueHole(new String[] {"20100101", "20100102"}, new String[] {"ca21", "ca3"}));
        holes.add(new IndexValueHole(new String[] {"20100101", "20100102"}, new String[] {"dab", "dac"}));
        holes.add(new IndexValueHole(new String[] {"20100101", "20100102"}, new String[] {"ca1", "ca11"}));
        holes.add(new IndexValueHole(new String[] {"20100101", "20100102"}, new String[] {"aba", "abc"}));
        holes.add(new IndexValueHole(new String[] {"20100102", "20100103"}, new String[] {"ca2", "ca21"}));

        config.setIndexValueHoles(holes);
        String result = JexlStringBuildingVisitor.buildQuery(PushdownMissingIndexRangeNodesVisitor.pushdownPredicates(script, config, helper));
        Assert.assertEquals(
                        "FOO == 'jsub' && (((_Hole_ = true) && (FOO == 'ca1')) || FOO == 'ca2' || ((_Hole_ = true) && (FOO == 'ca3')) || FOO == 'ca4' || UNINDEXED == 'ca1')",
                        result);
    }

}
