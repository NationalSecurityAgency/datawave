package datawave.query.planner;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_OR;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ArrayListMultimap;

import datawave.data.type.LcNoDiacriticsType;
import datawave.microservice.query.QueryImpl;
import datawave.query.config.ScanHintRule;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.ExceededOr;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.planner.scanhints.ExecutorScanHintRule;
import datawave.query.planner.scanhints.IvaratorScanHint;
import datawave.query.planner.scanhints.PriorityScanHintRule;
import datawave.query.util.MetadataHelper;

public class ScanHintRulesTest extends EasyMockSupport {
    private MetadataHelper metadataHelper;
    private DefaultQueryPlanner planner;
    private ShardQueryConfiguration config;

    @Before
    public void setup() {
        metadataHelper = mock(MetadataHelper.class);
        planner = new DefaultQueryPlanner();
        config = new ShardQueryConfiguration();

        config.setDatatypeFilter(Collections.emptySet());
        config.setDisableWhindexFieldMappings(true);

        planner.setMetadataHelper(metadataHelper);
        planner.setDisableWhindexFieldMappings(true);
    }

    private void setupMetadata() throws TableNotFoundException, InstantiationException, IllegalAccessException {
        expect(metadataHelper.getIndexedFields(EasyMock.anyObject())).andReturn(Collections.emptySet()).anyTimes();
        expect(metadataHelper.getReverseIndexedFields(EasyMock.anyObject())).andReturn(Collections.emptySet()).anyTimes();
        expect(metadataHelper.getIndexOnlyFields(EasyMock.anyObject())).andReturn(Collections.emptySet()).anyTimes();
        expect(metadataHelper.getAllFields(EasyMock.anyObject())).andReturn(Collections.singleton("A")).anyTimes();
        expect(metadataHelper.getExpansionFields(EasyMock.anyObject())).andReturn(Collections.emptySet()).anyTimes();
        expect(metadataHelper.getAllDatatypes()).andReturn(Collections.singleton(new LcNoDiacriticsType())).anyTimes();
        expect(metadataHelper.getDatatypesForField("A", Collections.emptySet())).andReturn(Collections.singleton(new LcNoDiacriticsType())).anyTimes();
        expect(metadataHelper.getAllNormalized()).andReturn(Collections.singleton("A")).anyTimes();
        expect(metadataHelper.getNonEventFields(EasyMock.anyObject())).andReturn(Collections.emptySet()).anyTimes();
        expect(metadataHelper.getCompositeToFieldMap(EasyMock.anyObject())).andReturn(ArrayListMultimap.create()).anyTimes();
        expect(metadataHelper.getCompositeTransitionDateMap(EasyMock.anyObject())).andReturn(Collections.emptyMap()).anyTimes();
        expect(metadataHelper.getCompositeFieldSeparatorMap(EasyMock.anyObject())).andReturn(Collections.emptyMap()).anyTimes();
        expect(metadataHelper.getTermFrequencyFields(EasyMock.anyObject())).andReturn(Collections.emptySet()).anyTimes();
    }

    @Test
    public void testDisabled() throws ParseException, DatawaveQueryException, TableNotFoundException, InstantiationException, IllegalAccessException,
                    JsonProcessingException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("A == 'pie'");
        // replace A == 'pie' with an ivarator
        SortedSet<String> foods = new TreeSet<>();
        foods.add("bacon");
        foods.add("eggs");
        foods.add("waffles");
        JexlNode child = QueryPropertyMarker.create(new ExceededOr("A", foods).getJexlNode(), EXCEEDED_OR);
        child.jjtSetParent(query);
        query.jjtGetChild(0).jjtSetParent(null);
        query.jjtAddChild(child, 0);

        config.setUseQueryTreeScanHintRules(false);
        config.setQueryTreeScanHintRules(Collections.singletonList(new IvaratorScanHint()));
        config.setQueryTree(query);

        setupMetadata();

        replayAll();

        planner.updateQueryTree(null, metadataHelper, null, config, JexlStringBuildingVisitor.buildQuery(query), new QueryImpl());

        verifyAll();

        assertEquals(Collections.emptyMap(), config.getTableHints());
    }

    @Test
    public void enabledEmptyHintsTest() throws TableNotFoundException, InstantiationException, IllegalAccessException, JsonProcessingException, ParseException,
                    DatawaveQueryException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("A == 'pie'");
        // replace A == 'pie' with an ivarator
        SortedSet<String> foods = new TreeSet<>();
        foods.add("bacon");
        foods.add("eggs");
        foods.add("waffles");
        JexlNode child = QueryPropertyMarker.create(new ExceededOr("A", foods).getJexlNode(), EXCEEDED_OR);
        child.jjtSetParent(query);

        query.jjtGetChild(0).jjtSetParent(null);
        query.jjtAddChild(child, 0);

        config.setUseQueryTreeScanHintRules(true);
        config.setQueryTreeScanHintRules(Collections.singletonList(new IvaratorScanHint()));
        config.setQueryTree(query);

        setupMetadata();

        replayAll();

        planner.updateQueryTree(null, metadataHelper, null, config, JexlStringBuildingVisitor.buildQuery(query), new QueryImpl());

        verifyAll();

        assertEquals(Collections.singleton("shard"), config.getTableHints().keySet());
        assertEquals(Collections.singleton("scan_type"), config.getTableHints().get("shard").keySet());
        assertEquals("ivarator", config.getTableHints().get("shard").get("scan_type"));
    }

    @Test
    public void overwriteScanHintTest() throws ParseException, JsonProcessingException, TableNotFoundException, InstantiationException, IllegalAccessException,
                    DatawaveQueryException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("A == 'pie'");
        // replace A == 'pie' with an ivarator
        SortedSet<String> foods = new TreeSet<>();
        foods.add("bacon");
        foods.add("eggs");
        foods.add("waffles");
        JexlNode child = QueryPropertyMarker.create(new ExceededOr("A", foods).getJexlNode(), EXCEEDED_OR);
        child.jjtSetParent(query);

        query.jjtGetChild(0).jjtSetParent(null);
        query.jjtAddChild(child, 0);

        config.setUseQueryTreeScanHintRules(true);
        config.setQueryTreeScanHintRules(Collections.singletonList(new IvaratorScanHint()));
        config.setQueryTree(query);

        Map<String,Map<String,String>> hints = new HashMap();
        Map<String,String> shardMap = new HashMap<>();
        hints.put("shard", shardMap);
        shardMap.put("scan_type", "default");

        config.setTableHints(hints);

        setupMetadata();

        replayAll();

        planner.updateQueryTree(null, metadataHelper, null, config, JexlStringBuildingVisitor.buildQuery(query), new QueryImpl());

        verifyAll();

        assertEquals(Collections.singleton("shard"), config.getTableHints().keySet());
        assertEquals(Collections.singleton("scan_type"), config.getTableHints().get("shard").keySet());
        assertEquals("ivarator", config.getTableHints().get("shard").get("scan_type"));
    }

    @Test
    public void unchainableTest() throws ParseException, JsonProcessingException, TableNotFoundException, InstantiationException, IllegalAccessException,
                    DatawaveQueryException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("A == 'pie'");
        // replace A == 'pie' with an ivarator
        SortedSet<String> foods = new TreeSet<>();
        foods.add("bacon");
        foods.add("eggs");
        foods.add("waffles");
        JexlNode child = QueryPropertyMarker.create(new ExceededOr("A", foods).getJexlNode(), EXCEEDED_OR);
        child.jjtSetParent(query);

        query.jjtGetChild(0).jjtSetParent(null);
        query.jjtAddChild(child, 0);

        config.setUseQueryTreeScanHintRules(true);
        List<ScanHintRule<JexlNode>> scanHintRules = new ArrayList();
        scanHintRules.add(new UnChainableScanHintRule());
        scanHintRules.add(new IvaratorScanHint());

        config.setQueryTreeScanHintRules(scanHintRules);
        config.setQueryTree(query);

        Map<String,Map<String,String>> hints = new HashMap();
        Map<String,String> shardMap = new HashMap<>();
        hints.put("shard", shardMap);
        shardMap.put("scan_type", "default");

        config.setTableHints(hints);

        setupMetadata();

        replayAll();

        planner.updateQueryTree(null, metadataHelper, null, config, JexlStringBuildingVisitor.buildQuery(query), new QueryImpl());

        verifyAll();

        assertEquals(Collections.singleton("shard"), config.getTableHints().keySet());
        assertEquals(Collections.singleton("scan_type"), config.getTableHints().get("shard").keySet());
        assertEquals("untouchable", config.getTableHints().get("shard").get("scan_type"));
    }

    @Test
    public void chainableTest() throws ParseException, JsonProcessingException, TableNotFoundException, InstantiationException, IllegalAccessException,
                    DatawaveQueryException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("A == 'pie'");
        // replace A == 'pie' with an ivarator
        SortedSet<String> foods = new TreeSet<>();
        foods.add("bacon");
        foods.add("eggs");
        foods.add("waffles");
        JexlNode child = QueryPropertyMarker.create(new ExceededOr("A", foods).getJexlNode(), EXCEEDED_OR);
        child.jjtSetParent(query);

        query.jjtGetChild(0).jjtSetParent(null);
        query.jjtAddChild(child, 0);

        config.setUseQueryTreeScanHintRules(true);
        List<ScanHintRule<JexlNode>> scanHintRules = new ArrayList();
        scanHintRules.add(new ChinableScanHintRule());
        scanHintRules.add(new IvaratorScanHint());

        config.setQueryTreeScanHintRules(scanHintRules);
        config.setQueryTree(query);

        Map<String,Map<String,String>> hints = new HashMap();
        Map<String,String> shardMap = new HashMap<>();
        hints.put("shard", shardMap);
        shardMap.put("scan_type", "default");

        config.setTableHints(hints);

        setupMetadata();

        replayAll();

        planner.updateQueryTree(null, metadataHelper, null, config, JexlStringBuildingVisitor.buildQuery(query), new QueryImpl());

        verifyAll();

        assertEquals(Collections.singleton("shard"), config.getTableHints().keySet());
        assertEquals(2, config.getTableHints().get("shard").keySet().size());
        assertEquals("ivarator", config.getTableHints().get("shard").get("scan_type"));
        assertEquals("1", config.getTableHints().get("shard").get("priority"));
    }

    private class ChinableScanHintRule extends PriorityScanHintRule {
        public ChinableScanHintRule() {
            super();
            setHintValue("1");
        }

        @Override
        public boolean isChainable() {
            return true;
        }

        @Override
        public Boolean apply(JexlNode jexlNode) {
            return true;
        }
    }

    private class UnChainableScanHintRule extends ExecutorScanHintRule {
        public UnChainableScanHintRule() {
            super();
            setHintValue("untouchable");
        }

        @Override
        public boolean isChainable() {
            return false;
        }

        @Override
        public Boolean apply(JexlNode jexlNode) {
            // always untouchable
            return true;
        }

        @Override
        public String toString() {
            return "You shall not pass rule";
        }
    }
}
