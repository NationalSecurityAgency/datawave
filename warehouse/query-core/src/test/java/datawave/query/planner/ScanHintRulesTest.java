package datawave.query.planner;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_OR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.verifyNoMoreInteractions;

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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

public class ScanHintRulesTest {
    private MetadataHelper metadataHelper;
    private DefaultQueryPlanner planner;
    private ShardQueryConfiguration config;

    @BeforeEach
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
        lenient().doReturn(Collections.emptySet()).when(metadataHelper).getIndexedFields(any());
        lenient().doReturn(Collections.emptySet()).when(metadataHelper).getHiddenFields(any());
        lenient().doReturn(Collections.emptySet()).when(metadataHelper).getReverseIndexedFields(any());
        lenient().doReturn(Collections.emptySet()).when(metadataHelper).getIndexOnlyFields(any());
        lenient().doReturn(Collections.singleton("A")).when(metadataHelper).getAllFields(any());
        lenient().doReturn(Collections.emptySet()).when(metadataHelper).getExpansionFields(any());
        lenient().doReturn(Collections.singleton(new LcNoDiacriticsType())).when(metadataHelper).getAllDatatypes();
        lenient().doReturn(Collections.singleton(new LcNoDiacriticsType())).when(metadataHelper).getDatatypesForField("A", Collections.emptySet());
        lenient().doReturn(Collections.singleton("A")).when(metadataHelper).getAllNormalized();
        lenient().doReturn(Collections.emptySet()).when(metadataHelper).getNonEventFields(any());
        lenient().doReturn(ArrayListMultimap.create()).when(metadataHelper).getCompositeToFieldMap(any());
        lenient().doReturn(Collections.emptyMap()).when(metadataHelper).getCompositeTransitionDateMap(any());
        lenient().doReturn(Collections.emptyMap()).when(metadataHelper).getCompositeFieldSeparatorMap(any());
        lenient().doReturn(Collections.emptySet()).when(metadataHelper).getTermFrequencyFields(any());
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

        planner.updateQueryTree(null, metadataHelper, null, config, JexlStringBuildingVisitor.buildQuery(query), new QueryImpl());

        assertEquals(Collections.emptyMap(), config.getTableHints());

        if (metadataHelper != null && mockingDetails(metadataHelper).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(metadataHelper));
        }
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

        planner.updateQueryTree(null, metadataHelper, null, config, JexlStringBuildingVisitor.buildQuery(query), new QueryImpl());

        assertEquals(Collections.singleton("shard"), config.getTableHints().keySet());
        assertEquals(Collections.singleton("scan_type"), config.getTableHints().get("shard").keySet());
        assertEquals("ivarator", config.getTableHints().get("shard").get("scan_type"));

        if (metadataHelper != null && mockingDetails(metadataHelper).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(metadataHelper));
        }
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

        planner.updateQueryTree(null, metadataHelper, null, config, JexlStringBuildingVisitor.buildQuery(query), new QueryImpl());

        assertEquals(Collections.singleton("shard"), config.getTableHints().keySet());
        assertEquals(Collections.singleton("scan_type"), config.getTableHints().get("shard").keySet());
        assertEquals("ivarator", config.getTableHints().get("shard").get("scan_type"));

        if (metadataHelper != null && mockingDetails(metadataHelper).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(metadataHelper));
        }
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

        planner.updateQueryTree(null, metadataHelper, null, config, JexlStringBuildingVisitor.buildQuery(query), new QueryImpl());

        assertEquals(Collections.singleton("shard"), config.getTableHints().keySet());
        assertEquals(Collections.singleton("scan_type"), config.getTableHints().get("shard").keySet());
        assertEquals("untouchable", config.getTableHints().get("shard").get("scan_type"));

        if (metadataHelper != null && mockingDetails(metadataHelper).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(metadataHelper));
        }
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

        planner.updateQueryTree(null, metadataHelper, null, config, JexlStringBuildingVisitor.buildQuery(query), new QueryImpl());

        assertEquals(Collections.singleton("shard"), config.getTableHints().keySet());
        assertEquals(2, config.getTableHints().get("shard").keySet().size());
        assertEquals("ivarator", config.getTableHints().get("shard").get("scan_type"));
        assertEquals("1", config.getTableHints().get("shard").get("priority"));

        if (metadataHelper != null && mockingDetails(metadataHelper).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(metadataHelper));
        }
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
