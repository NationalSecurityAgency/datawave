package datawave.query.jexl.visitors;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NoOpType;
import datawave.data.type.Type;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.TreeEqualityVisitor.Reason;
import datawave.query.model.QueryModel;
import datawave.query.util.MockMetadataHelper;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class QueryModelVisitorTest {
    
    private static final Logger log = Logger.getLogger(QueryModelVisitor.class);
    
    private QueryModel model;
    private Set<String> allFields;
    
    @Before
    public void setupModel() {
        
        model = new QueryModel();
        
        model.addTermToModel("FOO", "BAR1");
        model.addTermToModel("FOO", "BAR2");
        model.addTermToModel("FIELD", "FIELD");
        model.addTermToModel("CYCLIC_FIELD", "CYCLIC_FIELD");
        model.addTermToModel("CYCLIC_FIELD", "CYCLIC_FIELD_");
        model.addTermToModel("OUT", "IN");
        model.addTermToModel("AG", "AGE");
        model.addTermToModel("AG", "ETA");
        model.addTermToModel("NAM", "NAME");
        model.addTermToModel("NAM", "NOME");
        model.addTermToModel("GEN", "GENDER");
        model.addTermToModel("GEN", "GENERE");
        
        model.addTermToReverseModel("BAR1", "FOO");
        model.addTermToReverseModel("BAR2", "FOO");
        model.addTermToReverseModel("FIELD", "FIELD");
        model.addTermToReverseModel("CYCLIC_FIELD", "CYCLIC_FIELD");
        model.addTermToReverseModel("CYCLIC_FIELD_", "CYCLIC_FIELD");
        model.addTermToReverseModel("IN", "OUT");
        model.addTermToReverseModel("AGE", "AG");
        model.addTermToReverseModel("ETA", "AG");
        model.addTermToReverseModel("NAME", "NAM");
        model.addTermToReverseModel("NOME", "NAM");
        model.addTermToReverseModel("GENDER", "GEN");
        model.addTermToReverseModel("GENERE", "GEN");
        
        allFields = new HashSet<>();
        allFields.add("FOO");
        allFields.add("9_2");
        allFields.add("BAR1");
        allFields.add("BAR2");
        allFields.add("FIELD");
        allFields.add("CYCLIC_FIELD");
        allFields.add("CYCLIC_FIELD_");
        allFields.add("OUT");
        allFields.add("IN");
        allFields.add("1BAR");
        allFields.add("2BAR");
        allFields.add("3BAR");
        allFields.add("1FOO");
        allFields.add("2FOO");
        allFields.add("3FOO");
        allFields.add("AG");
        allFields.add("AGE");
        allFields.add("ETA");
        allFields.add("NAM");
        allFields.add("NAME");
        allFields.add("NOME");
        allFields.add("GEN");
        allFields.add("GENDER");
        allFields.add("GENERE");
    }
    
    private void assertScriptEquality(ASTJexlScript expectedScript, ASTJexlScript actualScript) {
        Reason reason = new Reason();
        boolean equal = TreeEqualityVisitor.isEqual(expectedScript, actualScript, reason);
        if (!equal) {
            log.error("Expected " + PrintingVisitor.formattedQueryString(expectedScript));
            log.error("Actual " + PrintingVisitor.formattedQueryString(actualScript));
        }
        Assert.assertTrue(reason.reason, equal);
    }
    
    @Test
    public void noAppliedMapping() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("NOMAPPINGNAME == 'baz'");
        ASTJexlScript newScript = QueryModelVisitor.applyModel(script, model, allFields);
        assertScriptEquality(script, newScript);
    }
    
    @Test
    public void singleMapping() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("OUT == 'baz'");
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery("IN == 'baz'");
        ASTJexlScript actualScript = QueryModelVisitor.applyModel(script, model, allFields);
        assertScriptEquality(expectedScript, actualScript);
    }
    
    @Test
    public void singleTermMapping() throws ParseException {
        for (int i = 0; i < 1000; i++) {
            ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO == 'baz'");
            ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery("(BAR1 == 'baz' || BAR2 == 'baz')");
            ASTJexlScript actualScript = QueryModelVisitor.applyModel(script, model, allFields);
            assertScriptEquality(expectedScript, actualScript);
        }
    }
    
    @Test
    public void singleTermMappingNot() throws ParseException {
        for (int i = 0; i < 1000; i++) {
            ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO != 'baz'");
            ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery("(BAR1 != 'baz' && BAR2 != 'baz')");
            ASTJexlScript actualScript = QueryModelVisitor.applyModel(script, model, allFields);
            assertScriptEquality(expectedScript, actualScript);
        }
    }
    
    @Test
    public void multipleMappings() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO == 'baz' && FIELD == 'taco' && OUT == 2");
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery("(BAR1 == 'baz' || BAR2 == 'baz') && FIELD == 'taco' && IN == 2");
        ASTJexlScript actualScript = QueryModelVisitor.applyModel(script, model, allFields);
        assertScriptEquality(expectedScript, actualScript);
    }
    
    @Test
    public void multipleMappingsWithBounds() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO > 'a' && FOO < 'z'");
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery("(BAR1 > 'a' && BAR1 < 'z') || (BAR2 > 'a' && BAR2 < 'z')");
        ASTJexlScript actualScript = QueryModelVisitor.applyModel(script, model, allFields);
        assertScriptEquality(expectedScript, actualScript);
    }
    
    @Test
    public void multipleMappingsWithBoundsIdentity() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("CYCLIC_FIELD > 'a' && CYCLIC_FIELD < 'z'");
        ASTJexlScript expectedScript = JexlASTHelper
                        .parseJexlQuery("(CYCLIC_FIELD > 'a' && CYCLIC_FIELD < 'z') || (CYCLIC_FIELD_ > 'a' && CYCLIC_FIELD_ < 'z')");
        ASTJexlScript actualScript = QueryModelVisitor.applyModel(script, model, allFields);
        assertScriptEquality(expectedScript, actualScript);
    }
    
    @Test
    public void functionMapping() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("filter:isNotNull(FOO)");
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery("filter:isNotNull(BAR1||BAR2)");
        ASTJexlScript actualScript = QueryModelVisitor.applyModel(script, model, allFields);
        assertScriptEquality(expectedScript, actualScript);
    }
    
    @Test
    public void functionWithMethod() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("filter:includeRegex(FOO, '1').size()");
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery("filter:includeRegex(BAR1||BAR2, '1').size()");
        ASTJexlScript actualScript = QueryModelVisitor.applyModel(script, model, allFields);
        assertScriptEquality(expectedScript, actualScript);
    }
    
    @Test
    public void functionWithMethodInExpression() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("filter:includeRegex(FOO, '1').size() > 0");
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery("filter:includeRegex(BAR1||BAR2, '1').size() > 0");
        ASTJexlScript actualScript = QueryModelVisitor.applyModel(script, model, allFields);
        assertScriptEquality(expectedScript, actualScript);
    }
    
    @Test
    public void identifierWithMethodWithFunctionInExpression() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("AG.containsAll(filter:someFunction(NAM, '1')) > 0");
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery("(AGE||ETA).containsAll(filter:someFunction((NAME||NOME), '1')) > 0");
        ASTJexlScript actualScript = QueryModelVisitor.applyModel(script, model, allFields);
        assertScriptEquality(expectedScript, actualScript);
    }
    
    @Test
    public void identifierWithMethodWithOneModelDoesNotAddParens() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("OUT.containsAll(filter:someFunction(OUT, '1')) > 0");
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery("IN.containsAll(filter:someFunction(IN, '1')) > 0");
        ASTJexlScript actualScript = QueryModelVisitor.applyModel(script, model, allFields);
        assertScriptEquality(expectedScript, actualScript);
        Assert.assertEquals(JexlStringBuildingVisitor.buildQuery(expectedScript), JexlStringBuildingVisitor.buildQuery(actualScript));
    }
    
    @Test
    public void wrongOptimizationInQueryModelVisitor() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("AGE > 10 && AGE > 0");
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery("AGE > 0 && AGE > 10");
        ASTJexlScript actualScript = QueryModelVisitor.applyModel(script, model, allFields);
        assertScriptEquality(expectedScript, actualScript);
    }
    
    @Test
    public void identifiersWithMethodsInAndWithGT() throws ParseException {
        ASTJexlScript script = JexlASTHelper
                        .parseJexlQuery("AGE.containsAll(filter:someFunction(BLAH, '1')) > 0 && AGE.containsAll(filter:someFunction(BOO, '1')) > 10");
        ASTJexlScript expectedScript = JexlASTHelper
                        .parseJexlQuery("AGE.containsAll(filter:someFunction(BLAH, '1')) > 0 && AGE.containsAll(filter:someFunction(BOO, '1')) > 10");
        ASTJexlScript actualScript = QueryModelVisitor.applyModel(script, model, allFields);
        assertScriptEquality(expectedScript, actualScript);
    }
    
    @Test
    public void anotherFunctionMapping() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("filter:isNull(FOO)");
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery("filter:isNull(BAR1||BAR2)");
        ASTJexlScript actualScript = QueryModelVisitor.applyModel(script, model, allFields);
        assertScriptEquality(expectedScript, actualScript);
    }
    
    @Test
    public void functionMappingWithTwoArgs() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("filter:someFunction(NAM,1,GEN,2)");
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery("filter:someFunction(NAME||NOME, 1, GENDER||GENERE, 2)");
        ASTJexlScript actualScript = QueryModelVisitor.applyModel(script, model, allFields);
        assertScriptEquality(expectedScript, actualScript);
    }
    
    @Test
    public void isNullFunctionMapping() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("filter:isNull(FOO)");
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery("filter:isNull(BAR1||BAR2)");
        ASTJexlScript actualScript = QueryModelVisitor.applyModel(script, model, allFields);
        assertScriptEquality(expectedScript, actualScript);
    }
    
    @Test
    public void attributeWithMethodMapping() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO.getValuesForGroups(0) == 1");
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery("(BAR2||BAR1).getValuesForGroups(0) == 1");
        ASTJexlScript actualScript = QueryModelVisitor.applyModel(script, model, allFields);
        assertScriptEquality(expectedScript, actualScript);
    }
    
    @Test
    public void attributeWithMethodAndFunctionMapping() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO.getValuesForGroups(grouping:getGroupsForMatchesInGroup(A, 'MEADOW', B, 'FEMALE')) < 19");
        ASTJexlScript expectedScript = JexlASTHelper
                        .parseJexlQuery("(BAR2||BAR1).getValuesForGroups(grouping:getGroupsForMatchesInGroup(A, 'MEADOW', B, 'FEMALE')) < 19");
        ASTJexlScript actualScript = QueryModelVisitor.applyModel(script, model, allFields);
        assertScriptEquality(expectedScript, actualScript);
    }
    
    @Test
    public void attributeWithMethodAndFunctionMappingInsideAndNode() throws ParseException {
        ASTJexlScript script = JexlASTHelper
                        .parseJexlQuery("FOO.getValuesForGroups(grouping:getGroupsForMatchesInGroup(OUT, 'MEADOW')) < 19 && FOO.getValuesForGroups(grouping:getGroupsForMatchesInGroup(OUT, 'MEADOW')) > 16");
        ASTJexlScript expectedScript = JexlASTHelper
                        .parseJexlQuery("(BAR2||BAR1).getValuesForGroups(grouping:getGroupsForMatchesInGroup(IN, 'MEADOW')) < 19 && (BAR2||BAR1).getValuesForGroups(grouping:getGroupsForMatchesInGroup(IN, 'MEADOW')) > 16");
        ASTJexlScript actualScript = QueryModelVisitor.applyModel(script, model, allFields);
        assertScriptEquality(expectedScript, actualScript);
    }
    
    @Test
    public void sizeMethodOnFunction() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("filter:includeRegex(NAM, 'MICHAEL').size() == 1");
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery("filter:includeRegex(NAME||NOME, 'MICHAEL').size() == 1");
        ASTJexlScript actualScript = QueryModelVisitor.applyModel(script, model, allFields);
        assertScriptEquality(expectedScript, actualScript);
    }
    
    @Test
    public void testSubstitutionsEverywhere() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("AG.getValuesForGroups(grouping:getGroupsForMatchesInGroup(NAM, 'MEADOW', GEN, 'FEMALE')) < 19");
        ASTJexlScript expectedScript = JexlASTHelper
                        .parseJexlQuery("(AGE||ETA).getValuesForGroups(grouping:getGroupsForMatchesInGroup(NAME||NOME, 'MEADOW', GENDER||GENERE, 'FEMALE')) < 19");
        ASTJexlScript actualScript = QueryModelVisitor.applyModel(script, model, allFields);
        assertScriptEquality(expectedScript, actualScript);
    }
    
    @Test
    public void testMoreMethods() throws ParseException {
        // @formatter:off
        String[] queries = {
                "NAM.size()",
                "(NAM).size()",
                "(NAME).size()",
                "NAM.values()",
                "NAM.getGroupsForValue(FOO)",
                "NAM.min().hashCode() != 0"
        };
        String [] expectedResults = {
                "(NAME||NOME).size()",
                "(NAME||NOME).size()",
                "NAME.size()",
                "(NAME||NOME).values()",
                "(NAME||NOME).getGroupsForValue(BAR1||BAR2)",
                "(NAME||NOME).min().hashCode() != 0"
        };
        // @formatter:on
        
        for (int i = 0; i < queries.length; i++) {
            ASTJexlScript script = JexlASTHelper.parseJexlQuery(queries[i]);
            ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(expectedResults[i]);
            ASTJexlScript actualScript = QueryModelVisitor.applyModel(script, model, allFields);
            assertScriptEquality(expectedScript, actualScript);
        }
    }
    
    @Test
    public void additiveExpansion() throws ParseException {
        ASTJexlScript script = JexlASTHelper
                        .parseJexlQuery("grouping:matchesInGroup(NAM, 'MEADOW', GEN, 'FEMALE').size() + grouping:matchesInGroup(NAM, 'ANTHONY', GEN, 'MALE').size() >= 1");
        ASTJexlScript expectedScript = JexlASTHelper
                        .parseJexlQuery("grouping:matchesInGroup(NAME||NOME, 'MEADOW', GENDER||GENERE, 'FEMALE').size() + grouping:matchesInGroup(NAME||NOME, 'ANTHONY', GENDER||GENERE, 'MALE').size() >= 1");
        ASTJexlScript actualScript = QueryModelVisitor.applyModel(script, model, allFields);
        assertScriptEquality(expectedScript, actualScript);
    }
    
    @Test
    public void contentFunctionMapping() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("content:phrase(FOO, termOffsetMap, 'a', 'little', 'phrase')");
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery("content:phrase(BAR1||BAR2, termOffsetMap, 'a', 'little', 'phrase')");
        ASTJexlScript actualScript = QueryModelVisitor.applyModel(script, model, allFields);
        assertScriptEquality(expectedScript, actualScript);
    }
    
    @Test
    public void testSpecialCharAppliedToNumericQuery() throws ParseException {
        model.addTermToModel("FOO1", "1BAR");
        model.addTermToModel("FOO1", "2BAR");
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO1 == 'baz'");
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery("($1BAR == 'baz' || $2BAR == 'baz')");
        ASTJexlScript actualScript = QueryModelVisitor.applyModel(script, model, allFields);
        assertScriptEquality(expectedScript, actualScript);
    }
    
    @Test
    public void testNullInQuery() throws ParseException {
        model.addTermToModel("FOO1", "1BAR");
        model.addTermToModel("FOO1", "2BAR");
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO1 == 'baz' && FOO1 == null");
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery("($1BAR == 'baz' || $2BAR == 'baz') && ($1BAR == null && $2BAR == null)");
        ASTJexlScript actualScript = QueryModelVisitor.applyModel(script, model, allFields);
        assertScriptEquality(expectedScript, actualScript);
    }
    
    @Test
    public void testCastesianProduct() throws ParseException {
        model.addTermToModel("FOO1", "1BAR");
        model.addTermToModel("FOO1", "2BAR");
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO == FOO1");
        ASTJexlScript groomed = JexlASTHelper.InvertNodeVisitor.invertSwappedNodes(script);
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery("(BAR1 == $1BAR || BAR2 == $1BAR || BAR1 == $2BAR || BAR2 == $2BAR)");
        ASTJexlScript actualScript = QueryModelVisitor.applyModel(groomed, model, allFields);
        assertScriptEquality(expectedScript, actualScript);
    }
    
    @Test
    public void testModelApplication() throws ParseException {
        model.addTermToModel("FOO1", "1BAR");
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO1 == 'baz'");
        ASTJexlScript groomed = JexlASTHelper.InvertNodeVisitor.invertSwappedNodes(script);
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery("$1BAR == 'baz'");
        ASTJexlScript actualScript = QueryModelVisitor.applyModel(groomed, model, allFields);
        assertScriptEquality(expectedScript, actualScript);
        
        List<ASTEQNode> actualNodes = JexlASTHelper.getEQNodes(actualScript);
        
        for (ASTEQNode node : actualNodes) {
            Assert.assertTrue(node.jjtGetChild(0) instanceof ASTReference);
            Assert.assertTrue(node.jjtGetChild(1) instanceof ASTReference);
        }
    }
    
    @Test
    public void testAppliedModelWithNullNoFail() throws ParseException {
        model.addTermToModel("FOO1", "BAR1");
        model.addTermToModel("OTHER", "9_2");
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO1 == 'baz' and OTHER == null");
        ASTJexlScript groomed = JexlASTHelper.InvertNodeVisitor.invertSwappedNodes(script);
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery("BAR1 == 'baz' and $9_2 == null");
        ASTJexlScript actualScript = QueryModelVisitor.applyModel(groomed, model, allFields);
        assertScriptEquality(expectedScript, actualScript);
        
        MockMetadataHelper helper = new MockMetadataHelper();
        helper.addNormalizers("FOO1", Sets.newHashSet(new LcNoDiacriticsType()));
        Multimap<String,String> maps = ArrayListMultimap.create();
        maps.put("9_2", "datatype1");
        helper.addFieldsToDatatypes(maps);
        Multimap<String,Type<?>> types = FetchDataTypesVisitor.fetchDataTypes(helper, Collections.singleton("datatype1"), actualScript);
        Assert.assertEquals(types.size(), 4);
        
        Assert.assertTrue(types.values().stream().allMatch((o) -> o instanceof LcNoDiacriticsType || o instanceof NoOpType));
    }
}
