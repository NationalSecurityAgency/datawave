package datawave.query.jexl.visitors;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NoOpType;
import datawave.data.type.Type;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.language.functions.jexl.NoExpansion;
import datawave.query.model.QueryModel;
import datawave.query.util.MockMetadataHelper;
import datawave.test.JexlNodeAssert;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class QueryModelVisitorTest {
    
    private final Logger log = org.apache.log4j.Logger.getLogger(QueryModelVisitorTest.class);
    
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
    
    @Test
    public void noAppliedMapping() throws ParseException {
        String original = "NOMAPPINGNAME == 'baz'";
        assertResult(original, original);
    }
    
    @Test
    public void singleMapping() throws ParseException {
        String original = "OUT == 'baz'";
        String expected = "IN == 'baz'";
        assertResult(original, expected);
    }
    
    @Test
    public void singleTermMapping() throws ParseException {
        for (int i = 0; i < 1000; i++) {
            String original = "FOO == 'baz'";
            String expected = "(BAR1 == 'baz' || BAR2 == 'baz')";
            assertResult(original, expected);
        }
    }
    
    @Test
    public void singleTermMappingNot() throws ParseException {
        for (int i = 0; i < 1000; i++) {
            String original = "FOO != 'baz'";
            String expected = "(BAR1 != 'baz' && BAR2 != 'baz')";
            assertResult(original, expected);
        }
    }
    
    @Test
    public void multipleMappings() throws ParseException {
        String original = "FOO == 'baz' && FIELD == 'taco' && OUT == 2";
        String expected = "(BAR1 == 'baz' || BAR2 == 'baz') && FIELD == 'taco' && IN == 2";
        assertResult(original, expected);
    }
    
    @Test
    public void multipleMappingsWithBounds() throws ParseException {
        String original = "((_Bounded_ = true) && (FOO > 'a' && FOO < 'z'))";
        String expected = "((_Bounded_ = true) && (BAR1 > 'a' && BAR1 < 'z')) || ((_Bounded_ = true) && (BAR2 > 'a' && BAR2 < 'z'))";
        assertResult(original, expected);
    }
    
    @Test
    public void multipleMappingsWithBoundsIdentity() throws ParseException {
        String original = "((_Bounded_ = true) && (CYCLIC_FIELD > 'a' && CYCLIC_FIELD < 'z'))";
        String expected = "(((_Bounded_ = true) && (CYCLIC_FIELD > 'a' && CYCLIC_FIELD < 'z')) || ((_Bounded_ = true) && (CYCLIC_FIELD_ > 'a' && CYCLIC_FIELD_ < 'z')))";
        assertResult(original, expected);
    }
    
    @Test
    public void functionMapping() throws ParseException {
        String original = "filter:isNotNull(FOO)";
        String expected = "filter:isNotNull(BAR1||BAR2)";
        assertResult(original, expected);
    }
    
    @Test
    public void functionWithMethod() throws ParseException {
        String original = "filter:includeRegex(FOO, '1').size()";
        String expected = "filter:includeRegex(BAR1||BAR2, '1').size()";
        assertResult(original, expected);
    }
    
    @Test
    public void functionWithMethodInExpression() throws ParseException {
        String original = "filter:includeRegex(FOO, '1').size() > 0";
        String expected = "filter:includeRegex(BAR1||BAR2, '1').size() > 0";
        assertResult(original, expected);
    }
    
    @Test
    public void identifierWithMethodWithFunctionInExpression() throws ParseException {
        String original = "AG.containsAll(filter:someFunction(NAM, '1')) > 0";
        String expected = "(AGE||ETA).containsAll(filter:someFunction((NAME||NOME), '1')) > 0";
        assertResult(original, expected);
    }
    
    @Test
    public void identifierWithMethodWithOneModelDoesNotAddParens() throws ParseException {
        String original = "OUT.containsAll(filter:someFunction(OUT, '1')) > 0";
        String expected = "IN.containsAll(filter:someFunction(IN, '1')) > 0";
        assertResult(original, expected);
    }
    
    @Test
    public void wrongOptimizationInQueryModelVisitor() throws ParseException {
        String original = "AGE > 10 && AGE > 0";
        String expected = "AGE > 0 && AGE > 10";
        assertResult(original, expected);
    }
    
    @Test
    public void identifiersWithMethodsInAndWithGT() throws ParseException {
        String original = "AGE.containsAll(filter:someFunction(BLAH, '1')) > 0 && AGE.containsAll(filter:someFunction(BOO, '1')) > 10";
        assertResult(original, original);
    }
    
    @Test
    public void anotherFunctionMapping() throws ParseException {
        String original = "filter:isNull(FOO)";
        String expected = "filter:isNull(BAR1||BAR2)";
        assertResult(original, expected);
    }
    
    @Test
    public void functionMappingWithTwoArgs() throws ParseException {
        String original = "filter:someFunction(NAM,1,GEN,2)";
        String expected = "filter:someFunction(NAME||NOME, 1, GENDER||GENERE, 2)";
        assertResult(original, expected);
    }
    
    @Test
    public void isNullFunctionMapping() throws ParseException {
        String original = "filter:isNull(FOO)";
        String expected = "filter:isNull(BAR1||BAR2)";
        assertResult(original, expected);
    }
    
    @Test
    public void attributeWithMethodMapping() throws ParseException {
        String original = "FOO.getValuesForGroups(0) == 1";
        String expected = "(BAR2||BAR1).getValuesForGroups(0) == 1";
        assertResult(original, expected);
    }
    
    @Test
    public void attributeWithMethodAndFunctionMapping() throws ParseException {
        String original = "FOO.getValuesForGroups(grouping:getGroupsForMatchesInGroup(A, 'MEADOW', B, 'FEMALE')) < 19";
        String expected = "(BAR2||BAR1).getValuesForGroups(grouping:getGroupsForMatchesInGroup(A, 'MEADOW', B, 'FEMALE')) < 19";
        assertResult(original, expected);
    }
    
    @Test
    public void attributeWithMethodAndFunctionMappingInsideAndNode() throws ParseException {
        String original = "FOO.getValuesForGroups(grouping:getGroupsForMatchesInGroup(OUT, 'MEADOW')) < 19 && FOO.getValuesForGroups(grouping:getGroupsForMatchesInGroup(OUT, 'MEADOW')) > 16";
        String expected = "(BAR2||BAR1).getValuesForGroups(grouping:getGroupsForMatchesInGroup(IN, 'MEADOW')) < 19 && (BAR2||BAR1).getValuesForGroups(grouping:getGroupsForMatchesInGroup(IN, 'MEADOW')) > 16";
        assertResult(original, expected);
    }
    
    @Test
    public void sizeMethodOnFunction() throws ParseException {
        String original = "filter:includeRegex(NAM, 'MICHAEL').size() == 1";
        String expected = "filter:includeRegex(NAME||NOME, 'MICHAEL').size() == 1";
        assertResult(original, expected);
    }
    
    @Test
    public void testSubstitutionsEverywhere() throws ParseException {
        String original = "AG.getValuesForGroups(grouping:getGroupsForMatchesInGroup(NAM, 'MEADOW', GEN, 'FEMALE')) < 19";
        String expected = "(AGE||ETA).getValuesForGroups(grouping:getGroupsForMatchesInGroup(NAME||NOME, 'MEADOW', GENDER||GENERE, 'FEMALE')) < 19";
        assertResult(original, expected);
    }
    
    @Test
    public void testMoreMethods() throws ParseException {
        assertResult("NAM.size()", "(NAME||NOME).size()");
        assertResult("(NAM).size()", "(NAME||NOME).size()");
        assertResult("(NAME).size()", "NAME.size()");
        assertResult("NAM.values()", "(NAME||NOME).values()");
        assertResult("NAM.getGroupsForValue(FOO)", "(NAME||NOME).getGroupsForValue(BAR1||BAR2)");
        assertResult("NAM.min().hashCode() != 0", "(NAME||NOME).min().hashCode() != 0");
    }
    
    @Test
    public void additiveExpansion() throws ParseException {
        String original = "grouping:matchesInGroup(NAM, 'MEADOW', GEN, 'FEMALE').size() + grouping:matchesInGroup(NAM, 'ANTHONY', GEN, 'MALE').size() >= 1";
        String expected = "grouping:matchesInGroup(NAME||NOME, 'MEADOW', GENDER||GENERE, 'FEMALE').size() + grouping:matchesInGroup(NAME||NOME, 'ANTHONY', GENDER||GENERE, 'MALE').size() >= 1";
        assertResult(original, expected);
    }
    
    @Test
    public void contentFunctionMapping() throws ParseException {
        String original = "content:phrase(FOO, termOffsetMap, 'a', 'little', 'phrase')";
        String expected = "content:phrase(BAR1||BAR2, termOffsetMap, 'a', 'little', 'phrase')";
        assertResult(original, expected);
    }
    
    @Test
    public void testSpecialCharAppliedToNumericQuery() throws ParseException {
        model.addTermToModel("FOO1", "1BAR");
        model.addTermToModel("FOO1", "2BAR");
        
        String original = "FOO1 == 'baz'";
        String expected = "($1BAR == 'baz' || $2BAR == 'baz')";
        assertResult(original, expected);
    }
    
    @Test
    public void testNullInQuery() throws ParseException {
        model.addTermToModel("FOO1", "1BAR");
        model.addTermToModel("FOO1", "2BAR");
        
        String original = "FOO1 == 'baz' && FOO1 == null";
        String expected = "($1BAR == 'baz' || $2BAR == 'baz') && ($1BAR == null && $2BAR == null)";
        assertResult(original, expected);
    }
    
    @Test
    public void testCastesianProduct() throws ParseException {
        model.addTermToModel("FOO1", "1BAR");
        model.addTermToModel("FOO1", "2BAR");
        
        String original = "FOO == FOO1";
        ASTJexlScript groomed = JexlASTHelper.InvertNodeVisitor.invertSwappedNodes(JexlASTHelper.parseJexlQuery(original));
        String expected = "(BAR1 == $1BAR || BAR2 == $1BAR || BAR1 == $2BAR || BAR2 == $2BAR)";
        assertResult(JexlStringBuildingVisitor.buildQuery(groomed), expected);
    }
    
    @Test
    public void testModelApplication() throws ParseException {
        model.addTermToModel("FOO1", "1BAR");
        
        String original = "FOO1 == 'baz'";
        ASTJexlScript groomed = JexlASTHelper.InvertNodeVisitor.invertSwappedNodes(JexlASTHelper.parseJexlQuery(original));
        String expected = "$1BAR == 'baz'";
        ASTJexlScript actualScript = assertResult(JexlStringBuildingVisitor.buildQuery(groomed), expected);
        
        List<ASTEQNode> actualNodes = JexlASTHelper.getEQNodes(actualScript);
        for (ASTEQNode node : actualNodes) {
            assertTrue(node.jjtGetChild(0) instanceof ASTReference);
            assertTrue(node.jjtGetChild(1) instanceof ASTReference);
        }
    }
    
    @Test
    public void testAppliedModelWithNullNoFail() throws ParseException {
        model.addTermToModel("FOO1", "BAR1");
        model.addTermToModel("OTHER", "9_2");
        
        String original = "FOO1 == 'baz' and OTHER == null";
        ASTJexlScript groomed = JexlASTHelper.InvertNodeVisitor.invertSwappedNodes(JexlASTHelper.parseJexlQuery(original));
        
        String expected = "BAR1 == 'baz' and $9_2 == null";
        ASTJexlScript actualScript = assertResult(JexlStringBuildingVisitor.buildQuery(groomed), expected);
        
        MockMetadataHelper helper = new MockMetadataHelper();
        helper.addNormalizers("FOO1", Sets.newHashSet(new LcNoDiacriticsType()));
        Multimap<String,String> maps = ArrayListMultimap.create();
        maps.put("9_2", "datatype1");
        helper.addFieldsToDatatypes(maps);
        Multimap<String,Type<?>> types = FetchDataTypesVisitor.fetchDataTypes(helper, Collections.singleton("datatype1"), actualScript);
        assertEquals(types.size(), 4);
        
        assertTrue(types.values().stream().allMatch((o) -> o instanceof LcNoDiacriticsType || o instanceof NoOpType));
    }
    
    @Test
    public void testModelExpansionWithNoExpansionFunction() {
        // model contains expansions for both FIELD_A and FIELD_B, presence of filter:noExpansion(FIELD_B) prevents
        // that portion of the model expansion.
        QueryModel model = new QueryModel();
        model.addTermToModel("FIELD_A", "FIELD_AA");
        model.addTermToModel("FIELD_A", "FIELD_AB");
        model.addTermToModel("FIELD_B", "FIELD_BB");
        model.addTermToModel("FIELD_B", "FIELD_BC");
        
        // base case, both original fields are expanded
        String query = "FIELD_A == 'bar' && FIELD_B == 'baz'";
        String expected = "(FIELD_AA == 'bar' || FIELD_AB == 'bar') && (FIELD_BB == 'baz' || FIELD_BC == 'baz')";
        testNoExpansion(query, expected, model, Collections.emptySet());
        
        // only FIELD_B is expanded
        query = "FIELD_A == 'bar' && FIELD_B == 'baz' && filter:noExpansion(FIELD_A)";
        expected = "FIELD_A == 'bar' && (FIELD_BB == 'baz' || FIELD_BC == 'baz')";
        testNoExpansion(query, expected, model, Sets.newHashSet("FIELD_A"));
        
        // only FIELD_A is expanded
        query = "FIELD_A == 'bar' && FIELD_B == 'baz' && filter:noExpansion(FIELD_B)";
        expected = "(FIELD_AB == 'bar' || FIELD_AA == 'bar') && FIELD_B == 'baz'";
        testNoExpansion(query, expected, model, Sets.newHashSet("FIELD_B"));
        
        // neither field is expanded
        query = "FIELD_A == 'bar' && FIELD_B == 'baz' && filter:noExpansion(FIELD_A,FIELD_B)";
        expected = "FIELD_A == 'bar' && FIELD_B == 'baz'";
        testNoExpansion(query, expected, model, Sets.newHashSet("FIELD_A", "FIELD_B"));
        
        // both fields are expanded, NoExpansion function specified a field that does not exist in the query
        query = "FIELD_A == 'bar' && FIELD_B == 'baz' && filter:noExpansion(FIELD_X,FIELD_Y)";
        expected = "(FIELD_AA == 'bar' || FIELD_AB == 'bar') && (FIELD_BB == 'baz' || FIELD_BC == 'baz')";
        testNoExpansion(query, expected, model, Sets.newHashSet("FIELD_X", "FIELD_Y"));
        
        query = "FIELD_A == 'bar' && FIELD_B == 'baz' && filter:noExpansion(FIELD_X,FIELD_Y,FIELD_A)";
        expected = "FIELD_A == 'bar' && (FIELD_BB == 'baz' || FIELD_BC == 'baz')";
        testNoExpansion(query, expected, model, Sets.newHashSet("FIELD_A", "FIELD_X", "FIELD_Y"));
        
        query = "FIELD_A == 'bar' && FIELD_B == 'baz' && filter:noExpansion(FIELD_A,FIELD_X,FIELD_Y)";
        expected = "FIELD_A == 'bar' && (FIELD_BB == 'baz' || FIELD_BC == 'baz')";
        testNoExpansion(query, expected, model, Sets.newHashSet("FIELD_A", "FIELD_X", "FIELD_Y"));
    }
    
    // NoExpansion only excludes fields in the original query. A query can still expand into an excluded field
    @Test
    public void testStillExpandIntoExcludedField() {
        QueryModel model = new QueryModel();
        model.addTermToModel("FIELD_A", "FIELD_B");
        model.addTermToModel("FIELD_A", "FIELD_C");
        
        String query = "FIELD_A == 'bar' && filter:noExpansion(FIELD_C)";
        String expected = "(FIELD_B == 'bar' || FIELD_C == 'bar')";
        testNoExpansion(query, expected, model, Sets.newHashSet("FIELD_C"));
    }
    
    private void testNoExpansion(String query, String expected, QueryModel model, Set<String> expectedFields) {
        try {
            Set<String> allFields = new HashSet<>();
            allFields.addAll(model.getForwardQueryMapping().keySet());
            allFields.addAll(model.getForwardQueryMapping().values());
            
            ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
            
            NoExpansionFunctionVisitor.VisitResult result = NoExpansionFunctionVisitor.findNoExpansionFields(script);
            
            JexlNodeAssert.assertThat(script).isNotEqualTo(result.script);
            Assert.assertEquals(expectedFields, result.noExpansionFields);
            
            ASTJexlScript applied;
            if (result.noExpansionFields.size() > 0) {
                applied = QueryModelVisitor.applyModel(result.script, model, allFields, result.noExpansionFields);
            } else {
                applied = QueryModelVisitor.applyModel(result.script, model, allFields);
            }
            
            if (log.isTraceEnabled()) {
                log.trace("expected: " + expected);
                log.trace("actual  : " + JexlStringBuildingVisitor.buildQueryWithoutParse(applied));
            }
            
            ASTJexlScript expectedScript = JexlASTHelper.parseAndFlattenJexlQuery(expected);
            assertTrue(TreeEqualityVisitor.checkEquality(expectedScript, applied).isEqual());
            
        } catch (ParseException e) {
            fail("Error testing query: " + query);
        }
    }
    
    private ASTJexlScript assertResult(String original, String expected) throws ParseException {
        ASTJexlScript originalScript = JexlASTHelper.parseJexlQuery(original);
        
        ASTJexlScript actualScript = QueryModelVisitor.applyModel(originalScript, model, allFields);
        
        // Verify the resulting script is as expected with a valid lineage.
        JexlNodeAssert.assertThat(actualScript).isEqualTo(expected).hasValidLineage();
        
        // Verify the original script was not modified, and has a valid lineage.
        JexlNodeAssert.assertThat(originalScript).isEqualTo(original).hasValidLineage();
        
        return actualScript;
    }
}
