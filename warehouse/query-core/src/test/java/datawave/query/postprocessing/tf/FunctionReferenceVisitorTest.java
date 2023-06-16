package datawave.query.postprocessing.tf;

import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The {@link FunctionReferenceVisitor} is purpose built for TermFrequency functions, but will also work on other functions
 */
public class FunctionReferenceVisitorTest {

    private String currentNamespace;
    private String currentFunctionName;
    private Multimap<String,Function> functions;

    @Test
    public void testArbitraryFunction() throws ParseException {
        String query = "f:function(x1,x2,x3,x4,x5)";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);

        Multimap<String,Function> functions = FunctionReferenceVisitor.functions(script);

        assertTrue(functions.containsKey("f"));
        assertEquals(1, functions.keySet().size());

        Collection<Function> functionsInNamespace = functions.get("f");
        assertEquals(1, functionsInNamespace.size());

        Function f = functionsInNamespace.iterator().next();
        assertEquals("function", f.name());
        assertEquals(5, f.args().size());

        Iterator<JexlNode> iter = f.args().iterator();
        assertEquals("x1", iter.next().jjtGetChild(0).image);
        assertEquals("x2", iter.next().jjtGetChild(0).image);
        assertEquals("x3", iter.next().jjtGetChild(0).image);
        assertEquals("x4", iter.next().jjtGetChild(0).image);
        assertEquals("x5", iter.next().jjtGetChild(0).image);
        assertFalse(iter.hasNext());
    }

    @Test
    public void testContentFunctionAdjacent() {
        String query = "content:adjacent(termOffsetMap, 'hello', 'world')";
        parseFunctions(query);
        assertNamespace("content");
        assertFunctionName("adjacent");
        assertFunctionArgs(Arrays.asList("termOffsetMap", "hello", "world"));

        query = "content:adjacent(BODY, termOffsetMap, 'hello', 'world')";
        parseFunctions(query);
        assertNamespace("content");
        assertFunctionName("adjacent");
        assertFunctionArgs(Arrays.asList("BODY", "termOffsetMap", "hello", "world"));
    }

    @Test
    public void testContentFunctionPhrase() {
        String query = "content:phrase(termOffsetMap, 'hello', 'world')";
        parseFunctions(query);
        assertNamespace("content");
        assertFunctionName("phrase");
        assertFunctionArgs(Arrays.asList("termOffsetMap", "hello", "world"));

        query = "content:phrase(BODY, termOffsetMap, 'hello', 'world')";
        parseFunctions(query);
        assertNamespace("content");
        assertFunctionName("phrase");
        assertFunctionArgs(Arrays.asList("BODY", "termOffsetMap", "hello", "world"));
    }

    @Test
    public void testContentFunctionScoredPhrase() {
        String query = "content:scoredPhrase(-1.5, 'red', 'car')";
        parseFunctions(query);
        assertNamespace("content");
        assertFunctionName("scoredPhrase");
        assertFunctionArgs(Arrays.asList("1.5", "red", "car"));

        query = "content:scoredPhrase(BODY, -1.5, 'red', 'car')";
        parseFunctions(query);
        assertNamespace("content");
        assertFunctionName("scoredPhrase");
        assertFunctionArgs(Arrays.asList("BODY", "1.5", "red", "car"));
    }

    @Test
    public void testContentFunctionWithin() {
        String query = "content:within(2, termOffsetMap,'brown', 'fox')";
        parseFunctions(query);
        assertNamespace("content");
        assertFunctionName("within");
        assertFunctionArgs(Arrays.asList("2", "termOffsetMap", "brown", "fox"));

        query = "content:within(BODY, 2, termOffsetMap, 'brown', 'fox')";
        parseFunctions(query);
        assertNamespace("content");
        assertFunctionName("within");
        assertFunctionArgs(Arrays.asList("BODY", "2", "termOffsetMap", "brown", "fox"));
    }

    @Test
    public void testFilterIsNull() {
        String query = "filter:isNull(FIELD)";
        parseFunctions(query);
        assertNamespace("filter");
        assertFunctionName("isNull");
        assertFunctionArgs(Collections.singletonList("FIELD"));

        query = "filter:isNull(FIELD_A||FIELD_B)";
        parseFunctions(query);
        assertNamespace("filter");
        assertFunctionName("isNull");
        assertFunctionArgs(Collections.singletonList("(FIELD_A || FIELD_B)"));
    }

    @Test
    public void testFilterIsNotNull() {
        String query = "filter:isNotNull(FIELD)";
        parseFunctions(query);
        assertNamespace("filter");
        assertFunctionName("isNotNull");
        assertFunctionArgs(Collections.singletonList("FIELD"));

        query = "filter:isNotNull(FIELD_A||FIELD_B)";
        parseFunctions(query);
        assertNamespace("filter");
        assertFunctionName("isNotNull");
        assertFunctionArgs(Collections.singletonList("(FIELD_A || FIELD_B)"));
    }

    @Test
    public void testFilterIncludeRegex() {
        String query = "filter:includeRegex(FIELD, 'ba.*')";
        parseFunctions(query);
        assertNamespace("filter");
        assertFunctionName("includeRegex");
        assertFunctionArgs(Arrays.asList("FIELD", "ba.*"));
    }

    @Test
    public void testFilterExcludeRegex() {
        String query = "filter:excludeRegex(FIELD, 'ba.*')";
        parseFunctions(query);
        assertNamespace("filter");
        assertFunctionName("excludeRegex");
        assertFunctionArgs(Arrays.asList("FIELD", "ba.*"));
    }

    @Test
    public void testFilterMatchesAtLeastCountOf() {
        String query = "filter:matchesAtLeastCountOf(3, FIELD, 'v1', 'v2', 'v3')";
        parseFunctions(query);
        assertNamespace("filter");
        assertFunctionName("matchesAtLeastCountOf");
        assertFunctionArgs(Arrays.asList("3", "FIELD", "v1", "v2", "v3"));
    }

    @Test
    public void testFilterOccurrence() {
        String query = "filter:occurrence(FOO, '==', 1)";
        parseFunctions(query);
        assertNamespace("filter");
        assertFunctionName("occurrence");
        assertFunctionArgs(Arrays.asList("FOO", "==", "1"));
    }

    @Test
    public void testFilterCompare() {
        String query = "filter:compare(FOO, '==', 'all', BAR)";
        parseFunctions(query);
        assertNamespace("filter");
        assertFunctionName("compare");
        assertFunctionArgs(Arrays.asList("FOO", "==", "all", "BAR"));
    }

    @Test
    public void testGetMaxTime() {
        String query = "filter:getMaxTime(FIELD)";
        parseFunctions(query);
        assertNamespace("filter");
        assertFunctionName("getMaxTime");
        assertFunctionArgs(Collections.singletonList("FIELD"));
    }

    @Test
    public void testTimeFunction() {
        String query = "filter:timeFunction(DEATH_DATE,BIRTH_DATE,'-','>',2522880000000L)";
        parseFunctions(query);
        assertNamespace("filter");
        assertFunctionName("timeFunction");
        assertFunctionArgs(Arrays.asList("DEATH_DATE", "BIRTH_DATE", "-", ">", "2522880000000L"));
    }

    @Test
    public void testBeforeLoadDate() {
        String query = "filter:afterLoadDate(FOO, '20140101')";
        parseFunctions(query);
        assertNamespace("filter");
        assertFunctionName("afterLoadDate");
        assertFunctionArgs(Arrays.asList("FOO", "20140101"));
    }

    @Test
    public void testAfterLoadDate() {
        String query = "filter:beforeLoadDate(FOO, '20140101')";
        parseFunctions(query);
        assertNamespace("filter");
        assertFunctionName("beforeLoadDate");
        assertFunctionArgs(Arrays.asList("FOO", "20140101"));
    }

    @Test
    public void testBetweenLoadDates() {
        String query = "filter:betweenLoadDates(FOO, '20140101', '20140102')";
        parseFunctions(query);
        assertNamespace("filter");
        assertFunctionName("betweenLoadDates");
        assertFunctionArgs(Arrays.asList("FOO", "20140101", "20140102"));
    }

    @Test
    public void testSingleNamespaceMultipleFunctionNames() {
        String query = "filter:isNotNull(FIELD) && filter:afterLoadDate(FOO,'20140101')";
        parseFunctions(query);
        assertNamespace("filter");
        assertFunctionNames("isNotNull", "afterLoadDate");
    }

    @Test
    public void testMultipleNamespaceMultipleFunctionNames() {
        String query = "filter:isNotNull(FIELD) && content:phrase(FOO,termOffsetMap, 'brown', 'fox')";
        parseFunctions(query);
        assertNameSpaces("filter", "content");
        assertFunctionNames("isNotNull", "phrase");
    }

    @Test
    public void testMultipleFunctionsWithNamespaceFilter() {
        Set<String> filter = Sets.newHashSet("filter", "content");
        String query = "filter:isNotNull(FIELD) && content:phrase(FOO,termOffsetMap, 'brown', 'fox')";
        parseFunctions(query, filter);
        assertNameSpaces("filter", "content");
        assertFunctionNames("isNotNull", "phrase");
    }

    @Test
    public void testMultipleFunctionsWithContentNamespaceFilter() {
        Set<String> filter = Collections.singleton("content");
        String query = "filter:isNotNull(FIELD) && content:phrase(FOO,termOffsetMap, 'brown', 'fox')";
        parseFunctions(query, filter);
        assertNameSpaces("content");
        assertFunctionNames("phrase");
    }

    @Test
    public void testMultipleFunctionsWithFilterNamespaceFilter() {
        Set<String> filter = Collections.singleton("filter");
        String query = "filter:isNotNull(FIELD) && content:phrase(FOO,termOffsetMap, 'brown', 'fox')";
        parseFunctions(query, filter);
        assertNameSpaces("filter");
        assertFunctionNames("isNotNull");
    }

    @Test
    public void testNamespaceFilterWithNoHits() {
        Set<String> filter = Collections.singleton("fourOhFourNamespaceNotfound");
        String query = "filter:isNotNull(FIELD) && content:phrase(FOO,termOffsetMap, 'brown', 'fox')";
        parseFunctions(query, filter);
        assertTrue(functions.isEmpty());
    }

    // helper methods

    private void parseFunctions(String query) {
        parseFunctions(query, Collections.emptySet());
    }

    private void parseFunctions(String query, Set<String> namespaceFilter) {
        try {
            ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
            this.functions = FunctionReferenceVisitor.functions(script, namespaceFilter);
        } catch (ParseException e) {
            fail("Failed to parse query: " + query);
        }
    }

    /**
     * Assert single namespace exists
     *
     * @param namespace
     *            single function namespace
     */
    private void assertNamespace(String namespace) {
        this.currentNamespace = namespace;
        assertTrue(functions.containsKey(currentNamespace));
        assertEquals(1, functions.keySet().size());
    }

    /**
     * Assert multiple namespaces
     *
     * @param namespaces
     *            multiple function namespaces
     */
    private void assertNameSpaces(String... namespaces) {
        for (String namespace : namespaces) {
            assertTrue(functions.containsKey(namespace));
        }
    }

    /**
     * Assert function name, assumes only one function exists
     *
     * @param functionName
     *            the function name
     */
    private void assertFunctionName(String functionName) {
        this.currentFunctionName = functionName;
        assertTrue(functions.containsKey(currentNamespace));
        Function f = functions.get(currentNamespace).iterator().next();
        assertEquals(currentFunctionName, f.name());
    }

    /**
     * Assert multiple function names, assumes multiple functions exist
     *
     * @param functionNames
     *            the function names
     */
    private void assertFunctionNames(String... functionNames) {
        for (String functionName : functionNames) {
            boolean found = false;
            for (String key : functions.keySet()) {
                for (Function f : functions.get(key)) {
                    if (f.name().equals(functionName)) {
                        found = true;
                        break;
                    }
                }
                if (found)
                    break;
            }
            assertTrue(found);
        }
    }

    /**
     * Assert function args for a query. Assumes
     *
     * @param args
     *            list of expected args
     */
    private void assertFunctionArgs(List<String> args) {
        assertTrue(functions.containsKey(currentNamespace));
        Function f = functions.get(currentNamespace).iterator().next();
        assertEquals(args.size(), f.args().size());

        Iterator<JexlNode> iter = f.args().iterator();
        for (String arg : args) {
            assertTrue(iter.hasNext());
            JexlNode node = iter.next();
            if (node instanceof ASTUnaryMinusNode) {
                node = node.jjtGetChild(0);
                assertEquals(arg, node.image);
            } else if (node instanceof ASTOrNode) {
                // don't both with recursively extracting all identifiers, just compare strings
                assertEquals(arg, JexlStringBuildingVisitor.buildQueryWithoutParse(node));
            } else {
                node = JexlASTHelper.dereference(node);
                assertEquals(arg, node.image);
            }
        }
    }
}
