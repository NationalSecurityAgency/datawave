package datawave.query.jexl.visitors.pushdown;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Collections;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.junit.jupiter.api.Test;

import datawave.query.jexl.JexlASTHelper;

class AnchorDetectionVisitorTest {

    private final Set<String> indexOnlyFields = Collections.singleton("IO");
    private final Set<String> indexedFields = Collections.singleton("F");
    private AnchorDetectionVisitor visitor;

    @Test
    void testIndexedLeaves() {
        //  @formatter:off
        String[] queries = new String[]{
                        "F == '1'",
                        "F != '1'",
                        "F < '2'",
                        "F > '2'",
                        "F <= '2'",
                        "F >= '2'",
                        "F =~ 'ba.*'",
                        "F !~ 'ba.*'",
        };
        //  @formatter:on

        test(queries, true);
    }

    @Test
    void testIndexOnlyLeaves() {
        //  @formatter:off
        String[] queries = new String[]{
                        "IO == '1'",
                        "IO != '1'",
                        "IO < '2'",
                        "IO > '2'",
                        "IO <= '2'",
                        "IO >= '2'",
                        "IO =~ 'ba.*'",
                        "IO !~ 'ba.*'",
        };
        //  @formatter:on

        test(queries, true);
    }

    @Test
    void testNonIndexedLeaves() {
        //  @formatter:off
        String[] queries = new String[]{
                        "FIELD == '1'",
                        "FIELD != '1'",
                        "FIELD < '2'",
                        "FIELD > '2'",
                        "FIELD <= '2'",
                        "FIELD >= '2'",
                        "FIELD =~ 'ba.*'",
                        "FIELD !~ 'ba.*'",
        };
        //  @formatter:on

        test(queries, false);
    }

    @Test
    void testNullLiterals() {
        test("F == null", false);
        test("F != null", false);
        test("IO == null", false);
        test("IO != null", false);
        test("FIELD == null", false);
        test("FIELD != null", false);
    }

    @Test
    void testFilterFunctions() {
        //  @formatter:off
        String[] queries = new String[]{
                        //  index only include/exclude are rewritten to regex nodes
                        "filter:include(F, 'ba.*')",
                        "filter:exclude(F, 'ba.*')",
                        "filter:include(FIELD, 'ba.*')",
                        "filter:exclude(FIELD, 'ba.*')",
                        //  isNull functions should be rewritten to 'F == null'
                        "filter:isNull(F)",
                        "filter:isNull(F)",
                        "filter:isNull(FIELD)",
                        "filter:isNull(FIELD)",
                        //  isNotNull functions should be rewritten to !(F == null)
                        "filter:isNotNull(F)",
                        "filter:isNotNull(F)",
                        "filter:isNotNull(FIELD)",
                        "filter:isNotNull(FIELD)",
                        "filter:compare(F,'==','any',F)",
                        "filter:compare(IO,'==','any',IO)",
                        "filter:compare(FIELD,'==','any',FIELD)",
        };
        //  @formatter:on

        test(queries, false);
    }

    @Test
    void testMarkers() {
        //  @formatter:off
        String[] anchorMarkers = new String[] {
                        "((_Bounded_ = true) && (F > '2' && F < '5'))",
                        "((_List_ = true) && ((id = 'id') && (field = 'F') && (params = '{\"ranges\":[[\"[r1\",\"r2]\"],[\"[r3\",\"f4]\"]]}')))",
                        "((_Value_ = true) && (F =~ 'ba.*'))",
                        "((_Term_ = true) && (_ANYFIELD_ =~ 'ba.*'))"
        };
        //  @formatter:on

        test(anchorMarkers, true);

        //  @formatter:off
        String[] nonAnchorMarkers = new String[]{
                        "((_Delayed_ = true) && (F == '1'))",
                        "((_Eval_ = true) && (F == '1'))",
                        "((_Hole_ = true) && (F == '1'))",
                        "((_Drop_ = true) && (F == '1'))",
                        "((_Lenient_ = true) && (F == '1'))",
                        "((_Strict_ = true) && (F == '1'))"
        };
        //  @formatter:on

        test(nonAnchorMarkers, false);
    }

    @Test
    void testUnions() {
        //  @formatter:off
        String[] anchorUnions = new String[] {
                        "F == '1' || F == '2'",
                        "F == '1' || IO == '1'",
                        "IO == '1' || IO == '2'"};
        //  @formatter:on

        test(anchorUnions, true);

        //  @formatter:off
        String[] nonAnchorUnions = new String[] {
                        "FIELD == '1' || F == '2'",
                        "F == '1' || IO == '1' || FIELD == '3'",
                        "FIELD == '1' || FIELD == '2'"};
        //  @formatter:onn

        test(nonAnchorUnions, false);
    }

    @Test
    void testIntersections() {
        //  @formatter:off
        String[] anchorIntersections = new String[] {
                        "F == '1' && F == '2'",
                        "F == '1' && IO == '1'",
                        "IO == '1' && IO == '2'",
                        "F == '1' && IO == null",
                        "IO == '1' && IO == null",
                        // intersection needs just one anchor to be executable
                        "X == '1' && F == '2'", "X == '1' && IO == '2'"
        };
        //  @formatter:on

        test(anchorIntersections, true);

        //  @formatter:off
        String[] nonAnchorQueries = new String[] {
                        "X == '1' && Y == '2' && Z == '3'",
                        "F == null && IO == null",
        };
        //  @formatter:on

        test(nonAnchorQueries, false);
    }

    @Test
    void testNestedUnions() {
        //  @formatter:off
        String[] anchorNestedUnions = new String[]{
                        "(F == '1' || F == '2') && (F == '3' || F == '4')",
                        "(F == '1' || F == '2') && (IO == '3' || IO == '4')",
                        "(IO == '1' || IO == '2') && (F == '3' || F == '4')",
                        "(F == '1' || IO == '2') && (F == '3' || IO == '4')",
                        "(IO == '1' || F == '2') && (IO == '3' || F == '4')",
        };
        //  @formatter:on

        test(anchorNestedUnions, true);
    }

    @Test
    void testNestedIntersections() {
        //  @formatter:off
        String[] anchorNestedIntersections = new String[]{
                        "(F == '1' && F == '2') || (F == '3' && F == '4')",
                        "(F == '1' && F == '2') || (IO == '3' && IO == '4')",
                        "(IO == '1' && IO == '2') || (F == '3' && F == '4')",
                        "(F == '1' && IO == '2') || (F == '3' && IO == '4')",
                        "(IO == '1' && F == '2') || (IO == '3' && F == '4')",
        };
        //  @formatter:on

        test(anchorNestedIntersections, true);
    }

    @Test
    void testFullContentPhraseFunction() {
        String query = "content:phrase(F, termOffsetMap, 'foo', 'bar') && F == 'foo' && F == 'bar'";
        test(query, true);
    }

    @Test
    void testArithmeticAndSizeMethods() {
        //  @formatter:off
        String[] queries = new String[]{
                        //  filter
                        "filter:getMinTime(F) == 1892160000000",
                        "filter:getMinTime(F) != 1892160000000",
                        "filter:getMinTime(F) > 1892160000000",
                        "filter:getMinTime(F) < 1892160000000",
                        "filter:getMinTime(F) >= 1892160000000",
                        "filter:getMinTime(F) <= 1892160000000",
                        //  method
                        "F.size() == 1",
                        "F.size() != 1",
                        "F.size() > 1",
                        "F.size() < 1",
                        "F.size() >= 1",
                        "F.size() <= 1",
        };
        //  @formatter:on

        test(queries, false);
    }

    private void test(String[] queries, boolean expected) {
        for (String query : queries) {
            test(query, expected);
        }
    }

    private void test(String query, boolean expected) {
        JexlNode node = parseQuery(query);
        assertEquals(expected, getVisitor().isAnchor(node));
    }

    private JexlNode parseQuery(String query) {
        try {
            ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
            return script.jjtGetChild(0);
        } catch (Exception e) {
            fail("Could not parse query: " + query);
            throw new IllegalStateException(e);
        }
    }

    private AnchorDetectionVisitor getVisitor() {
        if (visitor == null) {
            visitor = new AnchorDetectionVisitor(indexedFields, indexOnlyFields);
        }
        return visitor;
    }
}
