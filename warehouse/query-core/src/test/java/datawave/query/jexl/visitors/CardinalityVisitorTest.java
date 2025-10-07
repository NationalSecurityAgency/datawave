package datawave.query.jexl.visitors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.Test;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.count.CountMap;

public class CardinalityVisitorTest {

    private CountMap counts;

    @Test
    public void testEqualityCardinality() {
        test(11, "A == '11'");
        test(23, "B == '23'");
        test(34, "C == '34'");
        test(77, "D == '77'");
        test(88, "E == '88'");
    }

    @Test
    public void testRangeOperators() {
        test(Long.MAX_VALUE, "A <= '11'");
        test(Long.MAX_VALUE, "B >= '23'");
        test(Long.MAX_VALUE, "C < '34'");
        test(Long.MAX_VALUE, "D > '77'");
    }

    @Test
    public void testMarkers() {
        // some markers are treated as max cost
        test(Long.MAX_VALUE, "((_Value_ = true) && (F =~ 'ba.*'))");
        test(Long.MAX_VALUE, "((_Term_ = true) && (_ANYFIELD_ =~ 'ba.*'))");
        test(Long.MAX_VALUE, "((_List_ = true) && (((id = 'some-bogus-id') && (field = 'QUOTE') && (params = '{\"values\":[\"a\",\"b\",\"c\"]}'))))");
        test(Long.MAX_VALUE, "((_Bounded_ = true) && (A >= 1 && A <= 2))");

        // some markers are treated as zero cost
        test(0L, "((_Delayed_ = true) && (F =~ 'pushed'))");
        test(0L, "((_Eval_ = true) && (F =~ 'evaluation'))");
        test(0L, "((_Drop_ = true) && (F =~ 'evaluation'))");
        test(0L, "((_Lenient_ = true) && (F =~ 'evaluation'))");
    }

    @Test
    public void testIntersectionOfEqualityTerms() {
        // EQ and EQ
        test(11, "(A == '11' && A == '11')");
        test(23, "(B == '23' && C == '34')");
        test(23, "(C == '34' && B == '23')");
        test(77, "(D == '77' && E == '88')");

        // EQ and node absent from the count map (to simulate an error)
        test(11, "(A == '11' && A == '5')");
        test(11, "(A == '5' && A == '11')");
        test(11, "(A == '11' && B == '35')");
        test(11, "(A == '11' && C == '12345')");
    }

    @Test
    public void testUnionOfEqualityTerms() {
        // EQ or EQ
        test(22, "(A == '11' || A == '11')");
        test(57, "(B == '23' || C == '34')");
        test(57, "(C == '34' || B == '23')");
        test(165, "(D == '77' || E == '88')");

        // EQ or node absent from the count map (to simulate an error)
        test(11, "(A == '11' || A == '5')");
        test(11, "(A == '5' || A == '11')");
        test(11, "(A == '11' || B == '35')");
        test(11, "(A == '11' || C == '12345')");
    }

    @Test
    public void testIntersectionOfMixedTerms() {
        // i.e. EQ and regex
        test(23, "(B == '23' && C =~ 'ca.*')");
        test(23, "(B == '23' && C !~ 'ca.*')");
    }

    @Test
    public void testUnionOfMixedTerms() {
        // i.e. EQ or regex
        test(Long.MAX_VALUE, "(B == '23' || C =~ 'ca.*')");
        test(Long.MAX_VALUE, "(B == '23' || C !~ 'ca.*')");
    }

    @Test
    public void testIntersectionOfRangeOperators() {
        // EQ and range
        test(34L, "C == '34' && B < '5'");
        test(34L, "C == '34' && B > '5'");
        test(34L, "C == '34' && B <= '5'");
        test(34L, "C == '34' && B >= '5'");
    }

    @Test
    public void testUnionOfRangeOperators() {
        // EQ or range
        test(Long.MAX_VALUE, "C == '34' || B < '5'");
        test(Long.MAX_VALUE, "C == '34' || B > '5'");
        test(Long.MAX_VALUE, "C == '34' || B <= '5'");
        test(Long.MAX_VALUE, "C == '34' || B >= '5'");
    }

    @Test
    public void testIntersectionOfNoCardinalityTerms() {
        // i.e. regex and filter function
        test(11L, "A == '11' && filter:includeRegex(B, 'ba.*')");
    }

    @Test
    public void testUnionOfNoCardinalityTerms() {
        // i.e. regex or filter function
        test(11L, "A == '11' || filter:includeRegex(B, 'ba.*')");
    }

    @Test
    public void testIntersectionWithNegatedTerms() {
        test(11L, "A == '11' && !(A == '11')");
        test(11L, "A == '11' && A != '11'");
        test(11L, "A == '11' && A !~ 'ba.*'");
    }

    @Test
    public void testUnionWithNegatedTerms() {
        test(Long.MAX_VALUE, "A == '11' || !(A == '11')");
        test(Long.MAX_VALUE, "A == '11' || A != '11'");
        test(Long.MAX_VALUE, "A == '11' || A !~ 'ba.*'");
    }

    @Test
    public void testIntersectionsWithNullLiterals() {
        test(11L, "A == '11' && A == null");
        test(11L, "A == '11' && A != null");
        test(11L, "A == '11' && !(A == null)");
    }

    @Test
    public void testUnionsWithNullLiterals() {
        test(11L, "A == '11' || A == null");
        test(Long.MAX_VALUE, "A == '11' || A != null");
        test(Long.MAX_VALUE, "A == '11' || !(A == null)");
    }

    @Test
    public void testLongOverflow() {
        CountMap counts = new CountMap();
        counts.put("A == 'max-1'", Long.MAX_VALUE - 1);
        counts.put("B == 'max-1'", Long.MAX_VALUE - 1);

        test(Long.MAX_VALUE, "A == 'max-1' || B == 'max-1'", counts);
        test(Long.MAX_VALUE - 1, "A == 'max-1' && B == 'max-1'", counts);
    }

    private void test(long expected, String query) {
        test(expected, query, getCounts());
    }

    private void test(long expected, String query, CountMap countMap) {
        ASTJexlScript script = parse(query);
        assertEquals(expected, CardinalityVisitor.cardinality(script, countMap));
    }

    private ASTJexlScript parse(String query) {
        try {
            return JexlASTHelper.parseAndFlattenJexlQuery(query);
        } catch (ParseException e) {
            fail("Failed to parse query: " + query);
            throw new RuntimeException(e);
        }
    }

    private CountMap getCounts() {
        if (counts == null) {
            counts = new CountMap();
            counts.put("A == '11'", 11L);
            counts.put("B == '23'", 23L);
            counts.put("C == '34'", 34L);
            counts.put("D == '77'", 77L);
            counts.put("E == '88'", 88L);
        }
        return counts;
    }
}
