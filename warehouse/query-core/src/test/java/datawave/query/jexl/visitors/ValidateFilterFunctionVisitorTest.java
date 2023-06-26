package datawave.query.jexl.visitors;

import com.google.common.collect.Sets;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ValidateFilterFunctionVisitorTest {

    private final Set<String> indexOnlyFields = Sets.newHashSet("INDEX_ONLY", "INDEX_ONLY2");

    @Test
    public void testExcludeRegex() {
        test("filter:excludeRegex(FOO,'ba.*')", false);
        test("filter:excludeRegex(INDEX_ONLY,'ba.*')", true);
        test("FOO == 'bar' && filter:excludeRegex(FOO,'ba.*')", false);
        test("FOO == 'bar' && filter:excludeRegex(INDEX_ONLY,'ba.*')", true);
        test("FOO == 'bar' || filter:excludeRegex(FOO,'ba.*')", false);
        test("FOO == 'bar' || filter:excludeRegex(INDEX_ONLY,'ba.*')", true);
    }

    @Test
    public void testIncludeRegex() {
        test("filter:includeRegex(FOO,'ba.*')", false);
        test("filter:includeRegex(INDEX_ONLY,'ba.*')", true);
        test("FOO == 'bar' && filter:includeRegex(FOO,'ba.*')", false);
        test("FOO == 'bar' && filter:includeRegex(INDEX_ONLY,'ba.*')", true);
        test("FOO == 'bar' || filter:includeRegex(FOO,'ba.*')", false);
        test("FOO == 'bar' || filter:includeRegex(INDEX_ONLY,'ba.*')", true);
    }

    @Test
    public void testIncludeText() {
        // includeText function is allowed to have index-only fields
        test("f:includeText(FOO, 'bar')", false);
        test("f:includeText(INDEX_ONLY, 'bar')", false);

        test("FOO == 'bar' && f:includeText(FOO, 'bar')", false);
        test("FOO == 'bar' && f:includeText(INDEX_ONLY, 'bar')", false);

        test("FOO == 'bar' || f:includeText(FOO, 'bar')", false);
        test("FOO == 'bar' || f:includeText(INDEX_ONLY, 'bar')", false);
    }

    @Test
    public void testIsNull() {
        test("filter:isNull(FOO)", false);
        test("filter:isNull(INDEX_ONLY)", true);
        test("filter:isNull(FOO||INDEX_ONLY)", true);
        test("filter:isNull(INDEX_ONLY||FOO)", true);
        test("filter:isNull((FOO||INDEX_ONLY))", true);
        test("filter:isNull((INDEX_ONLY||FOO))", true);
        // intersection
        test("FOO == 'bar' && filter:isNull(FOO)", false);
        test("FOO == 'bar' && filter:isNull(INDEX_ONLY)", true);
        test("FOO == 'bar' && filter:isNull(FOO||INDEX_ONLY)", true);
        test("FOO == 'bar' && filter:isNull(INDEX_ONLY||FOO)", true);
        test("FOO == 'bar' && filter:isNull((FOO||INDEX_ONLY))", true);
        test("FOO == 'bar' && filter:isNull((INDEX_ONLY||FOO))", true);
        // union
        test("FOO == 'bar' || filter:isNull(FOO)", false);
        test("FOO == 'bar' || filter:isNull(INDEX_ONLY)", true);
        test("FOO == 'bar' || filter:isNull(FOO||INDEX_ONLY)", true);
        test("FOO == 'bar' || filter:isNull(INDEX_ONLY||FOO)", true);
        test("FOO == 'bar' || filter:isNull((FOO||INDEX_ONLY))", true);
        test("FOO == 'bar' || filter:isNull((INDEX_ONLY||FOO))", true);
    }

    // a date field shouldn't be index-only, but check anyway
    @Test
    public void testBetweenDates() {
        test("filter:betweenDates(FOO, '20140101', '20140102')", false);
        test("filter:betweenDates(FOO, '20140101_200000', '20140102_210000')", false);
        test("filter:betweenDates(FOO, '20140101', '20140102', 'yyyyMMdd')", false);
        test("filter:betweenDates(FOO, 'yyyyMMddHHmmss', '20140101', '20140102', 'yyyyMMdd')", false);
        test("filter:betweenDates(INDEX_ONLY, '20140101', '20140102')", true);
        test("filter:betweenDates(INDEX_ONLY, '20140101_200000', '20140102_210000')", true);
        test("filter:betweenDates(INDEX_ONLY, '20140101', '20140102', 'yyyyMMdd')", true);
        test("filter:betweenDates(INDEX_ONLY, 'yyyyMMddHHmmss', '20140101', '20140102', 'yyyyMMdd')", true);
        // intersection
        test("FOO == 'bar' && filter:betweenDates(FOO, '20140101', '20140102')", false);
        test("FOO == 'bar' && filter:betweenDates(FOO, '20140101_200000', '20140102_210000')", false);
        test("FOO == 'bar' && filter:betweenDates(FOO, '20140101', '20140102', 'yyyyMMdd')", false);
        test("FOO == 'bar' && filter:betweenDates(FOO, 'yyyyMMddHHmmss', '20140101', '20140102', 'yyyyMMdd')", false);
        test("FOO == 'bar' && filter:betweenDates(INDEX_ONLY, '20140101', '20140102')", true);
        test("FOO == 'bar' && filter:betweenDates(INDEX_ONLY, '20140101_200000', '20140102_210000')", true);
        test("FOO == 'bar' && filter:betweenDates(INDEX_ONLY, '20140101', '20140102', 'yyyyMMdd')", true);
        test("FOO == 'bar' && filter:betweenDates(INDEX_ONLY, 'yyyyMMddHHmmss', '20140101', '20140102', 'yyyyMMdd')", true);
        // union
        test("FOO == 'bar' || filter:betweenDates(FOO, '20140101', '20140102')", false);
        test("FOO == 'bar' || filter:betweenDates(FOO, '20140101_200000', '20140102_210000')", false);
        test("FOO == 'bar' || filter:betweenDates(FOO, '20140101', '20140102', 'yyyyMMdd')", false);
        test("FOO == 'bar' || filter:betweenDates(FOO, 'yyyyMMddHHmmss', '20140101', '20140102', 'yyyyMMdd')", false);
        test("FOO == 'bar' || filter:betweenDates(INDEX_ONLY, '20140101', '20140102')", true);
        test("FOO == 'bar' || filter:betweenDates(INDEX_ONLY, '20140101_200000', '20140102_210000')", true);
        test("FOO == 'bar' || filter:betweenDates(INDEX_ONLY, '20140101', '20140102', 'yyyyMMdd')", true);
        test("FOO == 'bar' || filter:betweenDates(INDEX_ONLY, 'yyyyMMddHHmmss', '20140101', '20140102', 'yyyyMMdd')", true);
    }

    @Test
    public void testBeforeDate() {
        test("filter:beforeDate(FOO, '20140101')", false);
        test("filter:beforeDate(FOO, '20140101', 'yyyyMMdd')", false);
        test("filter:beforeDate(INDEX_ONLY, '20140101')", true);
        test("filter:beforeDate(INDEX_ONLY, '20140101', 'yyyyMMdd')", true);
        // intersection
        test("FOO == 'bar' && filter:beforeDate(FOO, '20140101')", false);
        test("FOO == 'bar' && filter:beforeDate(FOO, '20140101', 'yyyyMMdd')", false);
        test("FOO == 'bar' && filter:beforeDate(INDEX_ONLY, '20140101')", true);
        test("FOO == 'bar' && filter:beforeDate(INDEX_ONLY, '20140101', 'yyyyMMdd')", true);
        // union
        test("FOO == 'bar' || filter:beforeDate(FOO, '20140101')", false);
        test("FOO == 'bar' || filter:beforeDate(FOO, '20140101', 'yyyyMMdd')", false);
        test("FOO == 'bar' || filter:beforeDate(INDEX_ONLY, '20140101')", true);
        test("FOO == 'bar' || filter:beforeDate(INDEX_ONLY, '20140101', 'yyyyMMdd')", true);
    }

    @Test
    public void testAfterDate() {
        test("filter:afterDate(FOO, '20140101')", false);
        test("filter:afterDate(FOO, '20140101', 'yyyyMMdd')", false);
        test("filter:afterDate(INDEX_ONLY, '20140101')", true);
        test("filter:afterDate(INDEX_ONLY, '20140101', 'yyyyMMdd')", true);
        // intersection
        test("FOO == 'bar' && filter:afterDate(FOO, '20140101')", false);
        test("FOO == 'bar' && filter:afterDate(FOO, '20140101', 'yyyyMMdd')", false);
        test("FOO == 'bar' && filter:afterDate(INDEX_ONLY, '20140101')", true);
        test("FOO == 'bar' && filter:afterDate(INDEX_ONLY, '20140101', 'yyyyMMdd')", true);
        // union
        test("FOO == 'bar' || filter:afterDate(FOO, '20140101')", false);
        test("FOO == 'bar' || filter:afterDate(FOO, '20140101', 'yyyyMMdd')", false);
        test("FOO == 'bar' || filter:afterDate(INDEX_ONLY, '20140101')", true);
        test("FOO == 'bar' || filter:afterDate(INDEX_ONLY, '20140101', 'yyyyMMdd')", true);
    }

    @Test
    public void testBetweenLoadDates() {
        test("filter:betweenLoadDates(FOO, '20140101', '20140102')", false);
        test("filter:betweenLoadDates(FOO, '20140101_200000', '20140102_210000')", false);
        test("filter:betweenLoadDates(FOO, '20140101', '20140102', 'yyyyMMdd')", false);
        test("filter:betweenLoadDates(FOO, 'yyyyMMddHHmmss', '20140101', '20140102', 'yyyyMMdd')", false);
        test("filter:betweenLoadDates(INDEX_ONLY, '20140101', '20140102')", true);
        test("filter:betweenLoadDates(INDEX_ONLY, '20140101_200000', '20140102_210000')", true);
        test("filter:betweenLoadDates(INDEX_ONLY, '20140101', '20140102', 'yyyyMMdd')", true);
        test("filter:betweenLoadDates(INDEX_ONLY, 'yyyyMMddHHmmss', '20140101', '20140102', 'yyyyMMdd')", true);
        // intersection
        test("FOO == 'bar' && filter:betweenLoadDates(FOO, '20140101', '20140102')", false);
        test("FOO == 'bar' && filter:betweenLoadDates(FOO, '20140101_200000', '20140102_210000')", false);
        test("FOO == 'bar' && filter:betweenLoadDates(FOO, '20140101', '20140102', 'yyyyMMdd')", false);
        test("FOO == 'bar' && filter:betweenLoadDates(FOO, 'yyyyMMddHHmmss', '20140101', '20140102', 'yyyyMMdd')", false);
        test("FOO == 'bar' && filter:betweenLoadDates(INDEX_ONLY, '20140101', '20140102')", true);
        test("FOO == 'bar' && filter:betweenLoadDates(INDEX_ONLY, '20140101_200000', '20140102_210000')", true);
        test("FOO == 'bar' && filter:betweenLoadDates(INDEX_ONLY, '20140101', '20140102', 'yyyyMMdd')", true);
        test("FOO == 'bar' && filter:betweenLoadDates(INDEX_ONLY, 'yyyyMMddHHmmss', '20140101', '20140102', 'yyyyMMdd')", true);
        // union
        test("FOO == 'bar' || filter:betweenLoadDates(FOO, '20140101', '20140102')", false);
        test("FOO == 'bar' || filter:betweenLoadDates(FOO, '20140101_200000', '20140102_210000')", false);
        test("FOO == 'bar' || filter:betweenLoadDates(FOO, '20140101', '20140102', 'yyyyMMdd')", false);
        test("FOO == 'bar' || filter:betweenLoadDates(FOO, 'yyyyMMddHHmmss', '20140101', '20140102', 'yyyyMMdd')", false);
        test("FOO == 'bar' || filter:betweenLoadDates(INDEX_ONLY, '20140101', '20140102')", true);
        test("FOO == 'bar' || filter:betweenLoadDates(INDEX_ONLY, '20140101_200000', '20140102_210000')", true);
        test("FOO == 'bar' || filter:betweenLoadDates(INDEX_ONLY, '20140101', '20140102', 'yyyyMMdd')", true);
        test("FOO == 'bar' || filter:betweenLoadDates(INDEX_ONLY, 'yyyyMMddHHmmss', '20140101', '20140102', 'yyyyMMdd')", true);
    }

    @Test
    public void testBeforeLoadDate() {
        test("filter:beforeLoadDate(FOO, '20140101')", false);
        test("filter:beforeLoadDate(FOO, '20140101', 'yyyyMMdd')", false);
        test("filter:beforeLoadDate(INDEX_ONLY, '20140101')", true);
        test("filter:beforeLoadDate(INDEX_ONLY, '20140101', 'yyyyMMdd')", true);
        // intersection
        test("FOO == 'bar' && filter:beforeLoadDate(FOO, '20140101')", false);
        test("FOO == 'bar' && filter:beforeLoadDate(FOO, '20140101', 'yyyyMMdd')", false);
        test("FOO == 'bar' && filter:beforeLoadDate(INDEX_ONLY, '20140101')", true);
        test("FOO == 'bar' && filter:beforeLoadDate(INDEX_ONLY, '20140101', 'yyyyMMdd')", true);
        // union
        test("FOO == 'bar' || filter:beforeLoadDate(FOO, '20140101')", false);
        test("FOO == 'bar' || filter:beforeLoadDate(FOO, '20140101', 'yyyyMMdd')", false);
        test("FOO == 'bar' || filter:beforeLoadDate(INDEX_ONLY, '20140101')", true);
        test("FOO == 'bar' || filter:beforeLoadDate(INDEX_ONLY, '20140101', 'yyyyMMdd')", true);
    }

    @Test
    public void testAfterLoadDate() {
        test("filter:afterLoadDate(FOO, '20140101')", false);
        test("filter:afterLoadDate(FOO, '20140101', 'yyyyMMdd')", false);
        test("filter:afterLoadDate(INDEX_ONLY, '20140101')", true);
        test("filter:afterLoadDate(INDEX_ONLY, '20140101', 'yyyyMMdd')", true);
        // intersection
        test("FOO == 'bar' && filter:afterLoadDate(FOO, '20140101')", false);
        test("FOO == 'bar' && filter:afterLoadDate(FOO, '20140101', 'yyyyMMdd')", false);
        test("FOO == 'bar' && filter:afterLoadDate(INDEX_ONLY, '20140101')", true);
        test("FOO == 'bar' && filter:afterLoadDate(INDEX_ONLY, '20140101', 'yyyyMMdd')", true);
        // union
        test("FOO == 'bar' || filter:afterLoadDate(FOO, '20140101')", false);
        test("FOO == 'bar' || filter:afterLoadDate(FOO, '20140101', 'yyyyMMdd')", false);
        test("FOO == 'bar' || filter:afterLoadDate(INDEX_ONLY, '20140101')", true);
        test("FOO == 'bar' || filter:afterLoadDate(INDEX_ONLY, '20140101', 'yyyyMMdd')", true);
    }

    @Test
    public void testMatchesAtLeastCountOf() {
        test("filter:matchesAtLeastCountOf(7, FOO, 'v1', 'v2')", false);
        test("filter:matchesAtLeastCountOf(7, INDEX_ONLY, 'v1', 'v2')", true);

        test("FOO == 'bar' && filter:matchesAtLeastCountOf(7, FOO, 'v1', 'v2')", false);
        test("FOO == 'bar' && filter:matchesAtLeastCountOf(7, INDEX_ONLY, 'v1', 'v2')", true);

        test("FOO == 'bar' || filter:matchesAtLeastCountOf(7, FOO, 'v1', 'v2')", false);
        test("FOO == 'bar' || filter:matchesAtLeastCountOf(7, INDEX_ONLY, 'v1', 'v2')", true);
    }

    // date field shouldn't be index-only, but test for the case anyway
    @Test
    public void testTimeFunction() {
        test("filter:timeFunction(DEATH_DATE,BIRTH_DATE,'-','>',2522880000000L)", false);
        test("filter:timeFunction(INDEX_ONLY,BIRTH_DATE,'-','>',2522880000000L)", true);
        test("filter:timeFunction(DEATH_DATE,INDEX_ONLY,'-','>',2522880000000L)", true);

        test("FOO == 'bar' && filter:timeFunction(DEATH_DATE,BIRTH_DATE,'-','>',2522880000000L)", false);
        test("FOO == 'bar' && filter:timeFunction(INDEX_ONLY,BIRTH_DATE,'-','>',2522880000000L)", true);
        test("FOO == 'bar' && filter:timeFunction(DEATH_DATE,INDEX_ONLY,'-','>',2522880000000L)", true);

        test("FOO == 'bar' || filter:timeFunction(DEATH_DATE,BIRTH_DATE,'-','>',2522880000000L)", false);
        test("FOO == 'bar' || filter:timeFunction(INDEX_ONLY,BIRTH_DATE,'-','>',2522880000000L)", true);
        test("FOO == 'bar' || filter:timeFunction(DEATH_DATE,INDEX_ONLY,'-','>',2522880000000L)", true);
    }

    @Test
    public void testGetMinTimeAndOtherLogicalOperators() {
        test("filter:getMaxTime(DEATH_DATE) < 123456L", false);
        test("filter:getMaxTime(INDEX_ONLY) < 123456L", true);

        test("filter:getMaxTime(DEATH_DATE) <= 123456L", false);
        test("filter:getMaxTime(INDEX_ONLY) <= 123456L", true);

        test("filter:getMaxTime(DEATH_DATE) > 123456L", false);
        test("filter:getMaxTime(INDEX_ONLY) > 123456L", true);

        test("filter:getMaxTime(DEATH_DATE) >= 123456L", false);
        test("filter:getMaxTime(INDEX_ONLY) >= 123456L", true);

        test("filter:getMaxTime(DEATH_DATE) - 123456L", false);
        test("filter:getMaxTime(INDEX_ONLY) - 123456L", true);

        test("filter:getMaxTime(DEATH_DATE) + 123456L", false);
        test("filter:getMaxTime(INDEX_ONLY) + 123456L", true);

        test("filter:getMaxTime(DEATH_DATE) / 123456L", false);
        test("filter:getMaxTime(INDEX_ONLY) / 123456L", true);

        test("filter:getMaxTime(DEATH_DATE) * 123456L", false);
        test("filter:getMaxTime(INDEX_ONLY) * 123456L", true);

        test("filter:getMaxTime(DEATH_DATE) % 123456L", false);
        test("filter:getMaxTime(INDEX_ONLY) % 123456L", true);

        test("filter:getMaxTime(DEATH_DATE).size()", false);
        test("filter:getMaxTime(INDEX_ONLY).size()", true);
        test("filter:getMaxTime(DEATH_DATE).size() == true", false);
        test("filter:getMaxTime(INDEX_ONLY).size() == true", true);
    }

    @Test
    public void testGetMaxTime() {
        test("filter:getMaxTime(DEATH_DATE) > 123456L", false);
        test("filter:getMaxTime(INDEX_ONLY) > 123456L", true);
    }

    @Test
    public void testCompare() {
        test("filter:compare(FOO,'==','all',BAR)", false);
        test("filter:compare(FOO,'=','any',BAR)", false);
        test("filter:compare(FOO,'<','all',BAR)", false);
        test("filter:compare(FOO,'>','all',BAR)", false);

        test("filter:compare(FOO,'==','all',INDEX_ONLY)", true);
        test("filter:compare(INDEX_ONLY,'=','any',BAR)", true);
        test("filter:compare(INDEX_ONLY,'<','all',INDEX_ONLY2)", true);

        test("FOO == 'bar' && filter:compare(FOO,'==','all',INDEX_ONLY)", true);
        test("FOO == 'bar' && filter:compare(INDEX_ONLY,'=','any',BAR)", true);
        test("FOO == 'bar' && filter:compare(INDEX_ONLY,'<','all',INDEX_ONLY2)", true);

        test("FOO == 'bar' || filter:compare(FOO,'==','all',INDEX_ONLY)", true);
        test("FOO == 'bar' || filter:compare(INDEX_ONLY,'=','any',BAR)", true);
        test("FOO == 'bar' || filter:compare(INDEX_ONLY,'<','all',INDEX_ONLY2)", true);
    }

    @Test
    public void testNoExpansion() {
        test("FOO == 'bar' && filter:noExpansion(FOO)", false);
        test("FOO == 'bar' && filter:noExpansion(INDEX_ONLY)", false);
    }

    @Test
    public void testOccurrence() {
        test("filter:occurrence(FOO, '==', 1)", false);
        test("filter:occurrence(INDEX_ONLY, '==', 1)", true);
    }

    @Test
    public void testFunctionsAsArgs() {
        test("filter:occurrence(FOO, '==', filter:includeText(FOO2, 'ba.*').size())", false);
        test("filter:occurrence(INDEX_ONLY, '==', filter:includeText(FOO2, 'ba.*').size())", true);
        // this is the case we care about -- a nested function coming in as an argument to another function
        test("filter:occurrence(FOO, '==', filter:includeText(INDEX_ONLY, 'ba.*').size())", true);
    }

    @Test
    public void testMultipleFunctions() {
        // for when you have multiple filter functions in a query
        test("filter:includeRegex(FOO, 'ba.*') && filter:occurrence(FOO, '==', 1)", false);
        test("filter:includeRegex(INDEX_ONLY, 'ba.*') && filter:occurrence(FOO, '==', 1)", true);
        test("filter:includeRegex(FOO, 'ba.*') && filter:occurrence(INDEX_ONLY, '==', 1)", true);
    }

    @Test
    public void testFunctionInsideQueryPropertyMarker() {
        test("((_Eval_ = true) && (filter:includeRegex(INDEX_ONLY, 'ba.*')))", true);
        test("((_Delayed_ = true) && (filter:includeRegex(INDEX_ONLY, 'ba.*')))", true);
    }

    private void test(String query, boolean exceptionExpected) {
        try {
            ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
            ValidateFilterFunctionVisitor.validate(script, indexOnlyFields);

            if (exceptionExpected) {
                fail("Expected failure for query: " + query);
            }
        } catch (DatawaveFatalQueryException e) {
            assertTrue("Received an unexpected exception when validating query: " + query, exceptionExpected);
        } catch (ParseException e) {
            fail("Failed to parse query: " + query);
        }
    }

}
