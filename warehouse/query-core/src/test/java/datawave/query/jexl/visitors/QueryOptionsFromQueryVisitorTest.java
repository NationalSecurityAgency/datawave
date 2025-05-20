package datawave.query.jexl.visitors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.log4j.Logger;
import org.junit.Test;

import datawave.query.QueryParameters;
import datawave.query.jexl.JexlASTHelper;

public class QueryOptionsFromQueryVisitorTest {

    private static final Logger log = Logger.getLogger(QueryOptionsFromQueryVisitorTest.class);

    private final Map<String,String> optionsMap = new HashMap<>();

    @Test
    public void testOptionsFunction() throws ParseException {
        // Verify that an empty options function adds no parameters.
        assertResult("f:options()", "");
        assertTrue(optionsMap.isEmpty());

        // Verify any specified options are added as separate parameters.
        assertResult("f:options('include.grouping.context','true','hit.list','true','limit.fields','FOO_1_BAR=3,FOO_1=2')", "");
        assertOption("include.grouping.context", "true");
        assertOption("hit.list", "true");
        assertOption("limit.fields", "FOO_1_BAR=3,FOO_1=2");
    }

    @Test
    public void testRenameFunction() throws ParseException {
        // Verify that an empty rename function results in an empty parameter value.
        assertResult("f:rename()", "");
        assertOption(QueryParameters.RENAME_FIELDS, "");

        assertResult("f:rename('field1=field2','field3=field4')", "");
        assertOption(QueryParameters.RENAME_FIELDS, "field1=field2,field3=field4");
    }

    @Test
    public void testGroupByFunction() throws ParseException {
        // Verify that an empty groupby function results in an empty parameter value.
        assertResult("f:groupby()", "");
        assertOption(QueryParameters.GROUP_FIELDS, "");

        assertResult("f:groupby('field1','field2','field3')", "");
        assertOption(QueryParameters.GROUP_FIELDS, "field1,field2,field3");
    }

    @Test
    public void testSumFunction() throws ParseException {
        assertResult("f:sum()", "");
        assertOption(QueryParameters.SUM_FIELDS, "");

        assertResult("f:sum(FIELD)", "");
        assertOption(QueryParameters.SUM_FIELDS, "FIELD");

        assertResult("f:sum(FIELD_A, FIELD_B)", "");
        assertOption(QueryParameters.SUM_FIELDS, "FIELD_A,FIELD_B");
    }

    @Test
    public void testCountFunction() throws ParseException {
        assertResult("f:count()", "");
        assertOption(QueryParameters.COUNT_FIELDS, "");

        assertResult("f:count(FIELD)", "");
        assertOption(QueryParameters.COUNT_FIELDS, "FIELD");

        assertResult("f:count(FIELD_A, FIELD_B)", "");
        assertOption(QueryParameters.COUNT_FIELDS, "FIELD_A,FIELD_B");
    }

    @Test
    public void testMinFunction() throws ParseException {
        assertResult("f:min()", "");
        assertOption(QueryParameters.MIN_FIELDS, "");

        assertResult("f:min(FIELD)", "");
        assertOption(QueryParameters.MIN_FIELDS, "FIELD");

        assertResult("f:min(FIELD_A, FIELD_B)", "");
        assertOption(QueryParameters.MIN_FIELDS, "FIELD_A,FIELD_B");
    }

    @Test
    public void testMaxFunction() throws ParseException {
        assertResult("f:max()", "");
        assertOption(QueryParameters.MAX_FIELDS, "");

        assertResult("f:max(FIELD)", "");
        assertOption(QueryParameters.MAX_FIELDS, "FIELD");

        assertResult("f:max(FIELD_A, FIELD_B)", "");
        assertOption(QueryParameters.MAX_FIELDS, "FIELD_A,FIELD_B");
    }

    @Test
    public void testAverageFunction() throws ParseException {
        assertResult("f:average()", "");
        assertOption(QueryParameters.AVERAGE_FIELDS, "");

        assertResult("f:average(FIELD)", "");
        assertOption(QueryParameters.AVERAGE_FIELDS, "FIELD");

        assertResult("f:average(FIELD_A, FIELD_B)", "");
        assertOption(QueryParameters.AVERAGE_FIELDS, "FIELD_A,FIELD_B");
    }

    @Test
    public void testStrictFunction() throws ParseException {
        assertResult("f:strict()", "");
        assertOption(QueryParameters.STRICT_FIELDS, "");

        assertResult("f:strict(FIELD)", "");
        assertOption(QueryParameters.STRICT_FIELDS, "FIELD");

        assertResult("f:strict(FIELD_A, FIELD_B)", "");
        assertOption(QueryParameters.STRICT_FIELDS, "FIELD_A,FIELD_B");
    }

    @Test
    public void testLenientFunction() throws ParseException {
        assertResult("f:lenient()", "");
        assertOption(QueryParameters.LENIENT_FIELDS, "");

        assertResult("f:lenient(FIELD)", "");
        assertOption(QueryParameters.LENIENT_FIELDS, "FIELD");

        assertResult("f:lenient(FIELD_A, FIELD_B)", "");
        assertOption(QueryParameters.LENIENT_FIELDS, "FIELD_A,FIELD_B");
    }

    @Test
    public void testUniqueFunction() throws ParseException {
        // Verify an empty function results in an empty parameter value.
        assertResult("f:unique_by_day()", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "");
        assertOption(QueryParameters.MOST_RECENT_UNIQUE, null);

        // Verify that fields of no specified granularity are added with the default ALL granularity.
        assertResult("f:unique('field1','field2','field3')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "FIELD1[ALL],FIELD2[ALL],FIELD3[ALL]");
        assertOption(QueryParameters.MOST_RECENT_UNIQUE, null);

        // Verify that fields with DAY granularity are added as such.
        assertResult("f:unique('field1[DAY]','field2[DAY]','field3[DAY]')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "FIELD1[DAY],FIELD2[DAY],FIELD3[DAY]");
        assertOption(QueryParameters.MOST_RECENT_UNIQUE, null);

        // Verify that fields with HOUR granularity are added as such.
        assertResult("f:unique('field1[HOUR]','field2[HOUR]','field3[HOUR]')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "FIELD1[HOUR],FIELD2[HOUR],FIELD3[HOUR]");
        assertOption(QueryParameters.MOST_RECENT_UNIQUE, null);

        // Verify that fields with MINUTE granularity are added as such.
        assertResult("f:unique('field1[MINUTE]','field2[MINUTE]','field3[MINUTE]')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "FIELD1[MINUTE],FIELD2[MINUTE],FIELD3[MINUTE]");
        assertOption(QueryParameters.MOST_RECENT_UNIQUE, null);

        // Verify that fields from multiple unique functions are merged together.
        assertResult("f:unique('field1','field2') AND f:unique('field2[DAY]','field3[DAY]') AND f:unique('field4')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "FIELD1[ALL],FIELD2[ALL,DAY],FIELD3[DAY],FIELD4[ALL]");
        assertOption(QueryParameters.MOST_RECENT_UNIQUE, null);

        // Verify more complex fields with multiple granularity levels are merged together.
        assertResult("f:unique('field1[DAY]','field2[DAY,HOUR]','field3[HOUR,MINUTE]','field4[ALL,MINUTE]','field5')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "FIELD1[DAY],FIELD2[DAY,HOUR],FIELD3[HOUR,MINUTE],FIELD4[ALL,MINUTE],FIELD5[ALL]");
        assertOption(QueryParameters.MOST_RECENT_UNIQUE, null);

        // Lucene will parse comma-delimited granularity levels into separate strings. Ensure it still parses correctly.
        assertResult("f:unique('field1[DAY]','field2[DAY','HOUR]','field3[HOUR','MINUTE]','field4[ALL','MINUTE]','field5')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "FIELD1[DAY],FIELD2[DAY,HOUR],FIELD3[HOUR,MINUTE],FIELD4[ALL,MINUTE],FIELD5[ALL]");
        assertOption(QueryParameters.MOST_RECENT_UNIQUE, null);
    }

    @Test
    public void testMostRecentUniqueFunction() throws ParseException {
        // Verify an empty function results in an empty parameter value.
        assertResult("f:most_recent_unique_by_day()", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "");
        assertOption(QueryParameters.MOST_RECENT_UNIQUE, "true");

        // Verify that fields of no specified granularity are added with the default ALL granularity.
        assertResult("f:most_recent_unique('field1','field2','field3')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "FIELD1[ALL],FIELD2[ALL],FIELD3[ALL]");
        assertOption(QueryParameters.MOST_RECENT_UNIQUE, "true");

        // Verify that fields with DAY granularity are added as such.
        assertResult("f:most_recent_unique('field1[DAY]','field2[DAY]','field3[DAY]')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "FIELD1[DAY],FIELD2[DAY],FIELD3[DAY]");
        assertOption(QueryParameters.MOST_RECENT_UNIQUE, "true");

        // Verify that fields with HOUR granularity are added as such.
        assertResult("f:most_recent_unique('field1[HOUR]','field2[HOUR]','field3[HOUR]')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "FIELD1[HOUR],FIELD2[HOUR],FIELD3[HOUR]");
        assertOption(QueryParameters.MOST_RECENT_UNIQUE, "true");

        // Verify that fields with MINUTE granularity are added as such.
        assertResult("f:most_recent_unique('field1[MINUTE]','field2[MINUTE]','field3[MINUTE]')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "FIELD1[MINUTE],FIELD2[MINUTE],FIELD3[MINUTE]");
        assertOption(QueryParameters.MOST_RECENT_UNIQUE, "true");

        // Verify that fields from multiple unique functions are merged together.
        assertResult("f:most_recent_unique('field1','field2') AND f:unique('field2[DAY]','field3[DAY]') AND f:unique('field4')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "FIELD1[ALL],FIELD2[ALL,DAY],FIELD3[DAY],FIELD4[ALL]");
        assertOption(QueryParameters.MOST_RECENT_UNIQUE, "true");

        // Verify more complex fields with multiple granularity levels are merged together.
        assertResult("f:most_recent_unique('field1[DAY]','field2[DAY,HOUR]','field3[HOUR,MINUTE]','field4[ALL,MINUTE]','field5')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "FIELD1[DAY],FIELD2[DAY,HOUR],FIELD3[HOUR,MINUTE],FIELD4[ALL,MINUTE],FIELD5[ALL]");
        assertOption(QueryParameters.MOST_RECENT_UNIQUE, "true");

        // Lucene will parse comma-delimited granularity levels into separate strings. Ensure it still parses correctly.
        assertResult("f:most_recent_unique('field1[DAY]','field2[DAY','HOUR]','field3[HOUR','MINUTE]','field4[ALL','MINUTE]','field5')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "FIELD1[DAY],FIELD2[DAY,HOUR],FIELD3[HOUR,MINUTE],FIELD4[ALL,MINUTE],FIELD5[ALL]");
        assertOption(QueryParameters.MOST_RECENT_UNIQUE, "true");
    }

    @Test
    public void testUniqueByDay() throws ParseException {
        // Verify an empty function results in an empty unique parameter.
        assertResult("f:unique_by_day()", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "");

        // Verify fields are added with the DAY granularity.
        assertResult("f:unique_by_day('field1','field2','field3')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "FIELD1[DAY],FIELD2[DAY],FIELD3[DAY]");

        // Verify fields from multiple functions are merged.
        assertResult("f:unique('field1','field2[HOUR]') AND f:unique_by_day('field1','field2','field3') AND f:unique_by_day('field4')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "FIELD1[ALL,DAY],FIELD2[DAY,HOUR],FIELD3[DAY],FIELD4[DAY]");
    }

    @Test
    public void testUniqueByHour() throws ParseException {
        // Verify an empty function results in an empty unique parameter.
        assertResult("f:unique_by_hour()", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "");

        // Verify fields are added with the HOUR granularity.
        assertResult("f:unique_by_hour('field1','field2','field3')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "FIELD1[HOUR],FIELD2[HOUR],FIELD3[HOUR]");

        // Verify fields from multiple functions are merged.
        assertResult("f:unique('field1','field2[DAY]') AND f:unique_by_hour('field1','field2','field3') AND f:unique_by_hour('field4')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "FIELD1[ALL,HOUR],FIELD2[DAY,HOUR],FIELD3[HOUR],FIELD4[HOUR]");
    }

    @Test
    public void testUniqueByMonth() throws ParseException {
        // Verify an empty function results in an empty unique parameter.
        assertResult("f:unique_by_month()", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "");

        // Verify fields are added with the HOUR granularity.
        assertResult("f:unique_by_month('field1','field2','field3')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "FIELD1[MONTH],FIELD2[MONTH],FIELD3[MONTH]");

        // Verify fields from multiple functions are merged.
        assertResult("f:unique('field1','field2[DAY]') AND f:unique_by_month('field1','field2','field3') AND f:unique_by_month('field4')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "FIELD1[ALL,MONTH],FIELD2[DAY,MONTH],FIELD3[MONTH],FIELD4[MONTH]");
    }

    @Test
    public void testUniqueBySecond() throws ParseException {
        // Verify an empty function results in an empty unique parameter.
        assertResult("f:unique_by_second()", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "");

        // Verify fields are added with the HOUR granularity.
        assertResult("f:unique_by_second('field1','field2','field3')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "FIELD1[SECOND],FIELD2[SECOND],FIELD3[SECOND]");

        // Verify fields from multiple functions are merged.
        assertResult("f:unique('field1','field2[DAY]') AND f:unique_by_second('field1','field2','field3') AND f:unique_by_second('field4')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "FIELD1[ALL,SECOND],FIELD2[DAY,SECOND],FIELD3[SECOND],FIELD4[SECOND]");
    }

    @Test
    public void testUniqueByMillisecond() throws ParseException {
        // Verify an empty function results in an empty unique parameter.
        assertResult("f:unique_by_millisecond()", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "");

        // Verify fields are added with the HOUR granularity.
        assertResult("f:unique_by_millisecond('field1','field2','field3')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "FIELD1[MILLISECOND],FIELD2[MILLISECOND],FIELD3[MILLISECOND]");

        // Verify fields from multiple functions are merged.
        assertResult("f:unique('field1','field2[DAY]') AND f:unique_by_millisecond('field1','field2','field3') AND f:unique_by_millisecond('field4')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "FIELD1[ALL,MILLISECOND],FIELD2[DAY,MILLISECOND],FIELD3[MILLISECOND],FIELD4[MILLISECOND]");
    }

    @Test
    public void testUniqueByYear() throws ParseException {
        // Verify an empty function results in an empty unique parameter.
        assertResult("f:unique_by_year()", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "");

        // Verify fields are added with the MINUTE granularity.
        assertResult("f:unique_by_year('field1','field2','field3')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "FIELD1[YEAR],FIELD2[YEAR],FIELD3[YEAR]");

        // Verify fields from multiple functions are merged.
        assertResult("f:unique('field1','field2[DAY]') AND f:unique_by_year('field1','field2','field3') AND f:unique_by_year('field4')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "FIELD1[ALL,YEAR],FIELD2[DAY,YEAR],FIELD3[YEAR],FIELD4[YEAR]");
    }

    @Test
    public void testUniqueByMinute() throws ParseException {
        // Verify an empty function results in an empty unique parameter.
        assertResult("f:unique_by_minute()", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "");

        // Verify fields are added with the MINUTE granularity.
        assertResult("f:unique_by_minute('field1','field2','field3')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "FIELD1[MINUTE],FIELD2[MINUTE],FIELD3[MINUTE]");

        // Verify fields from multiple functions are merged.
        assertResult("f:unique('field1','field2[DAY]') AND f:unique_by_minute('field1','field2','field3') AND f:unique_by_minute('field4')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "FIELD1[ALL,MINUTE],FIELD2[DAY,MINUTE],FIELD3[MINUTE],FIELD4[MINUTE]");
    }

    @Test
    public void testUniqueByTenth() throws ParseException {
        // Verify an empty function results in an empty unique parameter.
        assertResult("f:unique_by_tenth_of_hour()", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "");

        // Verify fields are added with the MINUTE granularity.
        assertResult("f:unique_by_tenth_of_hour('field1','field2','field3')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "FIELD1[TENTH_OF_HOUR],FIELD2[TENTH_OF_HOUR],FIELD3[TENTH_OF_HOUR]");

        // Verify fields from multiple functions are merged.
        assertResult("f:unique('field1','field2[DAY]') AND f:unique_by_tenth_of_hour('field1','field2','field3') AND f:unique_by_tenth_of_hour('field4')", "");
        assertOption(QueryParameters.UNIQUE_FIELDS, "FIELD1[ALL,TENTH_OF_HOUR],FIELD2[DAY,TENTH_OF_HOUR],FIELD3[TENTH_OF_HOUR],FIELD4[TENTH_OF_HOUR]");
    }

    @Test
    public void testNonFunctionNodesWithJunctions() throws ParseException {
        // Verify that only the function node is removed.
        assertResult("f:unique_by_minute('field1') AND FOO == 'bar'", "FOO == 'bar'");
        assertOption(QueryParameters.UNIQUE_FIELDS, "FIELD1[MINUTE]");

        // Verify that only the function node is removed.
        assertResult("f:unique_by_minute('field1') AND (FOO == 'bar' AND BAT == 'foo')", "(FOO == 'bar' AND BAT == 'foo')");
        assertOption(QueryParameters.UNIQUE_FIELDS, "FIELD1[MINUTE]");

        // Verify that only the function node is removed.
        assertResult("f:unique_by_minute('field1') OR FOO == 'bar'", "FOO == 'bar'");
        assertOption(QueryParameters.UNIQUE_FIELDS, "FIELD1[MINUTE]");

        // Verify that AND nodes are cleaned up.
        assertResult("(FOO == 'bar' OR (BAR == 'foo' AND f:groupby('field1','field2')))", "(FOO == 'bar' OR (BAR == 'foo'))");
        assertOption(QueryParameters.GROUP_FIELDS, "field1,field2");

        // Verify that OR nodes are cleaned up.
        assertResult("(FOO == 'bar' AND (BAR == 'foo' OR f:groupby('field1','field2')))", "(FOO == 'bar' AND (BAR == 'foo'))");
        assertOption(QueryParameters.GROUP_FIELDS, "field1,field2");
    }

    private void assertOption(String option, String value) {
        assertEquals(value, optionsMap.get(option));
    }

    private void assertResult(String original, String expected) throws ParseException {
        optionsMap.clear();

        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(expected);
        ASTJexlScript originalScript = JexlASTHelper.parseJexlQuery(original);

        ASTJexlScript result = QueryOptionsFromQueryVisitor.collect(originalScript, optionsMap);

        assertScriptEquality(result, expected);
        assertLineage(result);

        assertScriptEquality(originalScript, original);
        assertLineage(originalScript);

        assertTrue(TreeEqualityVisitor.isEqual(expectedScript, result));
    }

    private void assertScriptEquality(ASTJexlScript actual, String expected) throws ParseException {
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(expected);
        TreeEqualityVisitor.Comparison comparison = TreeEqualityVisitor.checkEquality(expectedScript, actual);
        if (!comparison.isEqual()) {
            log.error("Expected " + PrintingVisitor.formattedQueryString(expected));
            log.error("Actual " + PrintingVisitor.formattedQueryString(actual));
        }
        assertTrue(comparison.getReason(), comparison.isEqual());
    }

    private void assertLineage(JexlNode node) {
        assertTrue(JexlASTHelper.validateLineage(node, true));
    }
}
