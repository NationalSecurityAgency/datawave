package datawave.query.jexl.visitors;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Sets;

import datawave.data.type.LcNoDiacriticsType;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.MockMetadataHelper;

public class QueryFieldsVisitorTest {

    private static final MockMetadataHelper helper = new MockMetadataHelper();

    @BeforeClass
    public static void setup() {
        helper.addNormalizers("FOO", Collections.singleton(new LcNoDiacriticsType()));
        helper.addNormalizers("FOO2", Collections.singleton(new LcNoDiacriticsType()));
        helper.addNormalizers("FOO3", Collections.singleton(new LcNoDiacriticsType()));
        helper.addNormalizers("FOO4", Collections.singleton(new LcNoDiacriticsType()));
    }

    @Test
    public void testEQ() throws ParseException {
        String query = "FOO == 'bar'";
        test(query, Collections.singleton("FOO"));

        // identifiers
        query = "$12 == 'bar'";
        test(query, Collections.singleton("12"));
    }

    @Test
    public void testNE() throws ParseException {
        String query = "FOO != 'bar'";
        test(query, Collections.singleton("FOO"));
    }

    @Test
    public void testLT() throws ParseException {
        String query = "FOO < 'bar'";
        test(query, Collections.singleton("FOO"));
    }

    @Test
    public void testGT() throws ParseException {
        String query = "FOO > 'bar'";
        test(query, Collections.singleton("FOO"));
    }

    @Test
    public void testLE() throws ParseException {
        String query = "FOO <= 'bar'";
        test(query, Collections.singleton("FOO"));
    }

    @Test
    public void testGE() throws ParseException {
        String query = "FOO >= 'bar'";
        test(query, Collections.singleton("FOO"));
    }

    @Test
    public void testER() throws ParseException {
        String query = "FOO =~ 'ba.*'";
        test(query, Collections.singleton("FOO"));
    }

    @Test
    public void testNR() throws ParseException {
        String query = "FOO !~ 'ba.*'";
        test(query, Collections.singleton("FOO"));
    }

    @Test
    public void testMultiFieldedUnion() throws ParseException {
        String query = "FOO == 'bar' || FOO2 == 'baz'";
        test(query, Sets.newHashSet("FOO", "FOO2"));
    }

    @Test
    public void testMultiFieldedIntersection() throws ParseException {
        String query = "FOO == 'bar' && FOO2 == 'baz'";
        test(query, Sets.newHashSet("FOO", "FOO2"));
    }

    @Test
    public void testIntersectionOfNestedUnions() throws ParseException {
        String query = "(FOO == 'bar' || FOO2 == 'baz') && (FOO3 == 'xray' || FOO4 == 'zed')";
        test(query, Sets.newHashSet("FOO", "FOO2", "FOO3", "FOO4"));
    }

    @Test
    public void testUnionOfNestedIntersection() throws ParseException {
        String query = "(FOO == 'tank' && FOO2 == 'man') || (FOO3 == 'thanks ' && FOO4 == 'man')";
        test(query, Sets.newHashSet("FOO", "FOO2", "FOO3", "FOO4"));
    }

    // Query functions

    @Test
    public void testGroupByFunction() throws ParseException {
        String query = "f:groupby(FOO,BAR)";
        test(query, Sets.newHashSet("FOO", "BAR"));
    }

    @Test
    public void testNoExpansionFunction() throws ParseException {
        String query = "f:noExpansion(FOO,BAR)";
        test(query, Sets.newHashSet("FOO", "BAR"));
    }

    @Test
    public void testLenientFunction() throws ParseException {
        String query = "f:lenient(FOO,BAR)";
        test(query, Sets.newHashSet("FOO", "BAR"));
    }

    @Test
    public void testStrictFunction() throws ParseException {
        String query = "f:strict(FOO,BAR)";
        test(query, Sets.newHashSet("FOO", "BAR"));
    }

    @Test
    public void testExcerptFieldsFunction() throws ParseException {
        String query = "f:excerpt_fields(FOO,BAR)";
        test(query, Sets.newHashSet("FOO", "BAR"));
    }

    @Test
    public void testUniqueFunction() throws ParseException {
        String query = "f:unique(FOO,BAR,BAT)";
        test(query, Sets.newHashSet("FOO", "BAR", "BAT"));

        query = "f:unique('FOO[ALL]','BAR[DAY]','BAT[MINUTE,SECOND]')";
        test(query, Sets.newHashSet("FOO", "BAR", "BAT"));
    }

    @Test
    public void testUniqueByYearFunction() throws ParseException {
        String query = "f:unique_by_year(FOO,BAR,BAT)";
        test(query, Sets.newHashSet("FOO", "BAR", "BAT"));
    }

    @Test
    public void testUniqueByMonthFunction() throws ParseException {
        String query = "f:unique_by_month(FOO,BAR,BAT)";
        test(query, Sets.newHashSet("FOO", "BAR", "BAT"));
    }

    @Test
    public void testUniqueByDayFunction() throws ParseException {
        String query = "f:unique_by_day(FOO,BAR,BAT)";
        test(query, Sets.newHashSet("FOO", "BAR", "BAT"));
    }

    @Test
    public void testUniqueByHourFunction() throws ParseException {
        String query = "f:unique_by_hour(FOO,BAR,BAT)";
        test(query, Sets.newHashSet("FOO", "BAR", "BAT"));
    }

    @Test
    public void testUniqueByTenthOfHourFunction() throws ParseException {
        String query = "f:unique_by_tenth_of_hour(FOO,BAR,BAT)";
        test(query, Sets.newHashSet("FOO", "BAR", "BAT"));
    }

    @Test
    public void testUniqueByMinuteFunction() throws ParseException {
        String query = "f:unique_by_minute(FOO,BAR,BAT)";
        test(query, Sets.newHashSet("FOO", "BAR", "BAT"));
    }

    @Test
    public void testUniqueBySecondFunction() throws ParseException {
        String query = "f:unique_by_second(FOO,BAR,BAT)";
        test(query, Sets.newHashSet("FOO", "BAR", "BAT"));
    }

    @Test
    public void testUniqueByMillisecondFunction() throws ParseException {
        String query = "f:unique_by_millisecond(FOO,BAR,BAT)";
        test(query, Sets.newHashSet("FOO", "BAR", "BAT"));
    }

    @Test
    public void testRenameFunction() throws ParseException {
        String query = "f:rename('FOO=FOO2','BAR=BAR2')";
        test(query, Sets.newHashSet("FOO", "BAR"));
    }

    @Test
    public void testSumFunction() throws ParseException {
        String query = "f:sum(FOO,BAR,BAT)";
        test(query, Sets.newHashSet("FOO", "BAR", "BAT"));
    }

    @Test
    public void testMinFunction() throws ParseException {
        String query = "f:min(FOO,BAR,BAT)";
        test(query, Sets.newHashSet("FOO", "BAR", "BAT"));
    }

    @Test
    public void testMaxFunction() throws ParseException {
        String query = "f:max(FOO,BAR,BAT)";
        test(query, Sets.newHashSet("FOO", "BAR", "BAT"));
    }

    @Test
    public void testAverageFunction() throws ParseException {
        String query = "f:average(FOO,BAR,BAT)";
        test(query, Sets.newHashSet("FOO", "BAR", "BAT"));
    }

    @Test
    public void testCountFunction() throws ParseException {
        String query = "f:count(FOO,BAR,BAT)";
        test(query, Sets.newHashSet("FOO", "BAR", "BAT"));
    }

    // Content functions

    @Test
    public void testContentFunction_phrase() throws ParseException {
        String query = "content:phrase(FOO, termOffsetMap, 'bar', 'baz')";
        test(query, Collections.singleton("FOO"));

        // Multi-fielded
        query = "content:phrase((FOO|FOO2), termOffsetMap, 'bar', 'baz')";
        test(query, Sets.newHashSet("FOO", "FOO2"));

        // Fields within intersection
        query = "(content:phrase(termOffsetMap, 'bar', 'baz') && FOO == 'bar' && FOO == 'baz')";
        test(query, Collections.singleton("FOO"));
    }

    @Test
    public void testContentFunction_scoredPhrase() throws ParseException {
        String query = "content:scoredPhrase(FOO, -1.5, termOffsetMap, 'bar', 'baz')";
        test(query, Collections.singleton("FOO"));

        // Multi-fielded
        query = "content:scoredPhrase((FOO|FOO2), -1.5, termOffsetMap, 'bar', 'baz')";
        test(query, Sets.newHashSet("FOO", "FOO2"));

        // Fields within intersection
        query = "(content:scoredPhrase(-1.5, termOffsetMap, 'bar', 'baz') && FOO == 'bar' && FOO == 'baz')";
        test(query, Collections.singleton("FOO"));
    }

    @Test
    public void testContentFunction_adjacent() throws ParseException {
        String query = "content:adjacent(FOO, termOffsetMap, 'bar', 'baz')";
        test(query, Collections.singleton("FOO"));

        // Fields within intersection
        query = "(content:adjacent(termOffsetMap, 'bar', 'baz') && FOO == 'bar' && FOO == 'baz')";
        test(query, Collections.singleton("FOO"));
    }

    @Test
    public void testContentFunction_within() throws ParseException {
        String query = "content:within(FOO, 5, termOffsetMap, 'bar', 'baz')";
        test(query, Collections.singleton("FOO"));

        // Fields within intersection
        query = "(content:within(5, termOffsetMap, 'bar', 'baz') && FOO == 'bar' && FOO == 'baz')";
        test(query, Collections.singleton("FOO"));
    }

    // Filter functions

    @Test
    public void testFilterIncludeRegex() throws ParseException {
        String query = "filter:includeRegex(FOO, 'bar.*')";
        test(query, Collections.singleton("FOO"));
    }

    @Test
    public void testFilterExcludeRegex() throws ParseException {
        String query = "filter:excludeRegex(FOO, 'bar.*')";
        test(query, Collections.singleton("FOO"));
    }

    @Test
    public void testFilterTimeFunction() throws ParseException {
        String query = "filter:timeFunction(DEATH_DATE,BIRTH_DATE,'-','>',2522880000000L)";
        test(query, Sets.newHashSet("DEATH_DATE", "BIRTH_DATE"));
    }

    @Test
    public void testFilterIsNullFunction() throws ParseException {
        String query = "filter:isNull(FOO)";
        test(query, Collections.singleton("FOO"));
    }

    @Test
    public void testFilterOccurrenceFunction() throws ParseException {
        String query = "filter:occurrence(FOO,'>',3)";
        test(query, Collections.singleton("FOO"));
    }

    @Test
    public void testFilterBetweenDatesFunction() throws ParseException {
        String query = "filter:betweenDates(FOO, '20140101', '20140102')";
        test(query, Collections.singleton("FOO"));
    }

    @Test
    public void testFilterAfterDateFunction() throws ParseException {
        String query = "filter:afterDate(FOO, '20140101')";
        test(query, Collections.singleton("FOO"));
    }

    @Test
    public void testFilterBeforeDateFunction() throws ParseException {
        String query = "filter:beforeDate(FOO, '20140101')";
        test(query, Collections.singleton("FOO"));
    }

    @Test
    public void testFilterBetweenLoadDatesFunction() throws ParseException {
        String query = "filter:betweenLoadDates(FOO, '20140101', '20140102')";
        test(query, Collections.singleton("FOO"));
    }

    @Test
    public void testFilterAfterLoadDateFunction() throws ParseException {
        String query = "filter:afterLoadDate(FOO, '20140101')";
        test(query, Collections.singleton("FOO"));
    }

    @Test
    public void testFilterBeforeLoadDateFunction() throws ParseException {
        String query = "filter:beforeLoadDate(FOO, '20140101')";
        test(query, Collections.singleton("FOO"));
    }

    // Geowave functions
    @Test
    public void testGeoWaveFunction_intersects() throws ParseException {
        String query = "geowave:intersects(FOO, 'POINT(4 4)')";
        test(query, Collections.singleton("FOO"));
    }

    @Test
    public void testGeoWaveFunction_overlaps() throws ParseException {
        String query = "geowave:overlaps(FOO, 'POINT(5 5)')";
        test(query, Collections.singleton("FOO"));
    }

    @Test
    public void testGeoWaveFunction_intersectsAndOverlaps() throws ParseException {
        String query = "geowave:intersects(FOO, 'POINT(4 4)') || geowave:overlaps(FOO, 'POINT(5 5)')";
        test(query, Collections.singleton("FOO"));
    }

    @Test
    public void testGeoWaveFunction_contains() throws ParseException {
        String query = "geowave:contains(FOO, 'POINT(5 5)')";
        test(query, Collections.singleton("FOO"));
    }

    @Test
    public void testGeoWaveFunction_covers() throws ParseException {
        String query = "geowave:covers(FOO, 'POINT(5 5)')";
        test(query, Collections.singleton("FOO"));
    }

    @Test
    public void testGeoWaveFunction_covered_by() throws ParseException {
        String query = "geowave:covered_by(FOO, 'POINT(5 5)')";
        test(query, Collections.singleton("FOO"));
    }

    @Test
    public void testGeoWaveFunction_crosses() throws ParseException {
        String query = "geowave:crosses(FOO, 'POINT(5 5)')";
        test(query, Collections.singleton("FOO"));
    }

    @Test
    public void testGeoWaveFunction_within() throws ParseException {
        String query = "geowave:within(FOO, 'POINT(5 5)')";
        test(query, Collections.singleton("FOO"));
    }

    // Misc. tests
    @Test
    public void testAnyFieldAndNoField() throws ParseException {
        String query = "_ANYFIELD_ == 'bar' && _NOFIELD_ == 'baz'";
        test(query, Sets.newHashSet("_ANYFIELD_", "_NOFIELD_"));
    }

    @Test
    public void testExceededOrThresholdMarker() throws ParseException {
        // shamelessly lifted from ExecutableExpansionVisitorTest
        String query = "FOO == 'capone' && (((_List_ = true) && (((id = 'some-bogus-id') && (field = 'FOO2') && (params = '{\"values\":[\"a\",\"b\",\"c\"]}')))))";
        test(query, Sets.newHashSet("FOO", "FOO2"));
    }

    @Test
    public void testValueExceededMarker() throws ParseException {
        String query = "((_Value_ = true) && (FOO =~ 'ba.*'))";
        test(query, Collections.singleton("FOO"));
    }

    @Test
    public void testMethod() throws ParseException {
        String query = "QUOTE.size() == 1";
        test(query, Collections.emptySet());
    }

    private void test(String query, Set<String> fields) throws ParseException {

        // query as string entrance point
        Set<String> parsedFields = QueryFieldsVisitor.parseQueryFields(query, helper);
        assertEquals(fields, parsedFields);

        // query as ASTJexlTree entrance point
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        parsedFields = QueryFieldsVisitor.parseQueryFields(script, helper);
        assertEquals(fields, parsedFields);
    }
}
