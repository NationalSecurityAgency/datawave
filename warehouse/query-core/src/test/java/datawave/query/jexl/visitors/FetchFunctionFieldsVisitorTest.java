package datawave.query.jexl.visitors;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import datawave.data.type.LcNoDiacriticsType;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.MockMetadataHelper;

public class FetchFunctionFieldsVisitorTest {

    private final Set<Pair<String,String>> functions = new HashSet<>();
    private final MockMetadataHelper metadataHelper = new MockMetadataHelper();
    private String query;

    private final Set<FetchFunctionFieldsVisitor.FunctionFields> expected = new HashSet<>();

    @Before
    public void setUp() throws Exception {
        metadataHelper.addNormalizers("FOO", Collections.singleton(new LcNoDiacriticsType()));
        metadataHelper.addNormalizers("FOO2", Collections.singleton(new LcNoDiacriticsType()));
        metadataHelper.addNormalizers("FOO3", Collections.singleton(new LcNoDiacriticsType()));
        metadataHelper.addNormalizers("FOO4", Collections.singleton(new LcNoDiacriticsType()));
    }

    @After
    public void tearDown() throws Exception {
        query = null;
        functions.clear();
        clearExpected();
    }

    @Test
    public void testGroupByFunction() throws ParseException {
        givenQuery("f:groupby(FOO,BAR)");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("f", "groupby", "FOO", "BAR"));
        assertResults();
    }

    @Test
    public void testNoExpansionFunction() throws ParseException {
        givenQuery("f:noExpansion(FOO,BAR)");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("f", "noExpansion", "FOO", "BAR"));
        assertResults();
    }

    @Test
    public void testLenientFunction() throws ParseException {
        givenQuery("f:lenient(FOO,BAR)");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("f", "lenient", "FOO", "BAR"));
        assertResults();
    }

    @Test
    public void testStrictFunction() throws ParseException {
        givenQuery("f:strict(FOO,BAR)");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("f", "strict", "FOO", "BAR"));
        assertResults();
    }

    @Test
    public void testExcerptFieldsFunction() throws ParseException {
        givenQuery("f:excerpt_fields(FOO,BAR)");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("f", "excerpt_fields", "FOO", "BAR"));
        assertResults();
    }

    @Test
    public void testUniqueFunction() throws ParseException {
        givenQuery("f:unique(FOO,BAR,BAT)");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("f", "unique", "FOO", "BAR", "BAT"));
        assertResults();

        givenQuery("f:unique('FOO[ALL]','BAR[DAY]','BAT[MINUTE,SECOND]')");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("f", "unique", "FOO", "BAR", "BAT"));
        assertResults();
    }

    @Test
    public void testUniqueByYearFunction() throws ParseException {
        givenQuery("f:unique_by_year(FOO,BAR,BAT)");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("f", "unique_by_year", "FOO", "BAR", "BAT"));
        assertResults();
    }

    @Test
    public void testUniqueByMonthFunction() throws ParseException {
        givenQuery("f:unique_by_month(FOO,BAR,BAT)");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("f", "unique_by_month", "FOO", "BAR", "BAT"));
        assertResults();
    }

    @Test
    public void testUniqueByDayFunction() throws ParseException {
        givenQuery("f:unique_by_day(FOO,BAR,BAT)");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("f", "unique_by_day", "FOO", "BAR", "BAT"));
        assertResults();
    }

    @Test
    public void testUniqueByHourFunction() throws ParseException {
        givenQuery("f:unique_by_hour(FOO,BAR,BAT)");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("f", "unique_by_hour", "FOO", "BAR", "BAT"));
        assertResults();
    }

    @Test
    public void testUniqueByTenthOfHourFunction() throws ParseException {
        givenQuery("f:unique_by_tenth_of_hour(FOO,BAR,BAT)");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("f", "unique_by_tenth_of_hour", "FOO", "BAR", "BAT"));
        assertResults();
    }

    @Test
    public void testUniqueByMinuteFunction() throws ParseException {
        givenQuery("f:unique_by_minute(FOO,BAR,BAT)");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("f", "unique_by_minute", "FOO", "BAR", "BAT"));
        assertResults();
    }

    @Test
    public void testUniqueBySecondFunction() throws ParseException {
        givenQuery("f:unique_by_second(FOO,BAR,BAT)");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("f", "unique_by_second", "FOO", "BAR", "BAT"));
        assertResults();
    }

    @Test
    public void testUniqueByMillisecondFunction() throws ParseException {
        givenQuery("f:unique_by_millisecond(FOO,BAR,BAT)");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("f", "unique_by_millisecond", "FOO", "BAR", "BAT"));
        assertResults();
    }

    @Test
    public void testRenameFunction() throws ParseException {
        givenQuery("f:rename('FOO=FOO2','BAR=BAR2')");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("f", "rename", "FOO", "BAR"));
        assertResults();
    }

    @Test
    public void testSumFunction() throws ParseException {
        givenQuery("f:sum(FOO,BAR,BAT)");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("f", "sum", "FOO", "BAR", "BAT"));
        assertResults();
    }

    @Test
    public void testMinFunction() throws ParseException {
        givenQuery("f:min(FOO,BAR,BAT)");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("f", "min", "FOO", "BAR", "BAT"));
        assertResults();
    }

    @Test
    public void testMaxFunction() throws ParseException {
        givenQuery("f:max(FOO,BAR,BAT)");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("f", "max", "FOO", "BAR", "BAT"));
        assertResults();
    }

    @Test
    public void testAverageFunction() throws ParseException {
        givenQuery("f:average(FOO,BAR,BAT)");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("f", "average", "FOO", "BAR", "BAT"));
        assertResults();
    }

    @Test
    public void testCountFunction() throws ParseException {
        givenQuery("f:count(FOO,BAR,BAT)");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("f", "count", "FOO", "BAR", "BAT"));
        assertResults();
    }

    @Test
    public void testContentFunction_phrase() throws ParseException {
        givenQuery("content:phrase(FOO, termOffsetMap, 'bar', 'baz')");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("content", "phrase", "FOO"));
        assertResults();

        // Multi-fielded
        givenQuery("content:phrase((FOO|FOO2), termOffsetMap, 'bar', 'baz')");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("content", "phrase", "FOO", "FOO2"));
        assertResults();
    }

    @Test
    public void testContentFunction_scoredPhrase() throws ParseException {
        givenQuery("content:scoredPhrase(FOO, -1.5, termOffsetMap, 'bar', 'baz')");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("content", "scoredPhrase", "FOO"));
        assertResults();

        // Multi-fielded
        givenQuery("content:scoredPhrase((FOO|FOO2), -1.5, termOffsetMap, 'bar', 'baz')");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("content", "scoredPhrase", "FOO", "FOO2"));
        assertResults();
    }

    @Test
    public void testContentFunction_adjacent() throws ParseException {
        givenQuery("content:adjacent(FOO, termOffsetMap, 'bar', 'baz')");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("content", "adjacent", "FOO"));
        assertResults();
    }

    @Test
    public void testContentFunction_within() throws ParseException {
        givenQuery("content:within(FOO, 5, termOffsetMap, 'bar', 'baz')");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("content", "within", "FOO"));
        assertResults();
    }

    @Test
    public void testFilterIncludeRegex() throws ParseException {
        givenQuery("filter:includeRegex(FOO, 'bar.*')");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("filter", "includeRegex", "FOO"));
        assertResults();
    }

    @Test
    public void testFilterExcludeRegex() throws ParseException {
        givenQuery("filter:excludeRegex(FOO, 'bar.*')");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("filter", "excludeRegex", "FOO"));
        assertResults();
    }

    @Test
    public void testFilterTimeFunction() throws ParseException {
        givenQuery("filter:timeFunction(DEATH_DATE,BIRTH_DATE,'-','>',2522880000000L)");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("filter", "timeFunction", "DEATH_DATE", "BIRTH_DATE"));
        assertResults();
    }

    @Test
    public void testFilterIsNullFunction() throws ParseException {
        givenQuery("filter:isNull(FOO)");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("filter", "isNull", "FOO"));
        assertResults();
    }

    @Test
    public void testFilterOccurrenceFunction() throws ParseException {
        givenQuery("filter:occurrence(FOO,'>',3)");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("filter", "occurrence", "FOO"));
        assertResults();
    }

    @Test
    public void testFilterBetweenDatesFunction() throws ParseException {
        givenQuery("filter:betweenDates(FOO, '20140101', '20140102')");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("filter", "betweenDates", "FOO"));
        assertResults();
    }

    @Test
    public void testFilterAfterDateFunction() throws ParseException {
        givenQuery("filter:afterDate(FOO, '20140101')");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("filter", "afterDate", "FOO"));
        assertResults();
    }

    @Test
    public void testFilterBeforeDateFunction() throws ParseException {
        givenQuery("filter:beforeDate(FOO, '20140101')");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("filter", "beforeDate", "FOO"));
        assertResults();
    }

    @Test
    public void testFilterBetweenLoadDatesFunction() throws ParseException {
        givenQuery("filter:betweenLoadDates(FOO, '20140101', '20140102')");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("filter", "betweenLoadDates", "FOO"));
        assertResults();
    }

    @Test
    public void testFilterAfterLoadDateFunction() throws ParseException {
        givenQuery("filter:afterLoadDate(FOO, '20140101')");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("filter", "afterLoadDate", "FOO"));
        assertResults();
    }

    @Test
    public void testFilterBeforeLoadDateFunction() throws ParseException {
        givenQuery("filter:beforeLoadDate(FOO, '20140101')");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("filter", "beforeLoadDate", "FOO"));
        assertResults();
    }

    @Test
    public void testGeoWaveFunction_intersects() throws ParseException {
        givenQuery("geowave:intersects(FOO, 'POINT(4 4)')");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("geowave", "intersects", "FOO"));
        assertResults();
    }

    @Test
    public void testGeoWaveFunction_overlaps() throws ParseException {
        givenQuery("geowave:overlaps(FOO, 'POINT(5 5)')");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("geowave", "overlaps", "FOO"));
        assertResults();
    }

    @Test
    public void testGeoWaveFunction_intersectsAndOverlaps() throws ParseException {
        givenQuery("geowave:intersects(FOO, 'POINT(4 4)') || geowave:overlaps(FOO, 'POINT(5 5)')");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("geowave", "intersects", "FOO"));
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("geowave", "overlaps", "FOO"));
        assertResults();
    }

    @Test
    public void testGeoWaveFunction_contains() throws ParseException {
        givenQuery("geowave:contains(FOO, 'POINT(5 5)')");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("geowave", "contains", "FOO"));
        assertResults();
    }

    @Test
    public void testGeoWaveFunction_covers() throws ParseException {
        givenQuery("geowave:covers(FOO, 'POINT(5 5)')");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("geowave", "covers", "FOO"));
        assertResults();
    }

    @Test
    public void testGeoWaveFunction_covered_by() throws ParseException {
        givenQuery("geowave:covered_by(FOO, 'POINT(5 5)')");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("geowave", "covered_by", "FOO"));
        assertResults();
    }

    @Test
    public void testGeoWaveFunction_crosses() throws ParseException {
        givenQuery("geowave:crosses(FOO, 'POINT(5 5)')");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("geowave", "crosses", "FOO"));
        assertResults();
    }

    @Test
    public void testGeoWaveFunction_within() throws ParseException {
        givenQuery("geowave:within(FOO, 'POINT(5 5)')");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("geowave", "within", "FOO"));
        assertResults();
    }

    @Test
    public void testMultipleFunctionsWithoutFilter() throws ParseException {
        givenQuery("geowave:within(FOO, 'POINT(5 5)') && filter:includeRegex(BAR, 'abc') && f:strict(BAT,HAT)");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("geowave", "within", "FOO"));
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("filter", "includeRegex", "BAR"));
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("f", "strict", "BAT", "HAT"));
        assertResults();
    }

    @Test
    public void testMultipleFunctionsWithFilter() throws ParseException {
        givenFunctionFilter("filter", "includeRegex");
        givenQuery("geowave:within(FOO, 'POINT(5 5)') && filter:includeRegex(BAR, 'abc') && f:strict(BAT,HAT)");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("filter", "includeRegex", "BAR"));
        assertResults();
    }

    @Test
    public void testMultipleFilters() throws ParseException {
        givenFunctionFilter("filter", "includeRegex");
        givenFunctionFilter("f", "strict");
        givenQuery("geowave:within(FOO, 'POINT(5 5)') && filter:includeRegex(BAR, 'abc') && f:strict(BAT,HAT)");
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("filter", "includeRegex", "BAR"));
        expect(FetchFunctionFieldsVisitor.FunctionFields.of("f", "strict", "BAT", "HAT"));
        assertResults();
    }

    @Test
    public void testFilterWithNoMatches() throws ParseException {
        givenFunctionFilter("f", "lenient");
        givenQuery("geowave:within(FOO, 'POINT(5 5)') && filter:includeRegex(BAR, 'abc') && f:strict(BAT,HAT)");
        assertResults();
    }

    private void givenQuery(String query) {
        this.query = query;
    }

    private void givenFunctionFilter(String namespace, String function) {
        functions.add(Pair.of(namespace, function));
    }

    private void expect(FetchFunctionFieldsVisitor.FunctionFields functionFields) {
        this.expected.add(functionFields);
    }

    private void clearExpected() {
        this.expected.clear();
    }

    private void assertResults() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Set<FetchFunctionFieldsVisitor.FunctionFields> actual = FetchFunctionFieldsVisitor.fetchFields(script, functions, metadataHelper);
        Assert.assertEquals(expected, actual);
        clearExpected();
    }
}
