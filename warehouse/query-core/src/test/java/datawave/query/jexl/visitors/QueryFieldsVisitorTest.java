package datawave.query.jexl.visitors;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.Set;

import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
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

        helper.addField("FOO", "LcNoDiacriticsType");
        helper.addField("FOO2", "LcNoDiacriticsType");
        helper.addField("FOO3", "LcNoDiacriticsType");
        helper.addField("FOO4", "LcNoDiacriticsType");
    }

    @Test
    public void testEQ() throws ParseException {
        String query = "FOO == 'bar'";
        test(query, Collections.singleton("FOO"));

        // identifiers
        query = "$12 == 'bar'";
        test(query, Collections.singleton("12"));

        // grouping context
        query = "FOO.12 == 'bar'";
        test(query, Collections.singleton("FOO"));
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

    // Some functions

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
