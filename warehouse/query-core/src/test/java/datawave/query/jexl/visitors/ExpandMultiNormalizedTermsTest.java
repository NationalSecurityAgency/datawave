package datawave.query.jexl.visitors;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.DELAYED;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EVALUATION_ONLY;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_OR;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_TERM;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_VALUE;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.INDEX_HOLE;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.LENIENT;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.STRICT;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import datawave.data.type.IpAddressType;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.LcType;
import datawave.data.type.NoOpType;
import datawave.data.type.NumberType;
import datawave.data.type.PointType;
import datawave.data.type.StringType;
import datawave.data.type.TrimLeadingZerosType;
import datawave.data.type.Type;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.MockMetadataHelper;
import datawave.test.JexlNodeAssert;

public class ExpandMultiNormalizedTermsTest {

    private ShardQueryConfiguration config;
    private MockMetadataHelper helper;

    @Before
    public void before() {
        // Create fresh query config
        config = new ShardQueryConfiguration();
        config.setBeginDate(new Date(0));
        config.setEndDate(new Date(System.currentTimeMillis()));

        // Create fresh metadata helper.
        helper = new MockMetadataHelper();

        // Each test configures fields as needed
    }

    @Test
    public void testSimpleCase() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));

        helper.setIndexedFields(dataTypes.keySet());
        helper.setIndexOnlyFields(dataTypes.keySet());
        helper.addTermFrequencyFields(dataTypes.keySet());

        config.setQueryFieldsDatatypes(dataTypes);

        // No change expected
        String original = "FOO == 'bar'";
        expandTerms(original, original);
    }

    @Test
    public void testNumber() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("MULTI", Sets.newHashSet(new LcNoDiacriticsType(), new NumberType()));
        dataTypes.put("NUM", new NumberType());

        helper.setIndexedFields(dataTypes.keySet());
        helper.setIndexOnlyFields(dataTypes.keySet());
        helper.addTermFrequencyFields(dataTypes.keySet());

        config.setQueryFieldsDatatypes(dataTypes);

        String original = "NUM == '1'";
        String expected = "NUM == '+aE1'";
        expandTerms(original, expected);

        original = "MULTI == '1'";
        expected = "(MULTI == '1' || MULTI == '+aE1')";
        expandTerms(original, expected);
    }

    @Test
    public void testNoOp() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.put("NOOP", new NoOpType());

        helper.setIndexedFields(dataTypes.keySet());
        helper.setIndexOnlyFields(dataTypes.keySet());
        helper.addTermFrequencyFields(dataTypes.keySet());

        config.setQueryFieldsDatatypes(dataTypes);

        // No change expected
        String original = "NOOP == 'bar'";
        expandTerms(original, original);
    }

    @Test
    public void testMixedCase() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("NAME", Sets.newHashSet(new LcNoDiacriticsType(), new NoOpType()));

        helper.setIndexedFields(dataTypes.keySet());
        helper.setIndexOnlyFields(dataTypes.keySet());
        helper.addTermFrequencyFields(dataTypes.keySet());

        config.setQueryFieldsDatatypes(dataTypes);

        String original = "NAME == 'alice'";
        String expected = "NAME == 'alice'";
        expandTerms(original, expected);

        original = "NAME == 'Bob'";
        expected = "(NAME == 'bob' || NAME == 'Bob')";
        expandTerms(original, expected);

        original = "NAME == 'CHARLIE'";
        expected = "(NAME == 'CHARLIE' || NAME == 'charlie')";
        expandTerms(original, expected);
    }

    @Test
    public void testMixedCaseWithNumber() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.put("NUM", new NumberType());
        dataTypes.putAll("NAME", Sets.newHashSet(new LcNoDiacriticsType(), new NoOpType()));

        helper.setIndexedFields(dataTypes.keySet());
        helper.setIndexOnlyFields(dataTypes.keySet());
        helper.addTermFrequencyFields(dataTypes.keySet());

        config.setQueryFieldsDatatypes(dataTypes);

        String original = "(NAME == 'Alice' || NAME == 'BOB') && NUM < '1'";
        String expected = "((NAME == 'Alice' || NAME == 'alice') || (NAME == 'bob' || NAME == 'BOB')) && NUM < '+aE1'";
        expandTerms(original, expected);
    }

    @Test
    public void testIpAddressCase() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType(), new LcType()));
        dataTypes.putAll("IP", Sets.newHashSet(new LcNoDiacriticsType(), new IpAddressType()));

        helper.setIndexedFields(dataTypes.keySet());
        helper.setIndexOnlyFields(dataTypes.keySet());
        helper.addTermFrequencyFields(dataTypes.keySet());

        config.setQueryFieldsDatatypes(dataTypes);

        String original = "FOO == 'bar' && IP == '127.0.0.1'";
        String expected = "FOO == 'bar' && (IP == '127.0.0.1' || IP == '127.000.000.001')";
        expandTerms(original, expected);
    }

    @Test
    public void testNullTermCase() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType(), new LcType()));

        helper.setIndexedFields(dataTypes.keySet());
        helper.setIndexOnlyFields(dataTypes.keySet());
        helper.addTermFrequencyFields(dataTypes.keySet());

        config.setQueryFieldsDatatypes(dataTypes);

        // No change expected
        String original = "FOO == 'null'";
        expandTerms(original, original);
    }

    @Test
    public void testNormalizedBoundsCase() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new NumberType()));

        helper.setIndexedFields(dataTypes.keySet());
        helper.setIndexOnlyFields(dataTypes.keySet());
        helper.addTermFrequencyFields(dataTypes.keySet());

        config.setQueryFieldsDatatypes(dataTypes);

        String original = "FOO > '1' && FOO < '10'";
        String expected = "FOO > '+aE1' && FOO < '+bE1'";
        expandTerms(original, expected);
    }

    @Test
    public void testBoundedNormalizedBoundsCase() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new NumberType()));

        helper.setIndexedFields(dataTypes.keySet());
        helper.setIndexOnlyFields(dataTypes.keySet());
        helper.addTermFrequencyFields(dataTypes.keySet());

        config.setQueryFieldsDatatypes(dataTypes);

        String original = "((_Bounded_ = true) && (FOO > '1' && FOO < '10'))";
        String expected = "((_Bounded_ = true) && (FOO > '+aE1' && FOO < '+bE1'))";
        expandTerms(original, expected);
    }

    @Test
    public void testMultiNormalizedBounds() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new NumberType(), new LcNoDiacriticsType()));

        helper.setIndexedFields(dataTypes.keySet());
        helper.setIndexOnlyFields(dataTypes.keySet());
        helper.addTermFrequencyFields(dataTypes.keySet());

        config.setQueryFieldsDatatypes(dataTypes);

        String original = "FOO > 1 && FOO < 10";
        String expected = "(FOO > '1' || FOO > '+aE1') && (FOO < '+bE1' || FOO < '10')";
        expandTerms(original, expected);
    }

    @Test
    public void testBoundedMultiNormalizedBounds() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new NumberType(), new LcNoDiacriticsType()));

        helper.setIndexedFields(dataTypes.keySet());
        helper.setIndexOnlyFields(dataTypes.keySet());
        helper.addTermFrequencyFields(dataTypes.keySet());

        config.setQueryFieldsDatatypes(dataTypes);

        String original = "((_Bounded_ = true) && (FOO > 1 && FOO < 10))";
        String expected = "((((_Bounded_ = true) && (FOO > '+aE1' && FOO < '+bE1'))) || (((_Bounded_ = true) && (FOO > '1' && FOO < '10'))))";
        expandTerms(original, expected);
    }

    @Test
    public void testBoundedMultiNormalizedBounds2() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new TrimLeadingZerosType(), new StringType()));

        helper.setIndexedFields(dataTypes.keySet());
        helper.setIndexOnlyFields(dataTypes.keySet());
        helper.addTermFrequencyFields(dataTypes.keySet());

        config.setQueryFieldsDatatypes(dataTypes);

        String original = "((_Bounded_ = true) && (FOO > 1 && FOO < 10))";
        String expected = "((_Bounded_ = true) && (FOO > '1' && FOO < '10'))";
        expandTerms(original, expected);
    }

    @Test
    public void testBoundedMultiNormalizedBounds3() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new NumberType(), new LcNoDiacriticsType(), new TrimLeadingZerosType()));

        helper.setIndexedFields(dataTypes.keySet());
        helper.setIndexOnlyFields(dataTypes.keySet());
        helper.addTermFrequencyFields(dataTypes.keySet());

        config.setQueryFieldsDatatypes(dataTypes);

        String original = "((_Bounded_ = true) && (FOO > 1 && FOO < 10))";
        String expected = "((((_Bounded_ = true) && (FOO > '+aE1' && FOO < '+bE1'))) || (((_Bounded_ = true) && (FOO > '1' && FOO < '10'))))";
        expandTerms(original, expected);
    }

    @Test
    public void testUnNormalizedBoundsCase() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.put("NEW", new LcNoDiacriticsType());

        helper.setIndexedFields(dataTypes.keySet());
        config.setQueryFieldsDatatypes(dataTypes);

        String original = "NEW == 'boo' && NEW > '1' && NEW < '10'";
        String expected = "NEW == 'boo' && NEW > '1' && NEW < '10'";
        expandTerms(original, expected);
    }

    @Test
    public void testBoundedUnNormalizedBoundsCase() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.put("NEW", new LcNoDiacriticsType());

        helper.setIndexedFields(dataTypes.keySet());
        config.setQueryFieldsDatatypes(dataTypes);

        String original = "NEW == 'boo' && ((_Bounded_ = true) && (NEW > '1' && NEW < '10'))";
        String expected = "NEW == 'boo' && ((_Bounded_ = true) && (NEW > '1' && NEW < '10'))";
        expandTerms(original, expected);
    }

    @Test
    public void testNormalizedAndUnNormalizedBoundsCase() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.put("NEW", new LcNoDiacriticsType());

        config.setQueryFieldsDatatypes(dataTypes);

        String original = "NEW > '0' && NEW < '9' && FOO > 1 && FOO < 10";
        String expected = "NEW > '0' && NEW < '9' && FOO > 1 && FOO < 10";
        expandTerms(original, expected);
    }

    @Test
    public void testBoundedNormalizedAndUnNormalizedBoundsCase() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.put("NEW", new LcNoDiacriticsType());

        config.setQueryFieldsDatatypes(dataTypes);

        String original = "((_Bounded_ = true) && (NEW > '0' && NEW < '9')) && ((_Bounded_ = true) && (FOO > 1 && FOO < 10))";
        String expected = "((_Bounded_ = true) && (NEW > '0' && NEW < '9')) && ((_Bounded_ = true) && (FOO > 1 && FOO < 10))";
        expandTerms(original, expected);
    }

    @Test
    public void testMultiNormalizedFieldOpField() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType(), new LcType()));

        helper.setIndexedFields(dataTypes.keySet());
        helper.setIndexOnlyFields(dataTypes.keySet());
        helper.addTermFrequencyFields(dataTypes.keySet());

        config.setQueryFieldsDatatypes(dataTypes);

        // No change expected
        String original = "FOO == FOO";
        expandTerms(original, original);
    }

    @Test
    public void testMultiNormalizedLiteralOpLiteral() throws ParseException {
        // No changed expected
        String original = "'bar' == 'bar'";
        expandTerms(original, original);
    }

    @Test
    public void testNormalizedFunctions() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType(), new LcType()));

        helper.setIndexedFields(dataTypes.keySet());
        helper.setIndexOnlyFields(dataTypes.keySet());
        helper.addTermFrequencyFields(dataTypes.keySet());

        config.setQueryFieldsDatatypes(dataTypes);

        String original = "content:phrase(termOffsetMap, 'Foo', 'Bar')";
        String expected = "content:phrase(termOffsetMap, 'foo', 'bar')";
        expandTerms(original, expected);
    }

    @Test
    public void testNormalizedFunctionsWithField() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType(), new LcType()));

        helper.setIndexedFields(dataTypes.keySet());
        helper.setIndexOnlyFields(dataTypes.keySet());
        helper.addTermFrequencyFields(dataTypes.keySet());

        config.setQueryFieldsDatatypes(dataTypes);
        String original = "content:phrase(FOO, termOffsetMap, 'Foo', 'Bar')";
        String expected = "content:phrase(FOO, termOffsetMap, 'foo', 'bar')";
        expandTerms(original, expected);
    }

    @Test
    public void testWeirdlyNormalizedFunction() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.put("NUM", new NumberType());

        helper.setIndexedFields(dataTypes.keySet());
        helper.setIndexOnlyFields(dataTypes.keySet());
        helper.addTermFrequencyFields(dataTypes.keySet());

        config.setQueryFieldsDatatypes(dataTypes);
        String original = "content:phrase(termOffsetMap, '1', '2')";
        String expected = "content:phrase(termOffsetMap, '+aE1', '+aE2')";
        expandTerms(original, expected);
    }

    @Test
    public void testFilterFunctionNormalization() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.put("IP", new IpAddressType());

        helper.setIndexedFields(dataTypes.keySet());
        helper.setIndexOnlyFields(dataTypes.keySet());
        helper.addTermFrequencyFields(dataTypes.keySet());

        config.setQueryFieldsDatatypes(dataTypes);

        String original = "filter:includeRegex(IP, '1\\.2\\.3\\..*')";
        String expected = "filter:includeRegex(IP, '1\\\\.2\\\\.3\\\\..*')";
        expandTerms(original, expected);
    }

    @Test
    public void testFilterFunctionNormalizationWithMultipleNormalizers() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("IP", Sets.newHashSet(new IpAddressType(), new LcNoDiacriticsType()));

        helper.setIndexedFields(dataTypes.keySet());
        helper.setIndexOnlyFields(dataTypes.keySet());
        helper.addTermFrequencyFields(dataTypes.keySet());

        config.setQueryFieldsDatatypes(dataTypes);

        String original = "filter:includeRegex(IP, '1\\.2\\.3\\..*')";
        String expected = "filter:includeRegex(IP, '1\\\\.2\\\\.3\\\\..*')";
        expandTerms(original, expected);
    }

    @Test
    public void testFilterFunctionNormalizationWithNoPattern() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("IP", Sets.newHashSet(new IpAddressType(), new LcNoDiacriticsType()));

        helper.setIndexedFields(dataTypes.keySet());
        helper.setIndexOnlyFields(dataTypes.keySet());
        helper.addTermFrequencyFields(dataTypes.keySet());

        config.setQueryFieldsDatatypes(dataTypes);

        String original = "filter:includeRegex(IP, '1\\.2\\.3\\.4')";
        String expected = "filter:includeRegex(IP, '1\\\\.2\\\\.3\\\\.4')";
        expandTerms(original, expected);
    }

    @Test
    public void testFilterFunctionNormalizationWithIndexedNumeric() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.put("NUM", new NumberType());

        config.setQueryFieldsDatatypes(dataTypes);

        String original = "filter:includeRegex(NUM, '1')";
        expandTerms(original, original);
    }

    @Test
    public void testFilterFunctionNormalizationWithUnIndexedNumeric() throws ParseException {
        // Empty field-datatype multi-map
        config.setQueryFieldsDatatypes(HashMultimap.create());

        String original = "filter:includeRegex(NUM, '1')";
        expandTerms(original, original);
    }

    @Test
    public void testMultipleNormalizersForExceededRegex() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType(), new NoOpType()));

        config.setQueryFieldsDatatypes(dataTypes);

        String original = "((_Value_ = true) && (FOO =~ 'bar.*'))";
        expandTerms(original, original);
    }

    @Test
    public void testMultipleNormalizersForBRExceededThreshold() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType(), new NoOpType()));

        config.setQueryFieldsDatatypes(dataTypes);

        String original = "((_Value_ = true) && (FOO > '1' && FOO < '10'))";
        expandTerms(original, original);
    }

    @Test
    public void testQueryThatShouldMakeNoRanges() throws Exception {
        String query = "FOO == 125 " + " AND (BAR).getValuesForGroups(grouping:getGroupsForMatchesInGroup(FOO,'125')) >= 35"
                        + " and (BAR).getValuesForGroups(grouping:getGroupsForMatchesInGroup(FOO,'125')) <= 36"
                        + " and (BAZ).getValuesForGroups(grouping:getGroupsForMatchesInGroup(FOO,'125')) >= 25"
                        + " and (BAZ).getValuesForGroups(grouping:getGroupsForMatchesInGroup(FOO,'125')) <= 26" + " and WHATEVER == 'la'";

        ASTJexlScript queryTree = JexlASTHelper.parseJexlQuery(query);
        ASTJexlScript smashed = TreeFlatteningRebuildingVisitor.flatten(queryTree);
        ASTJexlScript script = ExpandMultiNormalizedTerms.expandTerms(config, helper, smashed);

        JexlNodeAssert.assertThat(script).isEqualTo(smashed).isEqualTo(queryTree).hasValidLineage();
        JexlNodeAssert.assertThat(queryTree).isEqualTo(query).hasValidLineage();
    }

    @Test
    public void testDelayedPredicates() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType(), new LcType()));

        helper.setIndexedFields(dataTypes.keySet());
        helper.setIndexOnlyFields(dataTypes.keySet());
        helper.addTermFrequencyFields(dataTypes.keySet());

        config.setQueryFieldsDatatypes(dataTypes);

        List<String> markers = Arrays.asList(new String[] {INDEX_HOLE.getLabel(), DELAYED.getLabel(), EVALUATION_ONLY.getLabel(), EXCEEDED_OR.getLabel()});
        for (String marker : markers) {
            String original = "((" + marker + " = true) && (FOO == 'Bar'))";
            String expected = "((" + marker + " = true) && (FOO == 'bar'))";
            expandTerms(original, expected);
        }

        markers = Arrays.asList(new String[] {EXCEEDED_TERM.getLabel(), EXCEEDED_VALUE.getLabel()});
        for (String marker : markers) {
            String original = "((" + marker + " = true) && (FOO == 'Bar'))";
            String expected = "((" + marker + " = true) && (FOO == 'Bar'))";
            expandTerms(original, expected);
        }
    }

    @Test
    public void testLenientMarkerDropped() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType(), new LcType()));

        helper.setIndexedFields(dataTypes.keySet());
        helper.setIndexOnlyFields(dataTypes.keySet());
        helper.addTermFrequencyFields(dataTypes.keySet());

        config.setQueryFieldsDatatypes(dataTypes);

        String original = "((" + LENIENT + " = true) && (FOO == 'Bar'))";
        String expected = "(FOO == 'bar')";
        expandTerms(original, expected);
    }

    @Test
    public void testLenientMarkerDroppedNoTypes() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();

        helper.setIndexedFields(dataTypes.keySet());
        helper.setIndexOnlyFields(dataTypes.keySet());
        helper.addTermFrequencyFields(dataTypes.keySet());

        config.setQueryFieldsDatatypes(dataTypes);

        String original = "((" + LENIENT + " = true) && (FOO == 'Bar'))";
        String expected = "FOO == 'Bar'";
        expandTerms(original, expected);
    }

    @Test
    public void testLenientWithFailedNormalization() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType(), new LcType(), new NumberType()));

        helper.setIndexedFields(dataTypes.keySet());
        helper.setIndexOnlyFields(dataTypes.keySet());
        helper.addTermFrequencyFields(dataTypes.keySet());

        config.setQueryFieldsDatatypes(dataTypes);

        // this tests for the successful normalization as a simple number can be normalized as a regex
        String original = "((" + LENIENT + " = true) && (FOO =~ '32'))";
        String expected = "(FOO =~ '\\+bE3\\.2' || FOO =~ '32')";
        expandTerms(original, expected);

        // This case used to fail numeric normalization, but is now supported.
        original = "((" + LENIENT + " = true) && (FOO =~ '3.*2'))";
        expected = "(FOO =~ '\\+[a-z]E3\\..*2' || FOO =~ '3.*2')";
        expandTerms(original, expected);
    }

    @Test
    public void testLenientWithAllFailedNormalization() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new NumberType(), new PointType()));

        helper.setIndexedFields(dataTypes.keySet());
        helper.setIndexOnlyFields(dataTypes.keySet());
        helper.addTermFrequencyFields(dataTypes.keySet());

        config.setQueryFieldsDatatypes(dataTypes);

        // this tests for the successful normalization as a simple number can be normalized as a regex
        String original = "((" + LENIENT + " = true) && (FOO == 'ab32'))";
        String expected = "(_Drop_ = true) && ((_Reason_ = 'Normalizations failed and not strict') && (_Query_ = 'FOO == \\'ab32\\''))";
        expandTerms(original, expected);
    }

    @Test
    public void testUnmarkedLenientWithAllFailedNormalization() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new NumberType(), new PointType()));

        helper.setIndexedFields(dataTypes.keySet());
        helper.setIndexOnlyFields(dataTypes.keySet());
        helper.addTermFrequencyFields(dataTypes.keySet());

        config.setQueryFieldsDatatypes(dataTypes);
        config.setLenientFields(Collections.singleton("FOO"));

        // this tests for the successful normalization as a simple number can be normalized as a regex
        String original = "(FOO == 'ab32')";
        String expected = "((_Drop_ = true) && ((_Reason_ = 'Normalizations failed and not strict') && (_Query_ = 'FOO == \\'ab32\\'')))";
        expandTerms(original, expected);
    }

    @Test
    public void testStrictWithAllFailedNormalization() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new NumberType(), new PointType()));

        helper.setIndexedFields(dataTypes.keySet());
        helper.setIndexOnlyFields(dataTypes.keySet());
        helper.addTermFrequencyFields(dataTypes.keySet());

        config.setQueryFieldsDatatypes(dataTypes);

        // this tests for the successful normalization as a simple number can be normalized as a regex
        String original = "((" + STRICT + " = true) && (FOO == 'ab32'))";
        String expected = "(_Eval_ = true) && (FOO == 'ab32')";
        expandTerms(original, expected);
    }

    @Test
    public void testFailedRegexNormalizersAndNRNodes() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType(), new LcType(), new NumberType()));

        helper.setIndexedFields(dataTypes.keySet());
        helper.addTermFrequencyFields(dataTypes.keySet());

        config.setQueryFieldsDatatypes(dataTypes);

        // this tests for the successful normalization as a simple number can be normalized as a regex
        String original = "FOO =~ '32' && FOO !~ '42'";
        String expected = "(FOO =~ '32' || FOO =~ '\\+bE3\\.2') && (FOO !~ '\\+bE4\\.2' && FOO !~ '42')";
        expandTerms(original, expected);

        // Where this case the numeric normalization used to fail, but should now support more complex numeric normalization.
        original = "FOO =~ '3.*2' && FOO !~ '3.*22'";
        expected = "(FOO =~ '\\+[a-z]E3\\..*2' || FOO =~ '3.*2') && FOO !~ '\\+[a-z]E3\\..*22' && FOO !~ '3.*22'";
        expandTerms(original, expected);
    }

    @Test
    public void testLenientFailedRegexNormalizersAndNRNodes() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType(), new LcType(), new NumberType()));

        helper.setIndexedFields(dataTypes.keySet());
        helper.addTermFrequencyFields(dataTypes.keySet());

        config.setQueryFieldsDatatypes(dataTypes);
        config.setLenientFields(Collections.singleton("FOO"));

        // this tests for the successful normalization as a simple number can be normalized as a regex
        String original = "FOO =~ '32' && FOO !~ '42'";
        String expected = "(FOO =~ '\\+bE3\\.2' || FOO =~ '32') && FOO !~ '\\+bE4\\.2' && FOO !~ '42'";
        expandTerms(original, expected);

        // This case used to fail numeric normalization, but should now be supported.
        original = "FOO =~ '3.*2' && FOO !~ '3.*22'";
        expected = "(FOO =~ '\\+[a-z]E3\\..*2' || FOO =~ '3.*2') && FOO !~ '\\+[a-z]E3\\..*22' && FOO !~ '3.*22'";
        expandTerms(original, expected);
    }

    @Test
    public void testNENodes() throws ParseException {
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType(), new LcType(), new NumberType()));

        helper.setIndexedFields(dataTypes.keySet());
        helper.addTermFrequencyFields(dataTypes.keySet());

        config.setQueryFieldsDatatypes(dataTypes);

        // this tests for the successful normalization as a simple number can be normalized as a regex
        String original = "FOO != '32' && FOO != '42'";
        String expected = "(FOO != '+bE3.2' && FOO != '32') && (FOO != '42' && FOO != '+bE4.2')";
        expandTerms(original, expected);
    }

    /**
     * For each node type test all lowercase, mixed case, and numeric
     *
     * @throws ParseException
     *             if the query fails to parse
     */
    @Test
    public void testAnyFieldTerms() throws ParseException {

        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType(), new LcType(), new NumberType(), new NoOpType()));
        helper.setDataTypes(dataTypes);

        // EQ
        expandTerms("_ANYFIELD_ == 'anywhere'", "_ANYFIELD_ == 'anywhere'");
        expandTerms("_ANYFIELD_ == 'oHIo'", "_ANYFIELD_ == 'ohio' || _ANYFIELD_ == 'oHIo'");
        expandTerms("_ANYFIELD_ == '123'", "_ANYFIELD_ == '+cE1.23' || _ANYFIELD_ == '123'");

        // NE
        expandTerms("_ANYFIELD_ != 'anywhere'", "_ANYFIELD_ != 'anywhere'");
        expandTerms("_ANYFIELD_ != 'oHIo'", "_ANYFIELD_ != 'ohio' && _ANYFIELD_ != 'oHIo'");
        expandTerms("_ANYFIELD_ != '123'", "_ANYFIELD_ != '+cE1.23' && _ANYFIELD_ != '123'");

        // ER
        expandTerms("_ANYFIELD_ =~ 'anywhere'", "_ANYFIELD_ =~ 'anywhere'");
        expandTerms("_ANYFIELD_ =~ 'oHIo'", "_ANYFIELD_ =~ 'ohio' || _ANYFIELD_ =~ 'oHIo'");
        expandTerms("_ANYFIELD_ =~ '123'", "_ANYFIELD_ =~ '\\+cE1\\.23' || _ANYFIELD_ =~ '123'");

        // NR
        expandTerms("_ANYFIELD_ !~ 'anywhere'", "_ANYFIELD_ !~ 'anywhere'");
        expandTerms("_ANYFIELD_ !~ 'oHIo'", "_ANYFIELD_ !~ 'ohio' && _ANYFIELD_ !~ 'oHIo'");
        expandTerms("_ANYFIELD_ !~ '123'", "_ANYFIELD_ !~ '\\+cE1\\.23' && _ANYFIELD_ !~ '123'");

        // LT
        expandTerms("_ANYFIELD_ < 'anywhere'", "_ANYFIELD_ < 'anywhere'");
        expandTerms("_ANYFIELD_ < 'oHIo'", "_ANYFIELD_ < 'ohio' || _ANYFIELD_ < 'oHIo'");
        expandTerms("_ANYFIELD_ < '123'", "_ANYFIELD_ < '+cE1.23' || _ANYFIELD_ < '123'");

        // LE
        expandTerms("_ANYFIELD_ <= 'anywhere'", "_ANYFIELD_ <= 'anywhere'");
        expandTerms("_ANYFIELD_ <= 'oHIo'", "_ANYFIELD_ <= 'ohio' || _ANYFIELD_ <= 'oHIo'");
        expandTerms("_ANYFIELD_ <= '123'", "_ANYFIELD_ <= '+cE1.23' || _ANYFIELD_ <= '123'");

        // GT
        expandTerms("_ANYFIELD_ > 'anywhere'", "_ANYFIELD_ > 'anywhere'");
        expandTerms("_ANYFIELD_ > 'oHIo'", "_ANYFIELD_ > 'ohio' || _ANYFIELD_ > 'oHIo'");
        expandTerms("_ANYFIELD_ > '123'", "_ANYFIELD_ > '+cE1.23' || _ANYFIELD_ > '123'");

        // GE
        expandTerms("_ANYFIELD_ >= 'anywhere'", "_ANYFIELD_ >= 'anywhere'");
        expandTerms("_ANYFIELD_ >= 'oHIo'", "_ANYFIELD_ >= 'ohio' || _ANYFIELD_ >= 'oHIo'");
        expandTerms("_ANYFIELD_ >= '123'", "_ANYFIELD_ >= '+cE1.23' || _ANYFIELD_ >= '123'");
    }

    private void expandTerms(String original, String expected) throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(original);
        ASTJexlScript expanded = ExpandMultiNormalizedTerms.expandTerms(config, helper, script);

        // Verify the script is as expected, and has a valid lineage.
        JexlNodeAssert.assertThat(expanded).isEqualTo(expected).hasValidLineage();

        // Verify the original script was not modified, and still has a valid lineage.
        JexlNodeAssert.assertThat(script).isEqualTo(original).hasValidLineage();
    }
}
