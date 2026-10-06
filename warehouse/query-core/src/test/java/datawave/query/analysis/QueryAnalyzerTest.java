package datawave.query.analysis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import org.junit.Test;

import datawave.query.analysis.QueryAnalyzer.AnalysisReport;
import datawave.query.analysis.QueryAnalyzer.Category;
import datawave.query.analysis.QueryAnalyzer.QueryAnalysis;
import datawave.query.analysis.QueryAnalyzer.QueryInput;
import datawave.query.analysis.QueryAnalyzer.Status;
import datawave.query.analysis.QueryAnalyzer.Syntax;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;

public class QueryAnalyzerTest {
    private final QueryAnalyzer analyzer = new QueryAnalyzer();

    @Test
    public void clustersMixedSyntaxAndRetainsDuplicatePositions() {
        AnalysisReport report = analyzer.analyze(Arrays.asList(new QueryInput("NAME:alice", Syntax.LUCENE), new QueryInput("NAME == 'bob'", Syntax.JEXL),
                        new QueryInput("NAME:alice", Syntax.LUCENE), new QueryInput("CITY:paris", Syntax.LUCENE)));
        assertEquals(2, report.getClusters().size());
        List<QueryAnalysis> names = report.getClusters().get(report.getQueries().get(0).getSignature());
        assertEquals(3, names.size());
        assertEquals(0, names.get(0).getIndex());
        assertEquals(1, names.get(1).getIndex());
        assertEquals(2, names.get(2).getIndex());
        assertEquals("NAME:alice", names.get(0).getInput().getQuery());
        assertEquals(Collections.singleton("NAME"), names.get(0).getFields());
        assertEquals(EnumSet.of(Category.EQUALITY), names.get(0).getCategories());
        assertFalse(names.get(0).getSignature().contains("alice"));
        assertTrue(names.get(0).getJexl().contains("alice"));
    }

    @Test
    public void normalizesCommutativeAssociativeJunctionsButPreservesGrouping() {
        assertEquals(signature("A == '1' && (B == '2' && C == '3')"), signature("C == 'x' && A == 'y' && B == 'z'"));
        assertEquals(signature("A == '1' || (B == '2' || C == '3')"), signature("C == 'x' || A == 'y' || B == 'z'"));
        assertNotEquals(signature("A == '1' && (B == '2' || C == '3')"), signature("(A == '1' && B == '2') || C == '3'"));
        assertNotEquals(signature("A == '1' && A == '2'"), signature("A == '1'"));
    }

    @Test
    public void preservesFieldsOperatorsAndOperandOrder() {
        assertNotEquals(signature("A == 'x'"), signature("a == 'x'"));
        assertNotEquals(signature("A.1 == 'x'"), signature("A.2 == 'x'"));
        assertNotEquals(signature("A > 1"), signature("A >= 1"));
        assertNotEquals(signature("A > 1"), signature("1 > A"));
        assertNotEquals(signature("A == 'x'"), signature("A =~ 'x'"));
        assertNotEquals(signature("A == 'x'"), signature("A != 'x'"));
    }

    @Test
    public void ignoresScalarValuesButRetainsNullAndBooleanLiterals() {
        assertEquals(signature("AGE >= '10'"), signature("AGE >= 20"));
        assertEquals(signature("AGE >= -10"), signature("AGE >= 20"));
        assertNotEquals(signature("A == null"), signature("A == 'null'"));
        assertNotEquals(signature("A == true"), signature("A == false"));
        assertTrue(analyze("A != null").getCategories().containsAll(EnumSet.of(Category.EQUALITY, Category.NULL_CHECK, Category.NEGATION)));
    }

    @Test
    public void recognizesRegexRangeNegationAndBooleanCategories() {
        QueryAnalysis result = analyze("(NAME =~ 'al.*' || NAME !~ 'bo.*') && AGE >= 18 && !(CITY == 'rome')");
        assertEquals(EnumSet.of(Category.REGEX, Category.RANGE, Category.NEGATION, Category.DISJUNCTION, Category.CONJUNCTION, Category.EQUALITY),
                        result.getCategories());
        assertEquals(Arrays.asList("AGE", "CITY", "NAME"), new ArrayList<>(result.getFields()));
    }

    @Test
    public void preservesFunctionNamesAndOrderedArguments() {
        assertEquals(signature("filter:includeRegex(NAME, 'al.*')"), signature("filter:includeRegex(NAME, 'bo.*')"));
        assertNotEquals(signature("filter:includeRegex(NAME, 'al.*')"), signature("filter:excludeRegex(NAME, 'al.*')"));
        assertNotEquals(signature("custom:compare(A, B)"), signature("custom:compare(B, A)"));
        QueryAnalysis result = analyze("filter:includeRegex(NAME, 'al.*')");
        assertEquals(Collections.singleton("filter:includeRegex"), result.getFunctions());
        assertEquals(Collections.singleton("NAME"), result.getFields());
        assertEquals(EnumSet.of(Category.FUNCTION), result.getCategories());
    }

    @Test
    public void recognizesContentGeoAndUnfieldedQueries() {
        QueryAnalysis content = analyze("content:phrase(TEXT, termOffsetMap, 'quick', 'fox')");
        assertTrue(content.getCategories().containsAll(EnumSet.of(Category.FUNCTION, Category.CONTENT)));
        assertEquals(Collections.singleton("TEXT"), content.getFields());
        assertTrue(analyze("geo:within_bounding_box(LOCATION, '1_2', '3_4')").getCategories().contains(Category.GEOSPATIAL));
        QueryAnalysis unfielded = analyzer.analyze(Collections.singletonList("hello"), Syntax.LUCENE).getQueries().get(0);
        assertEquals(Status.SUCCESS, unfielded.getStatus());
        assertTrue(unfielded.getCategories().contains(Category.UNFIELDED));
        assertTrue(unfielded.getFields().isEmpty());
    }

    @Test
    public void preservesDatawaveMarkersAndExcludesMarkerIdentifiers() {
        String bounded = "((_Bounded_ = true) && (AGE >= 20 && AGE <= 40))";
        QueryAnalysis lucene = analyzer.analyze(Collections.singletonList("AGE:[20 TO 40]"), Syntax.LUCENE).getQueries().get(0);
        assertEquals(Status.SUCCESS, lucene.getStatus());
        assertEquals(signature(bounded), lucene.getSignature());
        assertEquals(Collections.singleton("AGE"), lucene.getFields());
        assertTrue(lucene.getCategories().containsAll(EnumSet.of(Category.RANGE, Category.PROPERTY_MARKER)));
        assertNotEquals(signature(bounded), signature("AGE >= 20 && AGE <= 40"));
        assertNotEquals(signature("((_Delayed_ = true) && (A == 'x'))"), signature("((_Eval_ = true) && (A == 'x'))"));
        assertNotEquals(signature("((_Delayed_ = true) && (A == 'x'))"), signature("A == 'x'"));
    }

    @Test
    public void isolatesInvalidAndUnsupportedInputs() {
        List<QueryInput> inputs = Arrays.asList(new QueryInput("A ==", Syntax.JEXL), new QueryInput("FIELD:(", Syntax.LUCENE), new QueryInput(" ", Syntax.JEXL),
                        new QueryInput(null, Syntax.JEXL), new QueryInput("A == 'x'", null), null, new QueryInput("A = 'x'", Syntax.JEXL),
                        new QueryInput("A.size() > 0", Syntax.JEXL), new QueryInput("A == 'ok'", Syntax.JEXL));
        AnalysisReport report = analyzer.analyze(inputs);
        assertEquals(inputs.size(), report.getQueries().size());
        for (int i = 0; i < 8; i++) {
            QueryAnalysis result = report.getQueries().get(i);
            assertEquals(i < 6 ? Status.INVALID : Status.UNSUPPORTED, result.getStatus());
            assertNotNull(result.getError());
            assertNull(result.getSignature());
            assertTrue(result.getCategories().isEmpty());
        }
        assertEquals(Status.SUCCESS, report.getQueries().get(8).getStatus());
        assertEquals(1, report.getClusters().size());
    }

    @Test
    public void rejectsMalformedMarkersWithoutDroppingQueryStructure() {
        for (String query : Arrays.asList("((_Delayed_ = false) && A == 'x')", "((_Delayed_ = SOME_FIELD) && A == 'x')",
                        "(A == (_Delayed_ = true)) && B == 'x'", "(_Delayed_ = true) && (_Eval_ = true)")) {
            assertEquals(query, Status.UNSUPPORTED, analyze(query).getStatus());
        }
        assertEquals(Collections.singleton("termOffsetMap"), analyze("termOffsetMap == 'x'").getFields());
    }

    @Test
    public void handlesEscapesThroughTheRealParsers() {
        QueryAnalysis result = analyze("NAME == 'O\\'Brien' && PATH =~ 'a.*' ");
        assertEquals(Status.SUCCESS, result.getStatus());
        assertEquals(signature("NAME == 'Smith' && PATH =~ 'b.*'"), result.getSignature());
    }

    @Test
    public void honorsConfiguredLuceneParser() {
        LuceneToJexlQueryParser parser = new LuceneToJexlQueryParser();
        parser.setAllowedFields(Collections.singleton("NAME"));
        AnalysisReport report = new QueryAnalyzer(parser).analyze(Arrays.asList("CITY:paris", "NAME:alice"), Syntax.LUCENE);
        assertEquals(Status.INVALID, report.getQueries().get(0).getStatus());
        assertEquals(Status.SUCCESS, report.getQueries().get(1).getStatus());
    }

    @Test
    public void returnsImmutableDeterministicSnapshots() {
        List<String> inputs = new ArrayList<>(Arrays.asList("A == '1'", "B == '2'", "A == '3'"));
        AnalysisReport report = analyzer.analyze(inputs, Syntax.JEXL);
        Collections.reverse(inputs);
        AnalysisReport reversed = analyzer.analyze(inputs, Syntax.JEXL);
        assertEquals(new ArrayList<>(report.getClusters().keySet()), new ArrayList<>(reversed.getClusters().keySet()));
        assertEquals("A == '1'", report.getQueries().get(0).getInput().getQuery());
        assertThrows(UnsupportedOperationException.class, () -> report.getQueries().clear());
        assertThrows(UnsupportedOperationException.class, () -> report.getClusters().clear());
        assertThrows(UnsupportedOperationException.class, () -> report.getClusters().values().iterator().next().clear());
        assertThrows(UnsupportedOperationException.class, () -> report.getQueries().get(0).getFields().clear());
        assertThrows(UnsupportedOperationException.class, () -> report.getQueries().get(0).getCategories().clear());
        assertTrue(analyzer.analyze(Collections.emptyList(), Syntax.JEXL).getClusters().isEmpty());
    }

    private QueryAnalysis analyze(String query) {
        return analyzer.analyze(Collections.singletonList(query), Syntax.JEXL).getQueries().get(0);
    }

    private String signature(String query) {
        QueryAnalysis result = analyze(query);
        assertEquals(result.getError(), Status.SUCCESS, result.getStatus());
        return result.getSignature();
    }
}
