package datawave.query.transformer.annotation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import datawave.data.normalizer.Normalizer;

public class DefaultKeywordSearchExpressionParserTest {
    private DefaultKeywordSearchExpressionParser parser;

    @Before
    public void setUp() {
        parser = new DefaultKeywordSearchExpressionParser(Normalizer.LC_NO_DIACRITICS_NORMALIZER);
    }

    @Test
    public void standaloneAndPhraseValuesAreDistinct() {
        SearchExpression city = parser.parse("city").getExpressions().get(0);
        assertTrue(city instanceof StandalonePatternExpression);

        SearchExpression phrase = parser.parse("new york.*").getExpressions().get(0);
        assertTrue(phrase instanceof ProximityExpression);
        ProximityExpression proximity = (ProximityExpression) phrase;
        assertTrue(proximity.isOrdered());
        assertEquals(1, proximity.getDistance());
        assertEquals(List.of("new", "york.*"), sources(proximity.getComponents()));
    }

    @Test
    public void whitespaceAndEscapingAreScannedDeterministically() {
        assertEquals(List.of("new", "york"), sources(((ProximityExpression) parser.parse("new   york").getExpressions().get(0)).getComponents()));

        SearchExpression escapedSpace = parser.parse("new\\ york").getExpressions().get(0);
        assertTrue(escapedSpace instanceof StandalonePatternExpression);
        assertEquals("new york", ((StandalonePatternExpression) escapedSpace).getSource());

        SearchExpression escapedBackslash = parser.parse("new\\\\ york").getExpressions().get(0);
        assertTrue(escapedBackslash instanceof ProximityExpression);
        assertEquals(List.of("new\\", "york"), sources(((ProximityExpression) escapedBackslash).getComponents()));

        assertEquals("literal\\", ((StandalonePatternExpression) parser.parse("literal\\").getExpressions().get(0)).getSource());
    }

    @Test
    public void emptyValuesAndDuplicateCriteriaAreSkipped() {
        assertTrue(parser.parse("").isEmpty());
        assertTrue(parser.parse(" \t\n ").isEmpty());

        SearchExpressions expressions = new SearchExpressions(
                        List.of(parser.parse("CITY").getExpressions().get(0), parser.parse("city").getExpressions().get(0)));
        assertEquals(1, expressions.size());
    }

    @Test
    public void componentsAreNormalizedIndependently() {
        ProximityExpression phrase = (ProximityExpression) parser.parse("Néw YORK").getExpressions().get(0);
        assertEquals(List.of("new", "york"), sources(phrase.getComponents()));
    }

    @Test
    public void oneSurvivingComponentBecomesStandalone() {
        Normalizer<String> normalizer = mock(Normalizer.class);
        when(normalizer.normalize("discarded")).thenReturn("");
        when(normalizer.normalize("CITY")).thenReturn("city");

        SearchExpressions expressions = new DefaultKeywordSearchExpressionParser(normalizer).parse("discarded CITY");
        assertEquals(1, expressions.size());
        assertTrue(expressions.getExpressions().get(0) instanceof StandalonePatternExpression);
        assertEquals("city", ((StandalonePatternExpression) expressions.getExpressions().get(0)).getSource());
    }

    @Test
    public void regexBackslashesArePreserved() {
        SearchExpression expression = parser.parse("foo\\.bar").getExpressions().get(0);
        assertTrue(expression instanceof StandalonePatternExpression);
        assertEquals("foo\\.bar", ((StandalonePatternExpression) expression).getSource());
    }

    @Test
    public void returnedExpressionCollectionIsImmutable() {
        SearchExpressions expressions = parser.parse("city");
        try {
            expressions.getExpressions().add(expressions.getExpressions().get(0));
        } catch (UnsupportedOperationException e) {
            return;
        }
        throw new AssertionError("parser result must not expose a mutable expression collection");
    }

    private List<String> sources(List<StandalonePatternExpression> expressions) {
        return expressions.stream().map(StandalonePatternExpression::getSource).collect(java.util.stream.Collectors.toList());
    }
}
