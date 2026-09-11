package datawave.query.transformer.annotation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

    private List<String> sources(List<StandalonePatternExpression> expressions) {
        return expressions.stream().map(StandalonePatternExpression::getSource).collect(java.util.stream.Collectors.toList());
    }
}
