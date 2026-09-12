package datawave.query.transformer.annotation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

class JexlSearchExpressionExtractorTest {
    @Test
    void extractsFunctionsInArgumentOrderAndKeepsDuplicates() {
        SearchExpressions expressions = new JexlSearchExpressionExtractor(null).extract(
                        "content:phrase(termOffsetMap, 'New', 'York', 'New') || content:adjacent(termOffsetMap, 'a', 'b') || content:within(4, termOffsetMap, 'x', 'y')");
        List<ProximityExpression> proximity = expressions.getExpressions().stream().filter(ProximityExpression.class::isInstance)
                        .map(ProximityExpression.class::cast).collect(Collectors.toList());
        assertEquals(3, proximity.size());
        assertEquals(List.of("new", "york", "new"), sources(proximity.get(0)));
        assertTrue(proximity.get(0).isOrdered());
        assertEquals(1, proximity.get(0).getDistance());
        assertEquals(1, proximity.get(1).getDistance());
        assertEquals(4, proximity.get(2).getDistance());
    }

    @Test
    void acceptsUnfieldedAndAnyFieldButRejectsOtherFields() {
        String query = "content:phrase(termOffsetMap, 'a', 'b') || content:phrase(FIELD, termOffsetMap, 'c', 'd') || content:phrase(OTHER, termOffsetMap, 'e', 'f')";
        assertEquals(2, new JexlSearchExpressionExtractor(Set.of("FIELD")).extract(query).size());
        assertEquals(3, new JexlSearchExpressionExtractor(Set.of("_ANYFIELD_")).extract(query).size());
    }

    @Test
    void excludesNegationsAndScoredPhraseAndDoesNotInferFunctionTerms() {
        String query = "A == 'standalone' && content:scoredPhrase(-1, termOffsetMap, 'ignored', 'alsoIgnored') "
                        + "&& !(content:phrase(termOffsetMap, 'no', 'no')) && !(!(content:phrase(termOffsetMap, 'yes', 'yes')))";
        SearchExpressions expressions = new JexlSearchExpressionExtractor(null).extract(query);
        assertEquals(2, expressions.size());
        assertTrue(expressions.getExpressions().stream().anyMatch(e -> e instanceof StandalonePatternExpression));
        assertTrue(expressions.getExpressions().stream().anyMatch(e -> e instanceof ProximityExpression));
    }

    @Test
    void coversFieldedAndUnfieldedSignaturesForEachSupportedFunction() {
        assertEquals(1, new JexlSearchExpressionExtractor(Set.of("FIELD")).extract("content:adjacent(termOffsetMap, 'a', 'b')").size());
        assertEquals(1, new JexlSearchExpressionExtractor(Set.of("FIELD")).extract("content:adjacent('FIELD', termOffsetMap, 'c', 'd')").size());
        assertEquals(1, new JexlSearchExpressionExtractor(Set.of("FIELD")).extract("content:within(3, termOffsetMap, 'e', 'f')").size());
        assertEquals(1, new JexlSearchExpressionExtractor(Set.of("FIELD")).extract("content:within('FIELD', 4, termOffsetMap, 'g', 'h')").size());
        assertEquals(1, new JexlSearchExpressionExtractor(Set.of("FIELD")).extract("content:phrase(FIELD, termOffsetMap, 'i', 'j')").size());

    }

    @Test
    void retainsPhraseAndExplicitStandaloneWithSameLiteral() {
        SearchExpressions expressions = new JexlSearchExpressionExtractor(null).extract("A == 'same' && content:phrase(termOffsetMap, 'same', 'other')");
        assertEquals(2, expressions.size());
        assertTrue(expressions.getExpressions().stream().anyMatch(StandalonePatternExpression.class::isInstance));
        assertTrue(expressions.getExpressions().stream().anyMatch(ProximityExpression.class::isInstance));
    }

    @Test
    void malformedSupportedFunctionsFailLikeContentFunctionValidation() {
        assertThrows(IllegalArgumentException.class,
                        () -> new JexlSearchExpressionExtractor(null).extract("content:within('not-a-distance', termOffsetMap, 'a', 'b')"));
        assertThrows(IllegalArgumentException.class, () -> new JexlSearchExpressionExtractor(null).extract("content:phrase(termOffsetMap, FIELD)"));
    }

    @Test
    void regexStandaloneAndUnaryMinusDistanceIsRejectedByTheModel() {
        assertThrows(IllegalArgumentException.class,
                        () -> new JexlSearchExpressionExtractor(null).extract("A =~ 'Ab.*' && content:within(-2, termOffsetMap, 'x', 'y')"));
        SearchExpressions expressions = new JexlSearchExpressionExtractor(null).extract("A =~ 'Ab.*'");
        assertEquals(1, expressions.size());
        assertTrue(((StandalonePatternExpression) expressions.getExpressions().get(0)).getPatternSource().contains("ab"));
    }

    private static List<String> sources(ProximityExpression expression) {
        return expression.getComponents().stream().map(StandalonePatternExpression::getPatternSource).collect(Collectors.toList());
    }
}
