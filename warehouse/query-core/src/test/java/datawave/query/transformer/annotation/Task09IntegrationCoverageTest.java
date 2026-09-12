package datawave.query.transformer.annotation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.lucene.queryparser.flexible.core.QueryNodeParseException;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.junit.jupiter.api.Test;

import datawave.annotation.protobuf.v1.BoundaryType;
import datawave.annotation.protobuf.v1.SegmentBoundary;
import datawave.annotation.protobuf.v1.SegmentValue;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;

/** Focused Task 09 coverage for the integration contracts between the new APIs. */
class Task09IntegrationCoverageTest {
    @Test
    void directJexlRetainsStandalonePhraseAndIgnoresEffectiveNegation() {
        SearchExpressions expressions = new JexlSearchExpressionExtractor(null)
                        .extract("A == 'alone' && content:phrase(termOffsetMap, 'new', 'york') && !(B == 'ignored')");
        assertEquals(2, expressions.size());
        assertTrue(expressions.getExpressions().stream().anyMatch(StandalonePatternExpression.class::isInstance));
        ProximityExpression phrase = expressions.getExpressions().stream().filter(ProximityExpression.class::isInstance).map(ProximityExpression.class::cast)
                        .findFirst().orElseThrow();
        assertEquals(List.of("new", "york"), phrase.getComponents().stream().map(StandalonePatternExpression::getPatternSource).collect(Collectors.toList()));
    }

    @Test
    void luceneUsesExpandedSourceAndDoesNotImportGeneratedEqualityAlternatives() {
        RecordingParser parser = new RecordingParser();
        SearchExpressions expressions = new LuceneSearchExpressionExtractor(parser, Set.of("TEXT")).extract("TEXT:(\"new york\" OR expanded)",
                        "content:within(3, termOffsetMap, 'new', 'york') || TEXT == 'generated'");
        assertEquals("TEXT:(\"new york\" OR expanded)", parser.source);
        assertFalse(expressions.getExpressions().stream()
                        .anyMatch(e -> e instanceof StandalonePatternExpression && ((StandalonePatternExpression) e).getPatternSource().equals("generated")));
        assertTrue(expressions.getExpressions().stream().anyMatch(ProximityExpression.class::isInstance));
    }

    @Test
    void keywordParserPreservesRegexAndEscapedSpaceSemantics() {
        DefaultKeywordSearchExpressionParser parser = new DefaultKeywordSearchExpressionParser();
        SearchExpressions phrase = parser.parse("new york.*");
        assertEquals(1, phrase.size());
        assertTrue(phrase.getExpressions().get(0) instanceof ProximityExpression);
        SearchExpressions escaped = parser.parse("new\\ york");
        assertEquals(1, escaped.size());
        assertTrue(escaped.getExpressions().get(0) instanceof StandalonePatternExpression);
        assertEquals("new york", ((StandalonePatternExpression) escaped.getExpressions().get(0)).getPatternSource());
    }

    @Test
    void flattenBoundaryChangesOrderedMatchingWithoutReparsingCriteria() {
        TreeMap<SegmentBoundary,List<SegmentValue>> segments = segments(List.of(value("new", 1), value("york", 1)));
        AnnotationPositionView view = AnnotationPositionView.of(segments, new datawave.data.normalizer.LcNoDiacriticsNormalizer());
        ProximityExpression expression = new ProximityExpression(true, List.of(new StandalonePatternExpression("new"), new StandalonePatternExpression("york")),
                        1);
        OrderedAnnotationMatcher matcher = new OrderedAnnotationMatcher(expression);
        assertTrue(matcher.match(view, 0, false).isEmpty());
        assertEquals(1, matcher.match(view, 0, true).size());
    }

    @Test
    void unorderedWithinUsesConfiguredDistanceAndScoreThreshold() {
        TreeMap<SegmentBoundary,List<SegmentValue>> segments = new TreeMap<>(new BoundaryComparator());
        segments.put(boundary(0), List.of(value("alpha", .9f)));
        segments.put(boundary(1), List.of(value("beta", .1f)));
        segments.put(boundary(2), List.of(value("gamma", .9f)));
        AnnotationPositionView view = AnnotationPositionView.of(segments, new datawave.data.normalizer.LcNoDiacriticsNormalizer());
        ProximityExpression expression = new ProximityExpression(false,
                        List.of(new StandalonePatternExpression("alpha"), new StandalonePatternExpression("gamma")), 2);
        assertEquals(1, new UnorderedAnnotationMatcher(expression).match(view, .5f, false).size());
        assertTrue(new UnorderedAnnotationMatcher(expression).match(view, .95f, false).isEmpty());
    }

    @Test
    void mixedFactoryPathExpandsPhraseConstituentsWithPhraseContext() throws Exception {
        SegmentBoundary first = boundary(0);
        SegmentBoundary second = boundary(1);
        TreeMap<SegmentBoundary,List<SegmentValue>> segments = new TreeMap<>(new BoundaryComparator());
        segments.put(first, List.of(value("new", 1)));
        segments.put(second, List.of(value("york", 1)));
        SegmentHit one = new SegmentHit(first, first, 0);
        SegmentHit two = new SegmentHit(second, second, 0);
        PhraseHit phrase = PhraseHit.fromConstituents(List.of(one, two), segments, 0);
        assertEquals(2, phrase.getConstituentHits().size());
        assertEquals(first, phrase.getContextStart());
        assertEquals(second, phrase.getContextEnd());
        assertTrue(new AllHitsFactory().createFromHits("id", List.of(phrase), segments, 0, TimeUnit.MILLISECONDS) != null);
    }

    private static TreeMap<SegmentBoundary,List<SegmentValue>> segments(List<SegmentValue> values) {
        TreeMap<SegmentBoundary,List<SegmentValue>> result = new TreeMap<>(new BoundaryComparator());
        result.put(boundary(0), values);
        return result;
    }

    private static SegmentBoundary boundary(long start) {
        return SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart((int) start).setEnd((int) start + 1).build();
    }

    private static SegmentValue value(String value, float score) {
        return SegmentValue.newBuilder().setValue(value).setScore(score).build();
    }

    private static class RecordingParser extends LuceneToJexlQueryParser {
        private String source;

        @Override
        public QueryNode parseToLuceneQueryNode(String query) throws QueryNodeParseException {
            source = query;
            return super.parseToLuceneQueryNode(query);
        }
    }
}
