package datawave.query.transformer.annotation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.lucene.queryparser.flexible.core.QueryNodeParseException;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.junit.jupiter.api.Test;

import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;

class LuceneSearchExpressionExtractorTest {
    @Test
    void retainsPhraseProvenanceAndExplicitStandaloneTerm() {
        SearchExpressions expressions = new LuceneSearchExpressionExtractor(new LuceneToJexlQueryParser(), Set.of("TEXT"))
                        .extract("TEXT:\"new york\" OR TEXT:new");
        assertEquals(2, expressions.size());
        assertTrue(expressions.getExpressions().stream()
                        .anyMatch(e -> e instanceof StandalonePatternExpression && ((StandalonePatternExpression) e).getPatternSource().equals("new")));
        ProximityExpression phrase = expressions.getExpressions().stream().filter(ProximityExpression.class::isInstance).map(ProximityExpression.class::cast)
                        .findFirst().orElseThrow();
        assertEquals(List.of("new", "york"), phrase.getComponents().stream().map(StandalonePatternExpression::getPatternSource).collect(Collectors.toList()));
        assertEquals(1, phrase.getDistance());
    }

    @Test
    void importsOnlyGeneratedProximityAndUsesConfiguredParser() {
        CountingParser parser = new CountingParser();
        LuceneSearchExpressionExtractor extractor = new LuceneSearchExpressionExtractor(parser, Set.of("TEXT"));
        SearchExpressions expressions = extractor.extract("TEXT:\"new york\"~2", "content:within(4, termOffsetMap, 'new', 'york') || TEXT == 'ignored'");
        assertEquals(1, parser.calls);
        assertTrue(expressions.getExpressions().stream().anyMatch(e -> e instanceof ProximityExpression && ((ProximityExpression) e).getDistance() == 4));
        assertTrue(expressions.getExpressions().stream()
                        .noneMatch(e -> e instanceof StandalonePatternExpression && ((StandalonePatternExpression) e).getPatternSource().equals("ignored")));
    }

    @Test
    void excludesNegatedTermsAndRetainsDoubleNegation() {
        LuceneSearchExpressionExtractor extractor = new LuceneSearchExpressionExtractor(new LuceneToJexlQueryParser(), Set.of("TEXT"));
        SearchExpressions expressions = extractor.extract("TEXT:good NOT TEXT:bad");
        assertTrue(expressions.getExpressions().stream().anyMatch(e -> source(e).equals("good")));
        assertTrue(expressions.getExpressions().stream().noneMatch(e -> source(e).equals("bad")));

        SearchExpressions doubleNegative = new LuceneSearchExpressionExtractor(new DoubleNegativeParser(), Set.of("TEXT")).extract("ignored");
        assertTrue(doubleNegative.getExpressions().stream().anyMatch(e -> source(e).equals("kept")));
    }

    @Test
    void handlesSmartQuotesEscapesAndWildcardPhraseComponents() {
        LuceneSearchExpressionExtractor extractor = new LuceneSearchExpressionExtractor(new LuceneToJexlQueryParser(), Set.of("TEXT"));
        SearchExpressions expressions = extractor.extract("TEXT:\u201cnew york\u201d OR TEXT:\"new yo*\"");
        System.out.println(expressions.getExpressions());
        assertTrue(expressions.getExpressions().stream().filter(ProximityExpression.class::isInstance).map(ProximityExpression.class::cast)
                        .anyMatch(p -> sources(p).equals(List.of("new", "york"))));
        assertTrue(expressions.getExpressions().stream().filter(ProximityExpression.class::isInstance).map(ProximityExpression.class::cast)
                        .anyMatch(p -> sources(p).size() == 2 && sources(p).get(0).equals("new") && sources(p).get(1).startsWith("yo")));
    }

    @Test
    void enforcesLuceneFieldEligibilityAndAnyField() {
        String query = "TEXT:\"new york\" OR OTHER:\"bad field\" OR \"unfielded phrase\"";
        assertEquals(2, new LuceneSearchExpressionExtractor(new LuceneToJexlQueryParser(), Set.of("TEXT")).extract(query).size());
        assertEquals(3, new LuceneSearchExpressionExtractor(new LuceneToJexlQueryParser(), Set.of("_ANYFIELD_")).extract(query).size());
    }

    @Test
    void deduplicatesEquivalentRawAndGeneratedProximity() {
        SearchExpressions expressions = new LuceneSearchExpressionExtractor(new LuceneToJexlQueryParser(), Set.of("TEXT")).extract("TEXT:\"new york\"",
                        "content:phrase(termOffsetMap, 'new', 'york')");
        assertEquals(1, expressions.getExpressions().stream().filter(ProximityExpression.class::isInstance).count());
    }

    @Test
    void importsTokenizedAnalyzerProximityWithoutGeneratedEqualityHits() {
        SearchExpressions expressions = new LuceneSearchExpressionExtractor(new LuceneToJexlQueryParser(), Set.of("TEXT")).extract("TEXT:running",
                        "content:within(3, termOffsetMap, 'run', 'ning') || TEXT == 'unrelated'");
        assertTrue(expressions.getExpressions().stream().anyMatch(e -> e instanceof ProximityExpression && ((ProximityExpression) e).getDistance() == 3));
        assertTrue(expressions.getExpressions().stream().noneMatch(e -> source(e).equals("unrelated")));
    }

    private static String source(SearchExpression expression) {
        return expression instanceof StandalonePatternExpression ? ((StandalonePatternExpression) expression).getPatternSource() : "";
    }

    private static List<String> sources(ProximityExpression expression) {
        return expression.getComponents().stream().map(StandalonePatternExpression::getPatternSource).collect(Collectors.toList());
    }

    private static class DoubleNegativeParser extends LuceneToJexlQueryParser {
        @Override
        public QueryNode parseToLuceneQueryNode(String query) {
            FieldQueryNode field = new FieldQueryNode("TEXT", "kept", 0, 4);
            ModifierQueryNode inner = new ModifierQueryNode(field, ModifierQueryNode.Modifier.MOD_NOT);
            return new ModifierQueryNode(inner, ModifierQueryNode.Modifier.MOD_NOT);
        }
    }

    private static class CountingParser extends LuceneToJexlQueryParser {
        private int calls;

        @Override
        public QueryNode parseToLuceneQueryNode(String query) throws QueryNodeParseException {
            calls++;
            return super.parseToLuceneQueryNode(query);
        }
    }
}
