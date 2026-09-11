package datawave.query.transformer.annotation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.lucene.queryparser.flexible.core.QueryNodeParseException;
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
        assertTrue(expressions.getExpressions().stream().anyMatch(e -> e instanceof ProximityExpression));
        assertTrue(expressions.getExpressions().stream()
                        .noneMatch(e -> e instanceof StandalonePatternExpression && ((StandalonePatternExpression) e).getPatternSource().equals("ignored")));
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
