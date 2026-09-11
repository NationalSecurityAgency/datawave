package datawave.query.config.annotation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

import datawave.query.transformer.annotation.KeywordParser;
import datawave.query.transformer.annotation.QueryExpressionExtractor;

public class AllHitsQueryConfigTest {
    @Test
    public void flattenBoundaryDefaultsToFalseAndIsCopied() {
        AllHitsQueryConfig config = new AllHitsQueryConfig();
        assertFalse(config.isFlattenBoundary());
        config.setFlattenBoundary(true);
        AllHitsQueryConfig copy = new AllHitsQueryConfig(config);
        assertEquals(config, copy);
        assertEquals(config.hashCode(), copy.hashCode());
    }

    @Test
    public void injectedDependenciesParticipateInEquality() {
        AllHitsQueryConfig first = new AllHitsQueryConfig();
        AllHitsQueryConfig second = new AllHitsQueryConfig();
        KeywordParser parser = keywords -> null;
        QueryExpressionExtractor extractor = query -> null;
        first.setKeywordParser(parser);
        first.setQueryExpressionExtractor(extractor);
        second.setKeywordParser(parser);
        second.setQueryExpressionExtractor(extractor);
        assertEquals(first, second);
        second.setKeywordParser(query -> null);
        assertNotEquals(first, second);
    }
}
