package datawave.query.language.analyzers.lucene;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

class LuceneAnalyzerFactoryTest {

    private final LuceneAnalyzerFactory factory = new LuceneAnalyzerFactory();

    @Test
    public void testSingleAnalyzer() {
        List<LanguageAwareAnalyzer> analyzers = factory.createAnalyzers(Set.of("en"));
        assertInstanceOf(EnglishLuceneAnalyzer.class, analyzers.get(0));
    }

    @Test
    public void testTwoAnalyzers() {
        List<LanguageAwareAnalyzer> analyzers = factory.createAnalyzers(Set.of("en", "es"));
        assertInstanceOf(EnglishLuceneAnalyzer.class, analyzers.get(0));
        assertInstanceOf(SpanishLuceneAnalyzer.class, analyzers.get(1));
    }

    @Test
    public void testAllAnalyzers() {
        List<LanguageAwareAnalyzer> analyzers = factory.createAnalyzers(Set.of("all"));
        assertEquals(32, analyzers.size());
    }

    @Test
    public void testAnalyzerReuse() {
        LuceneAnalyzerFactory localFactory = new LuceneAnalyzerFactory();
        LanguageAwareAnalyzer first = localFactory.createAnalyzer("en");
        LanguageAwareAnalyzer second = localFactory.createAnalyzer("en");
        assertNotSame(first, second);
        assertNotEquals(first, second);
    }
}
