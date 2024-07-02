package datawave.query.language.analyzers.lucene;

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GreekLuceneAnalyzerTest extends BaseLuceneTest {

    @BeforeEach
    public void setup() {
        analyzer = new GreekLuceneAnalyzer();
    }

    @Test
    public void testPhrase() {
        // city states
        // póleis kráti
        // πόλεις κράτη
        inputs.put("πόλεις", Set.of("πολ"));
        inputs.put("κράτη", Set.of("κρατ"));
        test(inputs);
    }

}
