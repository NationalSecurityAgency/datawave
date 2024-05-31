package datawave.query.language.analyzers.lucene;

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DutchLuceneAnalyzerTest extends BaseLuceneTest {

    @BeforeEach
    public void setup() {
        analyzer = new DanishLuceneAnalyzer();
    }

    @Test
    public void testDutchPhrase() {
        // all watches safe
        // alle horloges sfae
        inputs.put("alle", Set.of());
        inputs.put("horloges", Set.of("horlog"));
        inputs.put("sfae", Set.of());
        test(inputs);
    }
}
