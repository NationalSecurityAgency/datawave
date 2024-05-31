package datawave.query.language.analyzers.lucene;

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DanishLuceneAnalyzerTest extends BaseLuceneTest {

    @BeforeEach
    public void setup() {
        analyzer = new DanishLuceneAnalyzer();
    }

    @Test
    public void testDanishPhrase() {
        // saxons or vikings
        // saksere eller vikinger
        inputs.put("saksere", Set.of("saks"));
        inputs.put("eller", Set.of());
        inputs.put("vikinger", Set.of("viking"));
        test(inputs);
    }
}
