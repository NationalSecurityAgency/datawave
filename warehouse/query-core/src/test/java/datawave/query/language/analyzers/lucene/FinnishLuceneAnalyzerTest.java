package datawave.query.language.analyzers.lucene;

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FinnishLuceneAnalyzerTest extends BaseLuceneTest {

    @BeforeEach
    public void setup() {
        analyzer = new FinnishLuceneAnalyzer();
    }

    @Test
    public void testFinnishPhrase() {
        // skiers skiing across the snow
        // hiihtäjiä hiihtämässä lumen poikki
        inputs.put("hiihtäjiä", Set.of("hiihtäj"));
        inputs.put("hiihtämässä", Set.of("hiihtäm"));
        inputs.put("lumen", Set.of("lume"));
        inputs.put("poikki", Set.of());
        test(inputs);
    }
}
