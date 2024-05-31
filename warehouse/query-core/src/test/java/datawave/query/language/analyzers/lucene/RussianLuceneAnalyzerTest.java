package datawave.query.language.analyzers.lucene;

import java.util.Collections;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RussianLuceneAnalyzerTest extends BaseLuceneTest {

    @BeforeEach
    public void setup() {
        analyzer = new RussianLuceneAnalyzer();
    }

    @Test
    public void testSimplePhrase() {
        // whiskey neat, coffee black, bed at three
        inputs.put("чистый", Set.of("чист"));
        inputs.put("виски", Set.of("виск"));
        inputs.put("черный", Set.of("черн"));
        inputs.put("кофе", Set.of("коф"));
        inputs.put("кровать", Set.of("крова"));
        inputs.put("в", Collections.emptySet());
        inputs.put("3", Collections.emptySet());
        test(inputs);
    }
}
