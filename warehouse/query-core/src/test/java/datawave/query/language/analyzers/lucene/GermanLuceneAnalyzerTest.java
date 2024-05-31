package datawave.query.language.analyzers.lucene;

import java.util.Collections;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GermanLuceneAnalyzerTest extends BaseLuceneTest {

    @BeforeEach
    public void setup() {
        analyzer = new GermanLuceneAnalyzer();
    }

    @Test
    public void testPhrase() {
        // children's chocolate eggs
        // schokoladeneier für kinder
        inputs.put("schokoladeneier", Set.of("schokoladenei"));
        inputs.put("für", Collections.emptySet());
        inputs.put("kinder", Set.of("kind"));
        test(inputs);
    }

}
