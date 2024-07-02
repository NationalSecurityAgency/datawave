package datawave.query.language.analyzers.lucene;

import java.util.Collections;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SpanishLuceneAnalyzerTest extends BaseLuceneTest {

    @BeforeEach
    public void setup() {
        analyzer = new SpanishLuceneAnalyzer();
    }

    @Test
    public void testPhrase() {
        // swimming in the seas
        // nadando en los mares
        inputs.put("nadando", Set.of("nadand"));
        inputs.put("en", Collections.emptySet());
        inputs.put("los", Collections.emptySet());
        inputs.put("mares", Set.of("mar"));
        test(inputs);
    }

}
