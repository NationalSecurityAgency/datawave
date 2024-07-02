package datawave.query.language.analyzers.lucene;

import java.util.Collections;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GalicianLuceneAnalyzerTest extends BaseLuceneTest {

    @BeforeEach
    public void setup() {
        analyzer = new GalicianLuceneAnalyzer();
    }

    @Test
    public void testPhrase() {
        // starry night sky
        // ceo nocturno estrelado
        inputs.put("ceo", Collections.emptySet());
        inputs.put("nocturno", Set.of("nocturn"));
        inputs.put("estrelado", Set.of("estrel"));
        test(inputs);
    }

}
