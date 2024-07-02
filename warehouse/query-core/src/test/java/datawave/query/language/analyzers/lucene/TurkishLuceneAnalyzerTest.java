package datawave.query.language.analyzers.lucene;

import java.util.Collections;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TurkishLuceneAnalyzerTest extends BaseLuceneTest {

    @BeforeEach
    public void setup() {
        analyzer = new TurkishLuceneAnalyzer();
    }

    @Test
    public void testPhrase() {
        // turkish delight is delightful
        // türk lokumu çok güzel
        inputs.put("türk", Collections.emptySet());
        inputs.put("lokumu", Set.of("lok"));
        inputs.put("çok", Collections.emptySet());
        inputs.put("güzel", Collections.emptySet());
        test(inputs);
    }

}
