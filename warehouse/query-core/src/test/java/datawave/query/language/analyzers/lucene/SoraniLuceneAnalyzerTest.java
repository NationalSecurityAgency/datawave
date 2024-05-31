package datawave.query.language.analyzers.lucene;

import java.util.Collections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SoraniLuceneAnalyzerTest extends BaseLuceneTest {

    @BeforeEach
    public void setup() {
        analyzer = new SoraniLuceneAnalyzer();
    }

    @Test
    public void testPhrase() {
        // misty mountains
        // çiyayên mij
        inputs.put("çiyayên", Collections.emptySet());
        inputs.put("mij", Collections.emptySet());
        test(inputs);
    }

}
