package datawave.query.language.analyzers.lucene;

import java.util.Collections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SwedishLuceneAnalyzerTest extends BaseLuceneTest {

    @BeforeEach
    public void setup() {
        analyzer = new SwedishLuceneAnalyzer();
    }

    @Test
    public void testPhrase() {
        // swedish self assembly
        // svensk självmontering
        inputs.put("svensk", Collections.emptySet());
        inputs.put("självmontering", Collections.emptySet());
        test(inputs);
    }

}
