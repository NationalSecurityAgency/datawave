package datawave.query.language.analyzers.lucene;

import java.util.Collections;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RomanianLuceneAnalyzerTest extends BaseLuceneTest {

    @BeforeEach
    public void setup() {
        analyzer = new RomanianLuceneAnalyzer();
    }

    @Test
    public void testPhrase() {
        // flying in the clouds
        // zburând în nori
        inputs.put("zburând", Set.of("zbur"));
        inputs.put("în", Collections.emptySet());
        inputs.put("nori", Set.of("nor"));
        test(inputs);
    }

}
