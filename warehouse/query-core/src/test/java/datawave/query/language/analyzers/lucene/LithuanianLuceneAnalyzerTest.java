package datawave.query.language.analyzers.lucene;

import java.util.Collections;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LithuanianLuceneAnalyzerTest extends BaseLuceneTest {

    @BeforeEach
    public void setup() {
        analyzer = new LithuanianLuceneAnalyzer();
    }

    @Test
    public void testPhrase() {
        // baby seals are cute
        // ruonių kūdikiai yra mieli
        inputs.put("ruonių", Set.of("ruon"));
        inputs.put("kūdikiai", Set.of("kūdik"));
        inputs.put("yra", Collections.emptySet());
        inputs.put("mieli", Set.of("miel"));
        test(inputs);
    }

}
