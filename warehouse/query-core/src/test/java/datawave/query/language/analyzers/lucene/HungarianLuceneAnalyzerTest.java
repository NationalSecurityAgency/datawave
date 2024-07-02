package datawave.query.language.analyzers.lucene;

import java.util.Collections;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HungarianLuceneAnalyzerTest extends BaseLuceneTest {

    @BeforeEach
    public void setup() {
        analyzer = new HungarianLuceneAnalyzer();
    }

    @Test
    public void testPhrase() {
        // migrating tribes
        // vándorló törzsek
        inputs.put("vándorló", Collections.emptySet());
        inputs.put("törzsek", Set.of("törzs"));
        test(inputs);
    }

}
