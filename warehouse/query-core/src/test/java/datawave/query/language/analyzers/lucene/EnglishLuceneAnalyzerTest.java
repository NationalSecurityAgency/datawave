package datawave.query.language.analyzers.lucene;

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EnglishLuceneAnalyzerTest extends BaseLuceneTest {

    @BeforeEach
    public void setup() {
        analyzer = new EnglishLuceneAnalyzer();
    }

    @Test
    public void testSimplePhrase() {
        // the scientist broke the broken beaker
        inputs.put("the", Set.of());
        inputs.put("dogs", Set.of("dog"));
        inputs.put("chased", Set.of("chase"));
        inputs.put("cats", Set.of("cat"));
        test(inputs);
    }
}
