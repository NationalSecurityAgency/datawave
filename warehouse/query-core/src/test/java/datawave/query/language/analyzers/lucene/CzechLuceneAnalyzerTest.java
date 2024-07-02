package datawave.query.language.analyzers.lucene;

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CzechLuceneAnalyzerTest extends BaseLuceneTest {

    @BeforeEach
    public void setup() {
        analyzer = new CzechLuceneAnalyzer();
    }

    @Test
    public void testSimplePhrase() {
        // chess players
        // šachisté
        inputs.put("šachisté", Set.of("šachist"));
        test(inputs);
    }
}
