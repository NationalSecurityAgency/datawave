package datawave.query.language.analyzers.lucene;

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IndonesianLuceneAnalyzerTest extends BaseLuceneTest {

    @BeforeEach
    public void setup() {
        analyzer = new IndonesianLuceneAnalyzer();
    }

    @Test
    public void testPhrase() {
        // islands for days
        // pulau selama berhari-hari
        inputs.put("pulau", Set.of());
        inputs.put("selama", Set.of());
        inputs.put("berhari-hari", Set.of("hari"));
        test(inputs);
    }

}
