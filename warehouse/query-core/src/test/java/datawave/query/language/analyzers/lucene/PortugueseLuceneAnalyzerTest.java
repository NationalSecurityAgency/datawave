package datawave.query.language.analyzers.lucene;

import java.util.Collections;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PortugueseLuceneAnalyzerTest extends BaseLuceneTest {

    @BeforeEach
    public void setup() {
        analyzer = new PortugueseLuceneAnalyzer();
    }

    @Test
    public void testPhrase() {
        // sailing around the world
        // navegando pelo mundo
        inputs.put("navegando", Set.of("navegand"));
        inputs.put("pelo", Collections.emptySet());
        inputs.put("mundo", Set.of("mund"));
        test(inputs);
    }

}
