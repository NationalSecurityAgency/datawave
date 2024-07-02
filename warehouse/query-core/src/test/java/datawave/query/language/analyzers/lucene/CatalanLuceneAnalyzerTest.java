package datawave.query.language.analyzers.lucene;

import java.util.Collections;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CatalanLuceneAnalyzerTest extends BaseLuceneTest {

    @BeforeEach
    public void setup() {
        analyzer = new CatalanLuceneAnalyzer();
    }

    @Test
    public void testSimplePhrase() {
        // empires rise kingdoms fall
        // imperiite se izdigat kralstvata padat
        // империите се издигат кралствата падат
        inputs.put("imperiite", Set.of("imperiit"));
        inputs.put("se", Collections.emptySet());
        inputs.put("izdigat", Set.of("izdig"));
        inputs.put("kralstvata", Set.of("kralstvat"));
        inputs.put("padat", Set.of("pad"));
        test(inputs);
    }
}
