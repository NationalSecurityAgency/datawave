package datawave.query.language.analyzers.lucene;

import java.util.Collections;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ItalianLuceneAnalyzerTest extends BaseLuceneTest {

    @BeforeEach
    public void setup() {
        analyzer = new ItalianLuceneAnalyzer();
    }

    @Test
    public void testPhrase() {
        // vengeful dead poets
        // poeti morti vendicativi
        inputs.put("poeti", Collections.emptySet());
        inputs.put("morti", Collections.emptySet());
        inputs.put("vendicativi", Set.of("vendicativ"));
        test(inputs);
    }

}
