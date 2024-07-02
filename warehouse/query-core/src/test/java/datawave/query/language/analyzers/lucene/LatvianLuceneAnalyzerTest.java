package datawave.query.language.analyzers.lucene;

import java.util.Collections;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LatvianLuceneAnalyzerTest extends BaseLuceneTest {

    @BeforeEach
    public void setup() {
        analyzer = new LatvianLuceneAnalyzer();
    }

    @Test
    public void testPhrase() {
        // baby seals are cute
        // roņu mazuļi ir mīļi
        inputs.put("roņu", Set.of("ron"));
        inputs.put("mazuļi", Set.of("mazul"));
        inputs.put("ir", Collections.emptySet());
        inputs.put("mīļi", Set.of("mīl"));
        test(inputs);
    }

}
