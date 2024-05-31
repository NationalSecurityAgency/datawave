package datawave.query.language.analyzers.lucene;

import java.util.Collections;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PersianLuceneAnalyzerTest extends BaseLuceneTest {

    @BeforeEach
    public void setup() {
        analyzer = new PersianLuceneAnalyzer();
    }

    @Test
    public void testPhrase() {
        // remember the athenians
        // به یاد آتنی ها
        inputs.put("به", Collections.emptySet());
        inputs.put("یاد", Set.of("ياد"));
        inputs.put("آتنی", Set.of("اتني"));
        inputs.put("ها", Collections.emptySet());
        test(inputs);
    }

}
