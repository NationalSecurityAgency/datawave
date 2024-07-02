package datawave.query.language.analyzers.lucene;

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BengaliLuceneAnalyzerTest extends BaseLuceneTest {

    @BeforeEach
    public void setup() {
        analyzer = new BengaliLuceneAnalyzer();
    }

    @Test
    public void testSimplePhrase() {
        // lions tigers and bears
        // Sinha bāgha ēbaṁ bhāluka
        // সিংহ বাঘ এবং ভালুক
        inputs.put("সিংহ", Set.of());
        inputs.put("বাঘ", Set.of());
        inputs.put("এবং", Set.of());
        inputs.put("ভালুক", Set.of("ভাল"));
        test(inputs);
    }
}
