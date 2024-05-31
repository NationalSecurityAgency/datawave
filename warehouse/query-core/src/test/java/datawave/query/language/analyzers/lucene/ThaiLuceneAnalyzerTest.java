package datawave.query.language.analyzers.lucene;

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ThaiLuceneAnalyzerTest extends BaseLuceneTest {

    @BeforeEach
    public void setup() {
        analyzer = new ThaiLuceneAnalyzer();
    }

    @Test
    public void testPhrase() {
        // thai iced tea
        // Chā yĕn thịy
        // ชาเย็นไทย
        inputs.put("ชาเย็นไทย", Set.of("ชา", "เย็น", "ไทย"));
        test(inputs);
    }

}
