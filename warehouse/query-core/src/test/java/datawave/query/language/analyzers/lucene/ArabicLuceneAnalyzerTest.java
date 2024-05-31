package datawave.query.language.analyzers.lucene;

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ArabicLuceneAnalyzerTest extends BaseLuceneTest {

    @BeforeEach
    public void setup() {
        analyzer = new ArabicLuceneAnalyzer();
    }

    @Test
    public void testSimplePhrase() {
        // coffee drinkers in the city
        // sharibi alqahwat fi almadina
        // شاربي القهوة في المدينة
        inputs.put("شاربي", Set.of("شارب"));
        inputs.put("القهوة", Set.of("قهو"));
        inputs.put("في", Set.of());
        inputs.put("المدينة", Set.of("مدين"));
        test(inputs);
    }
}
