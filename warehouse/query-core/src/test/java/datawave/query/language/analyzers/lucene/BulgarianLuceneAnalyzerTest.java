package datawave.query.language.analyzers.lucene;

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BulgarianLuceneAnalyzerTest extends BaseLuceneTest {

    @BeforeEach
    public void setup() {
        analyzer = new BulgarianLuceneAnalyzer();
    }

    @Test
    public void testSimplePhrase() {
        // exercising in the park
        // uprazhneniya v parka
        // упражнения в парка
        inputs.put("упражнения", Set.of("упражнн"));
        inputs.put("в", Set.of());
        inputs.put("парка", Set.of("парк"));
        test(inputs);
    }
}
