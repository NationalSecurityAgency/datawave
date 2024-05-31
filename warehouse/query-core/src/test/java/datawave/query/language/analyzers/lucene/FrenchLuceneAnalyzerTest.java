package datawave.query.language.analyzers.lucene;

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FrenchLuceneAnalyzerTest extends BaseLuceneTest {

    @BeforeEach
    public void setup() {
        analyzer = new FrenchLuceneAnalyzer();
    }

    @Test
    public void testPhrase() {
        // city of lights
        // ville des lumières
        inputs.put("ville", Set.of("vile"));
        inputs.put("des", Set.of());
        inputs.put("lumières", Set.of("lumier"));
        test(inputs);

        // D'Artagnan is a musketeer
        // D'Artagnan est mousquetaire
        inputs.put("D'Artagnan", Set.of("artagnan"));
        inputs.put("est", Set.of());
        inputs.put("mousquetaire", Set.of("mousquetair"));
        test(inputs);
    }
}
