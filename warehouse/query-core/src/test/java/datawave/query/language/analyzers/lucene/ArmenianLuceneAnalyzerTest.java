package datawave.query.language.analyzers.lucene;

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ArmenianLuceneAnalyzerTest extends BaseLuceneTest {

    @BeforeEach
    public void setup() {
        analyzer = new ArmenianLuceneAnalyzer();
    }

    @Test
    public void testSimplePhrase() {
        // the mountains and clear skies
        // lerrnery yev parz yerkink’y
        // լեռները և պարզ երկինքը
        inputs.put("լեռները", Set.of());
        inputs.put("և", Set.of());
        inputs.put("պարզ", Set.of());
        inputs.put("երկինքը", Set.of("երկին"));
        test(inputs);
    }

}
