package datawave.query.language.analyzers.lucene;

import java.util.Collections;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NorwegianLuceneAnalysisTest extends BaseLuceneTest {

    @BeforeEach
    public void setup() {
        analyzer = new NorwegianLuceneAnalysis();
    }

    @Test
    public void testPhrase() {
        // baby seals are cute
        // babyseler er søte
        inputs.put("babyseler", Set.of("babysel"));
        inputs.put("er", Collections.emptySet());
        inputs.put("søte", Set.of("søt"));
        test(inputs);
    }
}
