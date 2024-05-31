package datawave.query.language.analyzers.lucene;

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BasqueLuceneAnalyzerTest extends BaseLuceneTest {

    @BeforeEach
    public void setup() {
        analyzer = new BasqueLuceneAnalyzer();
    }

    @Test
    public void testSimplePhrase() {
        // basking in the sunny weather
        // eguraldi eguzkitsuan gozatuz
        inputs.put("eguraldi", Set.of("egur"));
        inputs.put("eguzkitsuan", Set.of("egu"));
        inputs.put("gozatuz", Set.of("goza"));
        test(inputs);
    }

}
