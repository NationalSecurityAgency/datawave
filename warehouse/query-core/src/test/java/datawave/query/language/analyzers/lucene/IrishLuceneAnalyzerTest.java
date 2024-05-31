package datawave.query.language.analyzers.lucene;

import java.util.Collections;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IrishLuceneAnalyzerTest extends BaseLuceneTest {

    @BeforeEach
    public void setup() {
        analyzer = new IrishLuceneAnalyzer();
    }

    @Test
    public void testPhrase() {
        // sailing navigators
        // seoltóirí seoltóireachta
        inputs.put("seoltóirí", Collections.emptySet());
        inputs.put("seoltóireachta", Set.of("seoltóir"));
        test(inputs);
    }

}
