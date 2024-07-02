package datawave.query.language.analyzers.lucene;

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BrazilianLuceneAnalyzerTest extends BaseLuceneTest {

    @BeforeEach
    public void setup() {
        analyzer = new BrazilianLuceneAnalyzer();
    }

    @Test
    public void testSimplePhrase() {
        // brazilian jui jitsu
        // jiu jitsu brasileiro
        inputs.put("jiu", Set.of());
        inputs.put("jitsu", Set.of());
        inputs.put("brasileiro", Set.of("brasileir"));
        test(inputs);
    }

}
