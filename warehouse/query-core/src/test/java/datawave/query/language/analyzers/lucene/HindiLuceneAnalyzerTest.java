package datawave.query.language.analyzers.lucene;

import java.util.Collections;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HindiLuceneAnalyzerTest extends BaseLuceneTest {

    @BeforeEach
    public void setup() {
        analyzer = new HindiLuceneAnalyzer();
    }

    @Test
    public void testPhrase() {
        // sailing and fishing
        // naukaayan aur machhalee pakadana
        // नौकायन और मछली पकड़ना
        inputs.put("नौकायन", Set.of("नोकायन"));
        inputs.put("और", Collections.emptySet());
        inputs.put("मछली", Set.of("मछल"));
        inputs.put("पकड़ना", Set.of("पकड"));
        test(inputs);
    }

}
