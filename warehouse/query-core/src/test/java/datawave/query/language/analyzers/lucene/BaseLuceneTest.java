package datawave.query.language.analyzers.lucene;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class that makes tests for {@link LuceneAnalyzer}s easier to write
 */
public class BaseLuceneTest {

    private static final Logger log = LoggerFactory.getLogger(BaseLuceneTest.class);

    protected LanguageAwareAnalyzer analyzer;

    protected Map<String,Set<String>> inputs = new HashMap<>();

    /**
     * Test a map of defined inputs and their expected outputs
     *
     * @param inputs
     *            inputs and expectations
     */
    protected void test(Map<String,Set<String>> inputs) {
        assertFalse(inputs.isEmpty());
        inputs.keySet().forEach(input -> test(input, inputs.get(input)));
        inputs.clear();
    }

    /**
     * Test a token against it's expected outputs
     *
     * @param text
     *            the input
     * @param expected
     *            the expected outputs
     */
    protected void test(String text, Set<String> expected) {
        assertNotNull(analyzer, "Analyzer should not be null");
        Set<String> alternates = analyzer.findAlternates("FIELD", text);
        log.info("token: {} alternates: {}", text, alternates);
        assertEquals(expected, alternates);
    }
}
