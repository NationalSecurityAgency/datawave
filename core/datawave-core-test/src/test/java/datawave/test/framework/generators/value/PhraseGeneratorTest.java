package datawave.test.framework.generators.value;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PhraseGeneratorTest {

    private static final Logger log = LoggerFactory.getLogger(PhraseGeneratorTest.class);

    @Test
    public void testPhrases() {
        ValueGenerator<String> generator = PhraseGenerator.create();
        for (int i = 0; i < 5; i++) {
            String phrase = generator.next();
            log.info("{}", phrase);
            assertEquals(5, phrase.split(" ").length);
        }
    }
}
