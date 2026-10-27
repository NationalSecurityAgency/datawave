package datawave.test.framework.generators.value;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Random;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PhraseGeneratorTest {

    private static final Logger log = LoggerFactory.getLogger(PhraseGeneratorTest.class);

    private static final long SEED = 42L;

    @Test
    public void testPhrases() {
        ValueGenerator<String> generator = PhraseGenerator.create(new Random(SEED));
        for (int i = 0; i < 5; i++) {
            String phrase = generator.next();
            log.info("{}", phrase);
            assertEquals(5, phrase.split(" ").length);
        }
    }
}
