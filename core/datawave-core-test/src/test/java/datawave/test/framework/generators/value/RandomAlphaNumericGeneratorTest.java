package datawave.test.framework.generators.value;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RandomAlphaNumericGeneratorTest {

    private static final Logger log = LoggerFactory.getLogger(RandomAlphaNumericGeneratorTest.class);

    private static final long SEED = 42L;

    @Test
    public void testGenerate() {
        ValueGenerator<String> generator = RandomAlphaNumericGenerator.create(new Random(SEED));
        validate(generator);
    }

    @Test
    public void testGenerateWithLength() {
        ValueGenerator<String> generator = RandomAlphaNumericGenerator.create(12, new Random(SEED));
        validate(generator);
    }

    private void validate(ValueGenerator<String> generator) {
        Set<String> results = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            String value = generator.next();
            log.info("value: {}", value);
            results.add(value);
        }
        assertEquals(10, results.size(), 1, "Generator produced too many duplicate values");
    }
}
