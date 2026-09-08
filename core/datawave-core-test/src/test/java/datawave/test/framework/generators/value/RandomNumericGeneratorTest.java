package datawave.test.framework.generators.value;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RandomNumericGeneratorTest {

    private static final Logger log = LoggerFactory.getLogger(RandomNumericGeneratorTest.class);

    private static final long SEED = 42L;

    private static final int DEFAULT_LENGTH = 4;

    @Test
    public void testGenerate() {
        ValueGenerator<Integer> generator = RandomNumericGenerator.create(new Random(SEED));
        validate(generator, DEFAULT_LENGTH);
    }

    @Test
    public void testGenerateWithLength() {
        ValueGenerator<Integer> generator = RandomNumericGenerator.create(6, new Random(SEED));
        validate(generator, 6);
    }

    @Test
    public void testSingleDigitLength() {
        ValueGenerator<Integer> generator = RandomNumericGenerator.create(1, new Random(SEED));
        for (int i = 0; i < 25; i++) {
            assertEquals(1, generator.next().toString().length());
        }
    }

    private void validate(ValueGenerator<Integer> generator, int length) {
        Set<Integer> results = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            Integer value = generator.next();
            log.info("value: {}", value);
            assertEquals(length, value.toString().length(), "Generator produced a value of the wrong digit count");
            results.add(value);
        }
        assertEquals(10, results.size(), 1, "Generator produced too many duplicate values");
    }
}
