package datawave.test.framework.generators.value;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RandomNumericGeneratorTest {

    private static final Logger log = LoggerFactory.getLogger(RandomNumericGeneratorTest.class);

    @Test
    public void testGenerate() {
        ValueGenerator<Integer> generator = RandomNumericGenerator.create();
        validate(generator);
    }

    @Test
    public void testGenerateWithLength() {
        ValueGenerator<Integer> generator = RandomNumericGenerator.create(6);
        validate(generator);
    }

    private void validate(ValueGenerator<Integer> generator) {
        Set<Integer> results = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            Integer value = generator.next();
            log.info("value: {}", value);
            results.add(value);
        }
        assertEquals(10, results.size(), 1, "Generator produced too many duplicate values");
    }
}
