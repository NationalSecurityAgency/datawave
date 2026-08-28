package datawave.test.framework.generators.value;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PresetGeneratorTest {

    private static final Logger log = LoggerFactory.getLogger(PresetGeneratorTest.class);

    @Test
    public void testGenerate() {
        List<String> values = List.of("aa", "bb", "cc");
        ValueGenerator<String> generator = PresetGenerator.of(values);

        List<String> results = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            String next = generator.next();
            log.info("next: {}", next);
            results.add(next);
        }

        List<String> expected = List.of("aa", "bb", "cc", "aa", "bb", "cc");
        assertEquals(expected, results);
    }
}
