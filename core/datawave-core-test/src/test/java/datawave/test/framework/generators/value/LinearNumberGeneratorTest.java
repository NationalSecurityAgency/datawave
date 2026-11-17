package datawave.test.framework.generators.value;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

public class LinearNumberGeneratorTest {

    @Test
    public void testGenerate() {
        ValueGenerator<Integer> generator = LinearNumberGenerator.create();

        List<Integer> results = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            results.add(generator.next());
        }

        List<Integer> expected = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        assertEquals(expected, results);
    }

}
