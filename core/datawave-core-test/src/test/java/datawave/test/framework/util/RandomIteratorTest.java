package datawave.test.framework.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

public class RandomIteratorTest {

    @Test
    public void testIterator() {
        List<String> elements = List.of("a", "b", "c", "d", "e");
        RandomIterator<String> iterator = new RandomIterator<>(new Random(42L), elements);
        for (int i = 0; i < 10; i++) {
            assertTrue(iterator.hasNext());
            assertTrue(elements.contains(iterator.next()));
        }
    }

    @Test
    public void testSeededIterator() {
        List<String> elements = List.of("a", "b", "c", "d", "e");

        long seed = 1234567890L;
        RandomIterator<String> first = new RandomIterator<>(seed, elements);
        RandomIterator<String> second = new RandomIterator<>(seed, elements);

        for (int i = 0; i < 100; i++) {
            String a = first.next();
            String b = second.next();
            assertEquals(a, b);
        }
    }
}
