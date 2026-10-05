package datawave.test.framework.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

public class InfiniteIteratorTest {

    @Test
    public void testIterator() {
        List<String> elements = List.of("a", "b", "c");
        InfiniteIterator<String> infiniteIterator = new InfiniteIterator<>(elements);

        List<String> expected = List.of("a", "b", "c", "a", "b", "c", "a", "b", "c");

        for (String s : expected) {
            assertTrue(infiniteIterator.hasNext(), "InfiniteIterator should always be true");
            assertEquals(s, infiniteIterator.next());
        }
    }

    /**
     * An empty list has nothing to cycle through, and {@code hasNext()} always says otherwise, so the failure surfaces at construction rather than as a divide
     * by zero on the first call to {@code next()}.
     */
    @Test
    public void testNullOrEmptyElementsAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> new InfiniteIterator<>(null));
        assertThrows(IllegalArgumentException.class, () -> new InfiniteIterator<>(List.of()));
    }
}
