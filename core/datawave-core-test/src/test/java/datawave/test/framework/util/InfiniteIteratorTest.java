package datawave.test.framework.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
}
