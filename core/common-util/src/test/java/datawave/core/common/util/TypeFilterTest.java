package datawave.core.common.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

public class TypeFilterTest {

    @Test
    public void testNoOpConstructor() {
        TypeFilter filter = new TypeFilter();
        assertTrue(filter.accept("a"));
        assertTrue(filter.accept("b"));
        assertTrue(filter.accept("c"));
        assertTrue(filter.contains("a"));
        assertTrue(filter.contains("b"));
        assertTrue(filter.contains("c"));

        // round trip serialize and deserialize
        String data = filter.toString();
        assertEquals("*", data);
        filter = TypeFilter.fromString(data);

        // verify consistent behavior
        assertTrue(filter.accept("a"));
        assertTrue(filter.accept("b"));
        assertTrue(filter.accept("c"));
        assertTrue(filter.contains("a"));
        assertTrue(filter.contains("b"));
        assertTrue(filter.contains("c"));
    }

    @Test
    public void testNullFilter() {
        TypeFilter filter = new TypeFilter(null);
        assertTrue(filter.accept("a"));
        assertTrue(filter.accept("b"));
        assertTrue(filter.accept("c"));
        assertTrue(filter.contains("a"));
        assertTrue(filter.contains("b"));
        assertTrue(filter.contains("c"));

        // round trip serialize and deserialize
        String data = filter.toString();
        assertEquals("*", data);
        filter = TypeFilter.fromString(data);

        // verify consistent behavior
        assertTrue(filter.accept("a"));
        assertTrue(filter.accept("b"));
        assertTrue(filter.accept("c"));
        assertTrue(filter.contains("a"));
        assertTrue(filter.contains("b"));
        assertTrue(filter.contains("c"));
    }

    @Test
    public void testEmptyFilter() {
        TypeFilter filter = new TypeFilter(Collections.emptySet());
        assertTrue(filter.accept("a"));
        assertTrue(filter.accept("b"));
        assertTrue(filter.accept("c"));
        assertTrue(filter.contains("a"));
        assertTrue(filter.contains("b"));
        assertTrue(filter.contains("c"));

        // round trip serialize and deserialize
        String data = filter.toString();
        assertEquals("*", data);
        filter = TypeFilter.fromString(data);

        // verify consistent behavior
        assertTrue(filter.accept("a"));
        assertTrue(filter.accept("b"));
        assertTrue(filter.accept("c"));
        assertTrue(filter.contains("a"));
        assertTrue(filter.contains("b"));
        assertTrue(filter.contains("c"));
    }

    @Test
    public void testRequestedTypes() {
        TypeFilter filter = new TypeFilter(List.of("a", "b"));
        assertTrue(filter.accept("a"));
        assertTrue(filter.accept("b"));
        assertFalse(filter.accept("c"));
        assertTrue(filter.contains("a"));
        assertTrue(filter.contains("b"));
        assertFalse(filter.contains("c"));

        String data = filter.toString();
        assertEquals("a,b", data);
        filter = TypeFilter.fromString(data);

        assertTrue(filter.accept("a"));
        assertTrue(filter.accept("b"));
        assertFalse(filter.accept("c"));
        assertTrue(filter.contains("a"));
        assertTrue(filter.contains("b"));
        assertFalse(filter.contains("c"));
    }

    @Test
    public void testCopy() {
        TypeFilter filter = new TypeFilter(List.of("a", "b"));
        TypeFilter other = filter.copy();
        assertNotSame(filter, other);
        assertEquals(filter, other);
    }

    @Test
    public void testEmptyFilterSerDe() {
        TypeFilter filter = new TypeFilter();
        assertTrue(filter.accept("a"));

        String data = filter.toString();
        filter = TypeFilter.fromString(data);
        assertTrue(filter.accept("a"));
    }
}
