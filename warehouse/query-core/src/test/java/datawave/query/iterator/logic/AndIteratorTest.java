package datawave.query.iterator.logic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;

import datawave.query.attributes.Document;
import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.logic.NegationFilterTest.InterruptedIterable;

public class AndIteratorTest {

    @BeforeEach
    public void setup() {}

    @Test
    public void testSingleInclude() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b"), false));

        AndIterator iterator = new AndIterator(includes);
        iterator.initialize();

        assertFalse(iterator.isContextRequired());
        assertTrue(iterator.hasNext());
        assertEquals("a", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("b", iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testMultiInclude() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b", "e", "g"), false));
        includes.add(getItr(Lists.newArrayList("a", "b", "c", "d", "e", "f"), false));

        AndIterator iterator = new AndIterator(includes);
        iterator.initialize();

        assertFalse(iterator.isContextRequired());
        assertTrue(iterator.hasNext());
        assertEquals("a", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("b", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("e", iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testMultiIncludeContextRequired() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b", "c"), false));
        includes.add(getItr(Lists.newArrayList("b", "c", "d"), true));

        AndIterator iterator = new AndIterator(includes);
        iterator.initialize();

        assertFalse(iterator.isContextRequired());
        assertTrue(iterator.hasNext());
        assertEquals("b", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("c", iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testExcludeFirstMatch() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b"), false));
        includes.add(getItr(Lists.newArrayList("a", "b", "c", "d", "e", "f"), false));

        Set<NestedIterator<String>> excludes = new HashSet<>();
        excludes.add(getItr(Lists.newArrayList("a", "c", "e", "f"), false));

        AndIterator iterator = new AndIterator(includes, excludes);
        iterator.initialize();

        assertFalse(iterator.isContextRequired());
        assertTrue(iterator.hasNext());
        assertEquals("b", iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testExcludeSecondMatch() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b"), false));
        includes.add(getItr(Lists.newArrayList("a", "b", "c", "d", "e", "f"), false));

        Set<NestedIterator<String>> excludes = new HashSet<>();
        excludes.add(getItr(Lists.newArrayList("b", "c", "e", "f"), false));

        AndIterator iterator = new AndIterator(includes, excludes);
        iterator.initialize();

        assertFalse(iterator.isContextRequired());
        assertTrue(iterator.hasNext());
        assertEquals("a", iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testExcludeLastMatch() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b", "f"), false));
        includes.add(getItr(Lists.newArrayList("a", "b", "c", "d", "e", "f"), false));

        Set<NestedIterator<String>> excludes = new HashSet<>();
        excludes.add(getItr(Lists.newArrayList("c", "e", "f"), false));

        AndIterator iterator = new AndIterator(includes, excludes);
        iterator.initialize();

        assertFalse(iterator.isContextRequired());
        assertTrue(iterator.hasNext());
        assertEquals("a", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("b", iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testExcludeNoMatch() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b", "f"), false));
        includes.add(getItr(Lists.newArrayList("a", "b", "c", "d", "e", "f"), false));

        Set<NestedIterator<String>> excludes = new HashSet<>();
        excludes.add(getItr(Lists.newArrayList("g"), false));

        AndIterator iterator = new AndIterator(includes, excludes);
        iterator.initialize();

        assertFalse(iterator.isContextRequired());
        assertTrue(iterator.hasNext());
        assertEquals("a", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("b", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("f", iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testExcludeEmpty() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b", "f"), false));
        includes.add(getItr(Lists.newArrayList("a", "b", "c", "d", "e", "f"), false));

        Set<NestedIterator<String>> excludes = new HashSet<>();
        excludes.add(getItr(Lists.newArrayList(), false));

        AndIterator iterator = new AndIterator(includes, excludes);
        iterator.initialize();

        assertFalse(iterator.isContextRequired());
        assertTrue(iterator.hasNext());
        assertEquals("a", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("b", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("f", iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testDeferred() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b"), false));
        includes.add(getItr(Lists.newArrayList("b"), true));

        AndIterator iterator = new AndIterator(includes);
        iterator.initialize();

        assertFalse(iterator.isContextRequired());
        assertTrue(iterator.hasNext());
        assertEquals("b", iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testNegatedDeferred() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b"), false));

        Set<NestedIterator<String>> excludes = new HashSet<>();
        excludes.add(getItr(Lists.newArrayList("b"), true));

        AndIterator iterator = new AndIterator(includes, excludes);
        iterator.initialize();

        assertFalse(iterator.isContextRequired());
        assertTrue(iterator.hasNext());
        assertEquals("a", iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testDeferredChild() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b"), true));
        includes.add(getItr(Lists.newArrayList("b", "c"), true));

        AndIterator unsourcedIterator = new AndIterator(includes);

        includes = new HashSet<>();
        includes.add(unsourcedIterator);
        includes.add(getItr(Lists.newArrayList("c"), false));

        AndIterator iterator = new AndIterator(includes);
        iterator.initialize();

        assertFalse(iterator.isContextRequired());
        assertTrue(unsourcedIterator.isContextRequired());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testDeferredNegatedChild() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        Set<NestedIterator<String>> excludes = new HashSet<>();
        excludes.add(getItr(Lists.newArrayList("a", "b"), true));
        excludes.add(getItr(Lists.newArrayList("b", "c"), true));

        AndIterator unsourcedIterator = new AndIterator(includes, excludes);

        includes = new HashSet<>();
        includes.add(unsourcedIterator);
        includes.add(getItr(Lists.newArrayList("c"), false));

        AndIterator iterator = new AndIterator(includes);
        iterator.initialize();

        assertFalse(iterator.isContextRequired());
        assertTrue(unsourcedIterator.isContextRequired());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testContextWhenNotRequiredMoveAll() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("c", "d", "e"), false));
        includes.add(getItr(Lists.newArrayList("c", "d", "e"), false));

        AndIterator iterator = new AndIterator(includes);
        iterator.initialize();

        assertFalse(iterator.isContextRequired());
        iterator.setContext("e");
        assertTrue(iterator.hasNext());
        assertEquals("c", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("e", iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testContextWhenNotRequiredShortCircuit() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("c", "s", "z"), false));
        includes.add(getItr(Lists.newArrayList("c", "z"), false));

        AndIterator iterator = new AndIterator(includes);
        iterator.initialize();

        assertFalse(iterator.isContextRequired());
        iterator.setContext("d");
        assertTrue(iterator.hasNext());
        assertEquals("c", iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testContextWhenNotRequiredSkipLowestMoveLowest() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("c", "s", "z"), false));
        includes.add(getItr(Lists.newArrayList("c", "z"), false));

        AndIterator iterator = new AndIterator(includes);
        iterator.initialize();

        assertFalse(iterator.isContextRequired());
        iterator.setContext("z");
        assertTrue(iterator.hasNext());
        assertEquals("c", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("z", iterator.next());
        assertFalse(iterator.hasNext());
    }

    /**
     * This test triggers the "Failed include lookup, but dropping in lieu of other terms" warning
     * <p>
     * The limitations of this test framework prevent us from asserting the resulting document.
     */
    @Test
    public void testFailedIncludeLookup() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b", "c"), false));
        includes.add(getInterruptableItr(Lists.newArrayList("a", "b", "c"), false));

        AndIterator<String> iterator = new AndIterator<>(includes);
        iterator.initialize();

        for (String top : List.of("a", "b", "c")) {
            assertTrue(iterator.hasNext());
            assertEquals(top, iterator.next());
            Document d = iterator.document();
            assertNotNull(d);
        }

        // empty document at the end
        assertFalse(iterator.hasNext());
        assertEquals(0, iterator.document().size());
    }

    private NegationFilterTest.Itr<String> getItr(List<String> source, boolean contextRequired) {
        return new NegationFilterTest.Itr<>(source, contextRequired);
    }

    private NegationFilterTest.Itr<String> getInterruptableItr(List<String> source, boolean contextRequired) {
        InterruptedIterable<String> iterable = new InterruptedIterable<>(source.iterator());
        return new NegationFilterTest.Itr<>(iterable, contextRequired);
    }
}
