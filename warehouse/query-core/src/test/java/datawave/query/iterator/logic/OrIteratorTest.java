package datawave.query.iterator.logic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;

import datawave.query.iterator.NestedIterator;

public class OrIteratorTest {
    @Test
    public void testInclude() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b"), false));

        OrIterator iterator = new OrIterator(includes);
        iterator.initialize();

        assertTrue(iterator.hasNext());
        assertEquals("a", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("b", iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testMultiInclude() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b"), false));
        includes.add(getItr(Lists.newArrayList("b", "d"), false));

        OrIterator iterator = new OrIterator(includes);
        iterator.initialize();

        assertTrue(iterator.hasNext());
        assertEquals("a", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("b", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("d", iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testDeferredInclude() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b"), false));
        includes.add(getItr(Lists.newArrayList("b", "d"), true));

        OrIterator iterator = new OrIterator(includes);
        iterator.setContext("d");
        iterator.initialize();

        assertTrue(iterator.hasNext());
        iterator.setContext("d");
        assertEquals("a", iterator.next());
        assertTrue(iterator.hasNext());
        iterator.setContext("d");
        assertEquals("b", iterator.next());
        assertTrue(iterator.hasNext());
        iterator.setContext("d");
        assertEquals("d", iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testDeferredExclude() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("b", "d"), true));

        OrIterator iterator = new OrIterator(includes);
        iterator.setContext("b");
        iterator.initialize();

        assertTrue(iterator.hasNext());
        iterator.setContext("d");
        assertEquals("b", iterator.next());
        assertTrue(iterator.hasNext());
        iterator.setContext("e");
        assertEquals("d", iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testDeferredExcludeMove() {
        Set<NestedIterator<String>> excludes = new HashSet<>();
        excludes.add(getItr(Lists.newArrayList("b", "d"), true));

        OrIterator iterator = new OrIterator(Collections.emptySet(), excludes);
        iterator.setContext("a");
        iterator.initialize();

        iterator.setContext("a");
        assertEquals("a", iterator.move("a"));

        iterator.setContext("b");
        assertEquals(null, iterator.move("b"));

        iterator.setContext("d");
        assertEquals(null, iterator.move("d"));

        iterator.setContext("e");
        assertEquals("e", iterator.move("e"));
    }

    @Test
    public void testMultipleDeferredExcludeMove() {
        Set<NestedIterator<String>> excludes = new HashSet<>();
        excludes.add(getItr(Lists.newArrayList("b", "d"), true));
        excludes.add(getItr(Lists.newArrayList("f"), true));

        OrIterator iterator = new OrIterator(Collections.emptySet(), excludes);
        iterator.setContext("a");
        iterator.initialize();

        iterator.setContext("a");
        assertEquals("a", iterator.move("a"));

        // second iterator covers this one
        iterator.setContext("b");
        assertEquals("b", iterator.move("b"));

        // second iterator covers this one
        iterator.setContext("d");
        assertEquals("d", iterator.move("d"));

        iterator.setContext("e");
        assertEquals("e", iterator.move("e"));

        // first iterator covers this one
        iterator.setContext("f");
        assertEquals("f", iterator.move("f"));

        iterator.setContext("g");
        assertEquals("g", iterator.move("g"));
    }

    @Test
    public void testFirstDeferredInclude() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b"), true));
        includes.add(getItr(Lists.newArrayList("b", "d"), false));

        OrIterator iterator = new OrIterator(includes);
        iterator.setContext("a");
        iterator.initialize();

        iterator.setContext("a");
        assertEquals("a", iterator.move("a"));

        iterator.setContext("b");
        assertEquals("b", iterator.move("b"));

        iterator.setContext("c");
        assertEquals("d", iterator.move("c"));

        iterator.setContext("e");
        assertEquals(null, iterator.move("e"));
    }

    @Test
    public void testMoveNext() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b"), false));

        OrIterator iterator = new OrIterator(includes);
        iterator.initialize();

        assertEquals("a", iterator.move("a"));
        assertTrue(iterator.hasNext());
        assertEquals("b", iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testMoveNextPlusOne() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b"), false));

        OrIterator iterator = new OrIterator(includes);
        iterator.initialize();

        assertEquals("b", iterator.move("b"));
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testMoveLast() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b", "c"), false));

        OrIterator iterator = new OrIterator(includes);
        iterator.initialize();

        assertEquals("c", iterator.move("c"));
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testMoveBeyondLast() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b", "c"), false));

        OrIterator iterator = new OrIterator(includes);
        iterator.initialize();

        assertEquals(null, iterator.move("d"));
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testNestedOr() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b", "c"), false));
        Set<NestedIterator<String>> includes2 = new HashSet<>();
        includes2.add(getItr(Lists.newArrayList("a", "d", "e"), false));
        includes2.add(getItr(Lists.newArrayList("a", "d", "f"), false));
        includes.add(new OrIterator<>(includes2));

        OrIterator iterator = new OrIterator(includes);
        iterator.initialize();

        assertFalse(iterator.isContextRequired());
        assertTrue(iterator.hasNext());
        assertEquals("a", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("b", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("c", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("d", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("e", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("f", iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testNestedOrMove() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b", "c"), false));
        Set<NestedIterator<String>> includes2 = new HashSet<>();
        includes2.add(getItr(Lists.newArrayList("a", "d", "e"), false));
        includes2.add(getItr(Lists.newArrayList("a", "d", "f"), false));
        includes.add(new OrIterator<>(includes2));

        OrIterator iterator = new OrIterator(includes);
        iterator.initialize();

        assertFalse(iterator.isContextRequired());
        assertTrue(iterator.hasNext());
        assertEquals("b", iterator.move("a~"));
        assertTrue(iterator.hasNext());
        assertEquals("c", iterator.next());
        assertEquals("f", iterator.move("f"));
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testNestedOrMoveNegatedNoContext() {
        Set<NestedIterator<String>> childIncludes = new HashSet<>();
        Set<NestedIterator<String>> childExcludes = new HashSet<>();

        childIncludes.add(getItr(Lists.newArrayList("a", "d", "e"), false));
        childExcludes.add(getItr(Lists.newArrayList("f"), false));

        OrIterator childOr = new OrIterator(childIncludes, childExcludes);

        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(childOr);
        includes.add(getItr(Lists.newArrayList("a", "b", "c"), false));

        OrIterator iterator = new OrIterator(includes);

        assertTrue(iterator.isContextRequired());
        assertTrue(childOr.isContextRequired());

        assertThrows(IllegalStateException.class, iterator::initialize);
    }

    @Test
    public void testNestedOrMoveNegatedTest() {
        Set<NestedIterator<String>> childIncludes = new HashSet<>();
        Set<NestedIterator<String>> childExcludes = new HashSet<>();

        childExcludes.add(getItr(Lists.newArrayList("f"), false));

        OrIterator childOr = new OrIterator(childIncludes, childExcludes);

        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(childOr);
        includes.add(getItr(Lists.newArrayList("a", "b", "c"), false));

        OrIterator iterator = new OrIterator(includes);

        assertTrue(iterator.isContextRequired());
        assertTrue(childOr.isContextRequired());

        iterator.setContext("f");
        iterator.initialize();

        assertEquals(null, iterator.move("f"));
        assertFalse(iterator.hasNext());
        iterator.setContext("g");
        assertEquals("g", iterator.move("g"));
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testNestedOrMoveNegatedMultiTermTest() {
        Set<NestedIterator<String>> childIncludes = new HashSet<>();
        Set<NestedIterator<String>> childExcludes = new HashSet<>();

        childExcludes.add(getItr(Lists.newArrayList("f", "g"), false));

        OrIterator childOr = new OrIterator(childIncludes, childExcludes);

        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(childOr);
        includes.add(getItr(Lists.newArrayList("a", "b", "c"), false));

        OrIterator iterator = new OrIterator(includes);

        assertTrue(iterator.isContextRequired());
        assertTrue(childOr.isContextRequired());

        iterator.setContext("a");
        iterator.initialize();

        assertTrue(iterator.hasNext());
        iterator.setContext("b");
        assertEquals("a", iterator.next());
        assertTrue(iterator.hasNext());
        iterator.setContext("c");
        assertEquals("b", iterator.next());
        assertTrue(iterator.hasNext());
        iterator.setContext("d");
        assertEquals("c", iterator.next());
        assertTrue(iterator.hasNext());

        iterator.setContext("f");
        assertEquals(null, iterator.move("f"));
        assertFalse(iterator.hasNext());
        iterator.setContext("g");
        assertEquals(null, iterator.move("g"));
        assertFalse(iterator.hasNext());
        iterator.setContext("h");
        assertEquals("h", iterator.move("h"));
        assertFalse(iterator.hasNext());
    }

    private NegationFilterTest.Itr<String> getItr(List<String> source, boolean contextRequired) {
        return new NegationFilterTest.Itr<>(source, contextRequired);
    }
}
