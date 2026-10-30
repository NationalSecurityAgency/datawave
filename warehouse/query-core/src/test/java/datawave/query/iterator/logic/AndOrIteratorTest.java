package datawave.query.iterator.logic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;

import datawave.query.iterator.NestedIterator;

public class AndOrIteratorTest {
    // X AND (!Y OR !Z)
    @Test
    public void testAndNegatedOr() {
        Set<NestedIterator<String>> childIncludes = new HashSet<>();
        Set<NestedIterator<String>> childExcludes = new HashSet<>();

        childExcludes.add(getItr(Lists.newArrayList("b", "c"), false));
        childExcludes.add(getItr(Lists.newArrayList("a", "b"), false));

        OrIterator childOr = new OrIterator(childIncludes, childExcludes);

        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(childOr);
        includes.add(getItr(Lists.newArrayList("a", "b", "c"), false));

        NestedIterator iterator = new AndIterator(includes);
        iterator.initialize();

        assertFalse(iterator.isContextRequired());
        assertTrue(childOr.isContextRequired());

        assertTrue(iterator.hasNext());
        assertEquals("a", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("c", iterator.next());
        assertFalse(iterator.hasNext());
    }

    // X AND (Y OR !Z)
    @Test
    public void testAndMixedOr() {
        Set<NestedIterator<String>> childIncludes = new HashSet<>();
        Set<NestedIterator<String>> childExcludes = new HashSet<>();

        childIncludes.add(getItr(Lists.newArrayList("b", "c"), false));
        childExcludes.add(getItr(Lists.newArrayList("a", "b"), false));

        OrIterator childOr = new OrIterator(childIncludes, childExcludes);

        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(childOr);
        includes.add(getItr(Lists.newArrayList("a", "b", "c"), false));

        NestedIterator iterator = new AndIterator(includes);
        iterator.initialize();

        assertFalse(iterator.isContextRequired());
        assertTrue(childOr.isContextRequired());

        assertTrue(iterator.hasNext());
        assertEquals("b", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("c", iterator.next());
        assertFalse(iterator.hasNext());
    }

    // X AND (!Y AND !Z)
    @Test
    public void testAndNegatedAnd() {
        Set<NestedIterator<String>> childIncludes = new HashSet<>();
        Set<NestedIterator<String>> childExcludes = new HashSet<>();

        childExcludes.add(getItr(Lists.newArrayList("b", "c"), false));
        childExcludes.add(getItr(Lists.newArrayList("a", "b"), false));

        NestedIterator child = new AndIterator(childIncludes, childExcludes);

        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(child);
        includes.add(getItr(Lists.newArrayList("a", "b", "c", "d"), false));

        NestedIterator iterator = new AndIterator(includes);
        iterator.initialize();

        assertFalse(iterator.isContextRequired());
        assertTrue(child.isContextRequired());

        assertTrue(iterator.hasNext());
        assertEquals("d", iterator.next());
        assertFalse(iterator.hasNext());
    }

    // X OR !Y
    @Test
    public void testDeferredOrMissingContext() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        Set<NestedIterator<String>> excludes = new HashSet<>();

        includes.add(getItr(Lists.newArrayList("a", "b", "c", "d"), false));
        excludes.add(getItr(Lists.newArrayList("a"), false));

        NestedIterator iterator = new OrIterator(includes, excludes);
        assertThrows(IllegalStateException.class, iterator::initialize);
    }

    // !X OR !Y
    @Test
    public void testDeferredOr() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        Set<NestedIterator<String>> excludes = new HashSet<>();

        excludes.add(getItr(Lists.newArrayList("a", "b", "c", "d"), false));
        excludes.add(getItr(Lists.newArrayList("a"), false));

        NestedIterator iterator = new OrIterator(includes, excludes);
        assertThrows(IllegalStateException.class, iterator::initialize);
    }

    // !X AND !Y
    @Test
    public void testDeferredAnd() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        Set<NestedIterator<String>> excludes = new HashSet<>();

        excludes.add(getItr(Lists.newArrayList("a", "b", "c", "d"), false));
        excludes.add(getItr(Lists.newArrayList("a"), false));

        NestedIterator iterator = new AndIterator(includes, excludes);
        assertThrows(IllegalStateException.class, iterator::initialize);
    }

    // X AND !Y AND (!Z OR !A)
    @Test
    public void testNegatedNonDeferredInteractionWithDeferred() {
        Set<NestedIterator<String>> childIncludes = new HashSet<>();
        Set<NestedIterator<String>> childExcludes = new HashSet<>();

        childExcludes.add(getItr(Lists.newArrayList("b", "d"), false));
        childExcludes.add(getItr(Lists.newArrayList("c"), false));

        NestedIterator<String> child = new OrIterator<>(childIncludes, childExcludes);

        Set<NestedIterator<String>> includes = new HashSet<>();
        Set<NestedIterator<String>> excludes = new HashSet<>();

        includes.add(getItr(Lists.newArrayList("a", "b", "c", "d"), false));
        excludes.add(getItr(Lists.newArrayList("a"), false));
        includes.add(child);

        NestedIterator iterator = new AndIterator(includes, excludes);
        iterator.initialize();

        assertTrue(child.isContextRequired());
        assertFalse(iterator.isContextRequired());

        assertTrue(iterator.hasNext());
        assertEquals("b", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("c", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("d", iterator.next());
        assertFalse(iterator.hasNext());
    }

    // X AND (Y OR !Y)
    @Test
    public void testAndAlwaysTrue() {
        Set<NestedIterator<String>> childIncludes = new HashSet<>();
        Set<NestedIterator<String>> childExcludes = new HashSet<>();

        childIncludes.add(getItr(Lists.newArrayList("b", "c"), false));
        childExcludes.add(getItr(Lists.newArrayList("b", "c"), false));

        NestedIterator child = new OrIterator(childIncludes, childExcludes);

        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(child);
        includes.add(getItr(Lists.newArrayList("a", "b", "c", "d"), false));

        NestedIterator iterator = new AndIterator(includes);
        iterator.initialize();

        assertFalse(iterator.isContextRequired());
        assertTrue(child.isContextRequired());

        assertTrue(iterator.hasNext());
        assertEquals("a", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("b", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("c", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("d", iterator.next());
        assertFalse(iterator.hasNext());
    }

    // X AND (Y AND !Y)
    @Test
    public void testAndNeverTrue() {
        Set<NestedIterator<String>> childIncludes = new HashSet<>();
        Set<NestedIterator<String>> childExcludes = new HashSet<>();

        childIncludes.add(getItr(Lists.newArrayList("b", "c"), false));
        childExcludes.add(getItr(Lists.newArrayList("b", "c"), false));

        NestedIterator child = new AndIterator(childIncludes, childExcludes);

        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(child);
        includes.add(getItr(Lists.newArrayList("a", "b", "c", "d"), false));

        NestedIterator iterator = new AndIterator(includes);
        iterator.initialize();

        assertFalse(iterator.isContextRequired());
        assertFalse(child.isContextRequired());

        assertFalse(iterator.hasNext());
    }

    // X OR (Y AND Z)
    @Test
    public void testOrAnd() {
        Set<NestedIterator<String>> childIncludes = new HashSet<>();
        Set<NestedIterator<String>> childExcludes = new HashSet<>();

        childIncludes.add(getItr(Lists.newArrayList("a", "c", "f"), false));
        childIncludes.add(getItr(Lists.newArrayList("b", "c", "f"), false));

        NestedIterator child = new AndIterator(childIncludes, childExcludes);

        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(child);
        includes.add(getItr(Lists.newArrayList("a", "b", "c", "d"), false));

        NestedIterator iterator = new OrIterator(includes);
        iterator.initialize();

        assertFalse(iterator.isContextRequired());
        assertFalse(child.isContextRequired());

        assertTrue(iterator.hasNext());
        assertEquals("a", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("b", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("c", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("d", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("f", iterator.next());
        assertFalse(iterator.hasNext());
    }

    // X OR (Y AND !Z)
    @Test
    public void testOrMixedAnd() {
        Set<NestedIterator<String>> childIncludes = new HashSet<>();
        Set<NestedIterator<String>> childExcludes = new HashSet<>();

        childIncludes.add(getItr(Lists.newArrayList("a", "b"), false));
        childExcludes.add(getItr(Lists.newArrayList("b", "f"), false));

        NestedIterator child = new AndIterator(childIncludes, childExcludes);

        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(child);
        includes.add(getItr(Lists.newArrayList("c", "d"), false));

        NestedIterator iterator = new OrIterator(includes);
        iterator.initialize();

        assertFalse(iterator.isContextRequired());
        assertFalse(child.isContextRequired());

        assertTrue(iterator.hasNext());
        assertEquals("a", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("c", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("d", iterator.next());
        assertFalse(iterator.hasNext());
    }

    // X AND ((!Y OR Z) OR W)
    @Test
    public void testDeferredOrWithAccept() {
        Set<NestedIterator<String>> childIncludes = new HashSet<>();
        Set<NestedIterator<String>> childExcludes = new HashSet<>();

        childExcludes.add(getItr(Lists.newArrayList("a", "b", "c", "d"), false));
        childIncludes.add(getItr(Lists.newArrayList("b", "c"), false));

        NestedIterator child1 = new OrIterator(childIncludes, childExcludes);

        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(child1);
        includes.add(getItr(Lists.newArrayList("c", "d"), false));

        NestedIterator child2 = new OrIterator(includes);

        includes = new HashSet<>();
        includes.add(child2);
        includes.add(getItr(Lists.newArrayList("a", "b", "c", "d"), false));

        NestedIterator iterator = new AndIterator(includes);
        iterator.initialize();

        assertFalse(iterator.isContextRequired());
        assertTrue(child2.isContextRequired());
        assertTrue(child1.isContextRequired());

        assertTrue(iterator.hasNext());
        assertEquals("b", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("c", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("d", iterator.next());
        assertFalse(iterator.hasNext());
    }

    // X AND (Y OR !Z)
    @Test
    public void testDeferredOrAdvanceInMove() {
        Set<NestedIterator<String>> childIncludes = new HashSet<>();
        Set<NestedIterator<String>> childExcludes = new HashSet<>();

        childIncludes.add(getItr(Lists.newArrayList("b", "b1", "b2", "b3", "c"), false));
        childExcludes.add(getItr(Lists.newArrayList("a", "b"), false));

        OrIterator childOr = new OrIterator(childIncludes, childExcludes);

        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(childOr);
        includes.add(getItr(Lists.newArrayList("a", "c"), false));

        NestedIterator iterator = new AndIterator(includes);
        iterator.initialize();

        assertFalse(iterator.isContextRequired());
        assertTrue(childOr.isContextRequired());

        assertTrue(iterator.hasNext());
        assertEquals("c", iterator.next());
        assertFalse(iterator.hasNext());
    }

    // X AND (Y OR !Z)
    @Test
    public void testDeferredNoMatches() {
        Set<NestedIterator<String>> childIncludes = new HashSet<>();
        Set<NestedIterator<String>> childExcludes = new HashSet<>();

        childIncludes.add(getItr(Lists.newArrayList("b", "b1", "b2", "b3", "c"), false));
        childExcludes.add(getItr(Lists.newArrayList("z", "z1"), false));

        OrIterator childOr = new OrIterator(childIncludes, childExcludes);

        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(childOr);
        includes.add(getItr(Lists.newArrayList("z"), false));

        NestedIterator iterator = new AndIterator(includes);
        iterator.initialize();

        assertFalse(iterator.isContextRequired());
        assertTrue(childOr.isContextRequired());

        assertFalse(iterator.hasNext());
    }

    // X AND !(Y OR !Z)
    @Test
    public void testNegatedDeferred() {
        Set<NestedIterator<String>> childIncludes = new HashSet<>();
        Set<NestedIterator<String>> childExcludes = new HashSet<>();

        childIncludes.add(getItr(Lists.newArrayList("b", "b1", "b2", "b3", "c"), false));
        childExcludes.add(getItr(Lists.newArrayList("z1", "z2"), false));

        OrIterator childOr = new OrIterator(childIncludes, childExcludes);

        Set<NestedIterator<String>> includes = new HashSet<>();
        Set<NestedIterator<String>> excludes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("z"), false));
        excludes.add(childOr);

        NestedIterator iterator = new AndIterator(includes, excludes);
        iterator.initialize();

        assertFalse(iterator.isContextRequired());
        assertTrue(childOr.isContextRequired());

        assertFalse(iterator.hasNext());
    }

    private NegationFilterTest.Itr<String> getItr(List<String> source, boolean contextRequired) {
        return new NegationFilterTest.Itr<>(source, contextRequired);
    }
}
