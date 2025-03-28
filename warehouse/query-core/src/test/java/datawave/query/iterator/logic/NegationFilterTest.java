package datawave.query.iterator.logic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.TreeMultimap;

import datawave.query.attributes.Document;
import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.Util;

public class NegationFilterTest {
    @Test
    public void test() throws Throwable {
        List<String> filterSet1 = Lists.newArrayList("a", "b", "c", "q", "r", "s");
        Itr<String> f1 = new Itr<>(filterSet1);
        List<String> filterSet2 = Lists.newArrayList("a", "b", "c", "d", "e", "f");
        Itr<String> f2 = new Itr<>(filterSet2);
        TreeMultimap<String,NestedIterator<String>> mmap = TreeMultimap.create(Util.keyComparator(), Util.hashComparator());
        mmap.put(f1.next(), f1);
        mmap.put(f2.next(), f2);

        assertTrue(NegationFilter.isFiltered("e", mmap, Util.keyTransformer()));
        assertEquals(2, mmap.keySet().size());
        assertNotEquals(null, mmap.get("e"));
        assertNotEquals(null, mmap.get("q"));
        assertEquals(2, mmap.values().size());
    }

    @Test
    public void testDuplicateTerm() throws Throwable {
        List<String> filterSet1 = Lists.newArrayList("a", "b", "c", "q", "r", "s");
        Itr<String> f1 = new Itr<>(filterSet1);
        List<String> filterSet2 = Lists.newArrayList("a", "b", "c", "d", "e", "f");
        Itr<String> f2 = new Itr<>(filterSet2);
        TreeMultimap<String,NestedIterator<String>> mmap = TreeMultimap.create(Util.keyComparator(), Util.hashComparator());
        mmap.put(f1.next(), f1);
        mmap.put(f2.next(), f2);

        assertTrue(NegationFilter.isFiltered("c", mmap, Util.keyTransformer()));
        assertEquals(1, mmap.keySet().size());
        assertEquals("c", mmap.keySet().iterator().next());
        assertEquals(2, mmap.values().size());
    }

    @Test
    public void testExhausted() throws Throwable {
        List<String> filterSet1 = Lists.newArrayList("a", "b", "c");
        Itr<String> f1 = new Itr<>(filterSet1);
        List<String> filterSet2 = Lists.newArrayList("a", "b", "c", "d", "e", "f");
        Itr<String> f2 = new Itr<>(filterSet2);
        TreeMultimap<String,NestedIterator<String>> mmap = TreeMultimap.create(Util.keyComparator(), Util.hashComparator());
        mmap.put(f1.next(), f1);
        mmap.put(f2.next(), f2);

        assertTrue(NegationFilter.isFiltered("c", mmap, Util.keyTransformer()));
        assertEquals(1, mmap.keySet().size());
        assertEquals("c", mmap.keySet().iterator().next());
        assertEquals(2, mmap.values().size());
    }

    @Test
    public void testEmpty() throws Throwable {
        List<String> filterSet1 = Lists.newArrayList("a", "b");
        Itr<String> f1 = new Itr<>(filterSet1);
        List<String> filterSet2 = Lists.newArrayList("a", "b", "c", "d", "e", "f");
        Itr<String> f2 = new Itr<>(filterSet2);
        TreeMultimap<String,NestedIterator<String>> mmap = TreeMultimap.create(Util.keyComparator(), Util.hashComparator());
        mmap.put(f1.next(), f1);
        mmap.put(f2.next(), f2);

        assertTrue(NegationFilter.isFiltered("c", mmap, Util.keyTransformer()));
        assertEquals(1, mmap.keySet().size());
        assertEquals("c", mmap.keySet().iterator().next());
        assertEquals(1, mmap.values().size());
    }

    @Test
    public void testContains() throws Throwable {
        List<String> filterSet1 = Lists.newArrayList("a", "b");
        Itr<String> f1 = new Itr<>(filterSet1);
        List<String> filterSet2 = Lists.newArrayList("a", "b", "c", "d", "e", "f");
        Itr<String> f2 = new Itr<>(filterSet2);
        TreeMultimap<String,NestedIterator<String>> mmap = TreeMultimap.create(Util.keyComparator(), Util.hashComparator());
        mmap.put(f1.next(), f1);
        mmap.put("c", f2);

        assertTrue(NegationFilter.isFiltered("c", mmap, Util.keyTransformer()));
        // even though filterSet1 is outside the bounds, the contains check doesn't move anything, this is expected and good
        assertEquals(2, mmap.keySet().size());
        Iterator<String> i = mmap.keySet().iterator();
        assertEquals("a", i.next());
        assertEquals("c", i.next());
        assertFalse(i.hasNext());
        assertEquals(2, mmap.values().size());
    }

    // A wrapper around a java.util.Iterator
    static class Itr<K extends Comparable<K>> implements NestedIterator<K> {
        private Iterator<K> i;
        private boolean contextRequired;

        public Itr(Iterable<K> it, boolean contextRequired) {
            i = it.iterator();
            this.contextRequired = contextRequired;
        }

        public Itr(Iterable<K> it) {
            this(it, false);
        }

        @Override
        public boolean hasNext() {
            return i.hasNext();
        }

        @Override
        public K next() {
            return i.next();
        }

        @Override
        public void remove() {
            i.remove();
        }

        @Override
        public void initialize() {}

        @Override
        public K move(K minimum) {
            if (hasNext()) {
                K next = next();
                while (next.compareTo(minimum) < 0) {
                    if (hasNext())
                        next = next();
                    else
                        return null;
                }
                return next;
            } else {
                return null;
            }
        }

        @Override
        public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
            throw new UnsupportedEncodingException("Not implemented");
        }

        @Override
        public Collection<NestedIterator<K>> leaves() {
            return null;
        }

        @Override
        public Collection<NestedIterator<K>> children() {
            return null;
        }

        @Override
        public Document document() {
            return new Document();
        }

        @Override
        public boolean isContextRequired() {
            return contextRequired;
        }

        @Override
        public void setContext(K context) {
            // no-op
        }

        // tests involving this iterator are assumed to be for indexed event fields
        @Override
        public boolean isNonEventField() {
            return false;
        }
    }

    static class InterruptedIterator<K> implements Iterator<K> {

        private int count = 0;
        private final Iterator<K> i;

        public InterruptedIterator(Iterator<K> i) {
            this.i = i;
        }

        @Override
        public boolean hasNext() {
            return i.hasNext();
        }

        @Override
        public K next() {
            if (count == 0) {
                count++;
                return i.next();
            }
            throw new NoSuchElementException("Interrupted while calling next");
        }
    }

    static class InterruptedIterable<K> implements Iterable<K> {

        private final Iterator<K> i;

        public InterruptedIterable(Iterator<K> i) {
            this.i = i;
        }

        @Override
        public Iterator<K> iterator() {
            return new InterruptedIterator<>(i);
        }
    }
}
