package datawave.query.iterator.logic;

import com.google.common.collect.Lists;
import com.google.common.collect.TreeMultimap;
import datawave.query.iterator.Util;
import datawave.query.attributes.Document;
import datawave.query.iterator.NestedIterator;
import org.junit.Test;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;

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
    }
    
    // A wrapper around a java.util.Iterator
    static class Itr<K extends Comparable<K>> implements NestedIterator<K> {
        private Iterator<K> i;
        
        public Itr(Iterable<K> it) {
            i = it.iterator();
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
        public Collection<NestedIterator<K>> leaves() {
            return null;
        }
        
        @Override
        public Collection<NestedIterator<K>> children() {
            return null;
        }
        
        @Override
        public Document document() {
            return null;
        }
    }
}
