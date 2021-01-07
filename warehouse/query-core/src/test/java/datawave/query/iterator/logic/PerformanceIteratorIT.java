package datawave.query.iterator.logic;

import datawave.query.attributes.Document;
import datawave.query.iterator.NestedIterator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class PerformanceIteratorIT {
    
    @Before
    public void setup() {
        
    }
    
    @Test
    public void baselineOrTest() {
        StatsWrappedIterator source1 = new StatsWrappedIterator(new ArrayIterator(new String[] {"a", "b", "c", "d"}));
        StatsWrappedIterator source2 = new StatsWrappedIterator(new ArrayIterator(new String[] {"x", "y", "z"}));
        
        List<NestedIterator> sources = new ArrayList<>();
        sources.add(source1);
        sources.add(source2);
        
        OrIterator orIterator = new OrIterator(sources);
        orIterator.initialize();
        
        // // a and b
        // Assert.assertEquals(2, source1.getNextCount());
        // // x
        // Assert.assertEquals(1, source2.getNextCount());
        // Assert.assertEquals(0, source1.getMoveCount());
        // Assert.assertEquals(0, source2.getMoveCount());
        
        Assert.assertEquals(true, orIterator.hasNext());
        Assert.assertEquals("a", orIterator.next());
        
        // c
        Assert.assertEquals(1, source1.getNextCount());
        // still x
        Assert.assertEquals(0, source2.getNextCount());
        Assert.assertEquals(0, source1.getMoveCount());
        Assert.assertEquals(0, source2.getMoveCount());
        
        Assert.assertEquals(true, orIterator.hasNext());
        Assert.assertEquals("b", orIterator.next());
        
        // d
        Assert.assertEquals(2, source1.getNextCount());
        // still x
        Assert.assertEquals(0, source2.getNextCount());
        Assert.assertEquals(0, source1.getMoveCount());
        Assert.assertEquals(0, source2.getMoveCount());
        
        Assert.assertEquals(true, orIterator.hasNext());
        Assert.assertEquals("c", orIterator.next());
        
        // null
        Assert.assertEquals(3, source1.getNextCount());
        // still x
        Assert.assertEquals(0, source2.getNextCount());
        Assert.assertEquals(0, source1.getMoveCount());
        Assert.assertEquals(0, source2.getMoveCount());
        
        Assert.assertEquals(true, orIterator.hasNext());
        Assert.assertEquals("d", orIterator.next());
        
        // null
        Assert.assertEquals(4, source1.getNextCount());
        // y
        Assert.assertEquals(0, source2.getNextCount());
        Assert.assertEquals(0, source1.getMoveCount());
        Assert.assertEquals(0, source2.getMoveCount());
        
        Assert.assertEquals(true, orIterator.hasNext());
        Assert.assertEquals("x", orIterator.next());
        
        // null
        Assert.assertEquals(4, source1.getNextCount());
        // z
        Assert.assertEquals(1, source2.getNextCount());
        Assert.assertEquals(0, source1.getMoveCount());
        Assert.assertEquals(0, source2.getMoveCount());
        
        Assert.assertEquals(true, orIterator.hasNext());
        Assert.assertEquals("y", orIterator.next());
        
        // null
        Assert.assertEquals(4, source1.getNextCount());
        // null
        Assert.assertEquals(2, source2.getNextCount());
        Assert.assertEquals(0, source1.getMoveCount());
        Assert.assertEquals(0, source2.getMoveCount());
        
        Assert.assertEquals(true, orIterator.hasNext());
        Assert.assertEquals("z", orIterator.next());
        
        Assert.assertEquals(false, orIterator.hasNext());
        
        // null
        Assert.assertEquals(4, source1.getNextCount());
        // null
        Assert.assertEquals(3, source2.getNextCount());
        Assert.assertEquals(0, source1.getMoveCount());
        Assert.assertEquals(0, source2.getMoveCount());
    }
    
    @Test
    public void orNestedAndTest() {
        StatsWrappedIterator source1 = new StatsWrappedIterator(new ArrayIterator(new String[] {"a", "b", "c", "d"}));
        StatsWrappedIterator source2 = new StatsWrappedIterator(new ArrayIterator(new String[] {"x", "y", "z"}));
        
        // create an non-convergent AND within the OR, with nothing to anchor on to bypass this it has to be fully explored
        StatsWrappedIterator source3 = new StatsWrappedIterator(new ArrayIterator(new String[] {"q", "s", "u"}));
        StatsWrappedIterator source4 = new StatsWrappedIterator(new ArrayIterator(new String[] {"r", "t", "v"}));
        
        List<NestedIterator> sources = new ArrayList<>();
        sources.add(source3);
        sources.add(source4);
        
        AndIterator and = new AndIterator(sources);
        
        sources = new ArrayList<>();
        sources.add(source1);
        sources.add(source2);
        sources.add(and);
        
        OrIterator orIterator = new OrIterator(sources);
        orIterator.initialize();
        
        Assert.assertEquals(true, orIterator.hasNext());
        Assert.assertEquals("a", orIterator.next());
        Assert.assertEquals(true, orIterator.hasNext());
        Assert.assertEquals("b", orIterator.next());
        Assert.assertEquals(true, orIterator.hasNext());
        Assert.assertEquals("c", orIterator.next());
        Assert.assertEquals(true, orIterator.hasNext());
        Assert.assertEquals("d", orIterator.next());
        Assert.assertEquals(true, orIterator.hasNext());
        Assert.assertEquals("x", orIterator.next());
        Assert.assertEquals(true, orIterator.hasNext());
        Assert.assertEquals("y", orIterator.next());
        Assert.assertEquals(true, orIterator.hasNext());
        Assert.assertEquals("z", orIterator.next());
        Assert.assertEquals(false, orIterator.hasNext());
        
        Assert.assertEquals(4, source1.getNextCount());
        Assert.assertEquals(0, source1.getMoveCount());
        Assert.assertEquals(3, source2.getNextCount());
        Assert.assertEquals(0, source2.getMoveCount());
        Assert.assertEquals(0, source3.getNextCount());
        Assert.assertEquals(3, source3.getMoveCount());
        Assert.assertEquals(1, source4.getNextCount());
        Assert.assertEquals(2, source4.getMoveCount());
    }
    
    @Test
    public void andOrNestedAndTest() {
        StatsWrappedIterator source1 = new StatsWrappedIterator(new ArrayIterator(new String[] {"a", "b", "c", "d"}));
        StatsWrappedIterator source2 = new StatsWrappedIterator(new ArrayIterator(new String[] {"x", "y", "z"}));
        
        // create an non-convergent AND within the OR, with nothing to anchor on to bypass this it has to be fully explored
        StatsWrappedIterator source3 = new StatsWrappedIterator(new ArrayIterator(new String[] {"q", "s", "u"}));
        StatsWrappedIterator source4 = new StatsWrappedIterator(new ArrayIterator(new String[] {"r", "t", "v"}));
        
        List<NestedIterator> sources = new ArrayList<>();
        sources.add(source3);
        sources.add(source4);
        
        AndIterator nestedAnd = new AndIterator(sources);
        
        sources = new ArrayList<>();
        sources.add(source1);
        sources.add(source2);
        sources.add(nestedAnd);
        
        OrIterator orIterator = new OrIterator(sources);
        
        StatsWrappedIterator source5 = new StatsWrappedIterator(new ArrayIterator(new String[] {"x", "z"}));
        sources = new ArrayList<>();
        sources.add(source5);
        sources.add(orIterator);
        
        AndIterator rootAnd = new AndIterator(sources);
        rootAnd.initialize();
        
        Assert.assertEquals(true, rootAnd.hasNext());
        Assert.assertEquals("x", rootAnd.next());
        Assert.assertEquals(true, rootAnd.hasNext());
        Assert.assertEquals("z", rootAnd.next());
        Assert.assertEquals(false, rootAnd.hasNext());
        
        Assert.assertEquals(0, source1.getNextCount());
        Assert.assertEquals(1, source1.getMoveCount());
        Assert.assertEquals(1, source2.getNextCount());
        Assert.assertEquals(1, source2.getMoveCount());
        Assert.assertEquals(0, source3.getNextCount());
        Assert.assertEquals(1, source3.getMoveCount());
        Assert.assertEquals(0, source4.getNextCount());
        Assert.assertEquals(0, source4.getMoveCount());
    }
    
    @Test
    public void baselineAndTest() {
        StatsWrappedIterator source1 = new StatsWrappedIterator(new ArrayIterator(new String[] {"a", "b", "c", "d"}));
        StatsWrappedIterator source2 = new StatsWrappedIterator(new ArrayIterator(new String[] {"x", "y", "z"}));
        
        List<NestedIterator> sources = new ArrayList<>();
        sources.add(source1);
        sources.add(source2);
        
        AndIterator andIterator = new AndIterator(sources);
        andIterator.initialize();
        
        // a
        Assert.assertEquals(0, source1.getNextCount());
        // move to x
        Assert.assertEquals(0, source1.getMoveCount());
        // x
        Assert.assertEquals(0, source2.getNextCount());
        
        // no possible results
        Assert.assertEquals(false, andIterator.hasNext());
        
        // a
        Assert.assertEquals(0, source1.getNextCount());
        // move to x
        Assert.assertEquals(1, source1.getMoveCount());
        // x
        Assert.assertEquals(0, source2.getNextCount());
    }
    
    @Test
    public void baselineAndWorstCaseTest() {
        StatsWrappedIterator source1 = new StatsWrappedIterator(new ArrayIterator(new String[] {"a", "c", "e"}));
        StatsWrappedIterator source2 = new StatsWrappedIterator(new ArrayIterator(new String[] {"b", "d", "f"}));
        
        List<NestedIterator> sources = new ArrayList<>();
        sources.add(source1);
        sources.add(source2);
        
        AndIterator andIterator = new AndIterator(sources);
        andIterator.initialize();
        
        // a
        Assert.assertEquals(0, source1.getNextCount());
        // move to b, d, e
        Assert.assertEquals(0, source1.getMoveCount());
        // x
        Assert.assertEquals(0, source2.getNextCount());
        // move to c, e
        Assert.assertEquals(0, source2.getMoveCount());
        // no possible results
        Assert.assertEquals(false, andIterator.hasNext());
        
        // a
        Assert.assertEquals(0, source1.getNextCount());
        // move to b, d, e
        Assert.assertEquals(3, source1.getMoveCount());
        // x
        Assert.assertEquals(1, source2.getNextCount());
        // move to c, e
        Assert.assertEquals(2, source2.getMoveCount());
    }
    
    /**
     * Bypass the inefficiencies in the baselineAndWorstCase by using rootSource to move source1 and exhaust it
     */
    @Test
    public void nestedWorstCaseTest() {
        StatsWrappedIterator source1 = new StatsWrappedIterator(new ArrayIterator(new String[] {"a", "c", "e"}));
        StatsWrappedIterator source2 = new StatsWrappedIterator(new ArrayIterator(new String[] {"b", "d", "f"}));
        
        List<NestedIterator> sources = new ArrayList<>();
        sources.add(source1);
        sources.add(source2);
        
        AndIterator andIterator = new AndIterator(sources);
        
        StatsWrappedIterator rootSource = new StatsWrappedIterator(new ArrayIterator(new String[] {"x"}));
        List<NestedIterator> rootSources = new ArrayList<>();
        
        rootSources.add(andIterator);
        rootSources.add(rootSource);
        
        AndIterator rootAnd = new AndIterator(rootSources);
        rootAnd.initialize();
        
        Assert.assertEquals(0, source1.getNextCount());
        Assert.assertEquals(0, source1.getMoveCount());
        Assert.assertEquals(0, source2.getNextCount());
        Assert.assertEquals(0, source2.getMoveCount());
        
        // no possible results
        Assert.assertEquals(false, rootAnd.hasNext());
        
        Assert.assertEquals(0, source1.getNextCount());
        Assert.assertEquals(0, source2.getNextCount());
        Assert.assertEquals(0, rootSource.getNextCount());
        Assert.assertEquals(1, source1.getMoveCount());
        Assert.assertEquals(0, source2.getMoveCount());
    }
    
    /**
     * Ensure that regardless of the nesting of the inefficient iterator it is minimally exercised
     */
    @Test
    public void doubleNestingTest() {
        StatsWrappedIterator source1 = new StatsWrappedIterator(new ArrayIterator(new String[] {"a", "c", "e", "y"}));
        StatsWrappedIterator source2 = new StatsWrappedIterator(new ArrayIterator(new String[] {"b", "d", "f", "y"}));
        
        StatsWrappedIterator source3 = new StatsWrappedIterator(new ArrayIterator(new String[] {"y1", "y3", "z"}));
        StatsWrappedIterator source4 = new StatsWrappedIterator(new ArrayIterator(new String[] {"y2", "y4", "z"}));
        
        List<NestedIterator> sources = new ArrayList<>();
        sources.add(source3);
        sources.add(source4);
        
        AndIterator and2 = new AndIterator(sources);
        
        sources = new ArrayList<>();
        sources.add(source1);
        sources.add(source2);
        sources.add(and2);
        
        AndIterator and1 = new AndIterator(sources);
        
        StatsWrappedIterator rootSource = new StatsWrappedIterator(new ArrayIterator(new String[] {"x", "z1"}));
        List<NestedIterator> rootSources = new ArrayList<>();
        
        rootSources.add(and1);
        rootSources.add(rootSource);
        
        AndIterator rootAnd = new AndIterator(rootSources);
        rootAnd.initialize();
        
        Assert.assertEquals(0, source1.getNextCount());
        Assert.assertEquals(0, source1.getMoveCount());
        Assert.assertEquals(0, source2.getNextCount());
        Assert.assertEquals(0, source2.getMoveCount());
        
        // no possible results
        Assert.assertEquals(false, rootAnd.hasNext());
        
        Assert.assertEquals(0, source1.getNextCount());
        Assert.assertEquals(0, source2.getNextCount());
        Assert.assertEquals(0, rootSource.getNextCount());
        Assert.assertEquals(1, rootSource.getMoveCount());
        Assert.assertEquals(1, source1.getMoveCount());
        Assert.assertEquals(0, source2.getMoveCount());
        Assert.assertEquals(0, source3.getNextCount());
        Assert.assertEquals(0, source4.getNextCount());
        Assert.assertEquals(0, source3.getMoveCount());
        Assert.assertEquals(0, source4.getMoveCount());
    }
    
    /**
     * Like <code>nestedWorstCase()</code> but rather than hit the worst case on the first document, hit the worst case later in evaluation
     */
    @Test
    public void nestedWorstCaseRepeatTest() {
        StatsWrappedIterator source1 = new StatsWrappedIterator(new ArrayIterator(new String[] {"a", "c", "e"}));
        StatsWrappedIterator source2 = new StatsWrappedIterator(new ArrayIterator(new String[] {"a", "b", "d", "f"}));
        
        List<NestedIterator> sources = new ArrayList<>();
        sources.add(source1);
        sources.add(source2);
        
        AndIterator andIterator = new AndIterator(sources);
        
        StatsWrappedIterator rootSource = new StatsWrappedIterator(new ArrayIterator(new String[] {"a", "x"}));
        List<NestedIterator> rootSources = new ArrayList<>();
        
        rootSources.add(andIterator);
        rootSources.add(rootSource);
        
        AndIterator rootAnd = new AndIterator(rootSources);
        rootAnd.initialize();
        
        Assert.assertEquals(0, source1.getNextCount());
        Assert.assertEquals(0, source1.getMoveCount());
        Assert.assertEquals(0, source2.getNextCount());
        Assert.assertEquals(0, source2.getMoveCount());
        
        Assert.assertEquals(true, rootAnd.hasNext());
        Assert.assertEquals("a", rootAnd.next());
        
        Assert.assertEquals(1, source1.getNextCount());
        Assert.assertEquals(1, source2.getNextCount());
        Assert.assertEquals(1, rootSource.getNextCount());
        Assert.assertEquals(0, source1.getMoveCount());
        Assert.assertEquals(0, source2.getMoveCount());
        
        // no possible results
        Assert.assertEquals(false, rootAnd.hasNext());
        
        Assert.assertEquals(1, source1.getNextCount());
        Assert.assertEquals(1, source2.getNextCount());
        Assert.assertEquals(1, rootSource.getNextCount());
        Assert.assertEquals(0, source1.getMoveCount());
        Assert.assertEquals(1, source2.getMoveCount());
    }
    
    public static class StatsWrappedIterator<T extends Comparable<T>> implements NestedIterator<T> {
        private NestedIterator<T> delegate;
        
        private long nextCount = 0;
        private long moveCount = 0;
        
        public StatsWrappedIterator(NestedIterator<T> delegate) {
            this.delegate = delegate;
        }
        
        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }
        
        @Override
        public T next() {
            nextCount++;
            return delegate.next();
        }
        
        @Override
        public void initialize() {
            delegate.initialize();
        }
        
        @Override
        public T move(T minimum) {
            moveCount++;
            return delegate.move(minimum);
        }
        
        @Override
        public Collection<NestedIterator<T>> leaves() {
            return delegate.leaves();
        }
        
        @Override
        public Collection<NestedIterator<T>> children() {
            return delegate.children();
        }
        
        @Override
        public Document document() {
            return delegate.document();
        }
        
        @Override
        public boolean isContextRequired() {
            return delegate.isContextRequired();
        }
        
        @Override
        public void setContext(T context) {
            delegate.setContext(context);
        }
        
        @Override
        public T peek() {
            return delegate.peek();
        }
        
        public long getNextCount() {
            return nextCount;
        }
        
        public long getMoveCount() {
            return moveCount;
        }
    }
}
