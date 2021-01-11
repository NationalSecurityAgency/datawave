package datawave.query.iterator.logic;

import datawave.query.attributes.Document;
import datawave.query.iterator.NestedIterator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class PerformanceIteratorIT {
    
    @Before
    public void setup() {
        
    }
    
    // (X OR Y)
    @Test
    public void baselineOrTest() {
        StatsWrappedIterator source1 = new StatsWrappedIterator(new ArrayIterator(new String[] {"a", "b", "c", "d"}));
        StatsWrappedIterator source2 = new StatsWrappedIterator(new ArrayIterator(new String[] {"x", "y", "z"}));
        
        List<NestedIterator> sources = new ArrayList<>();
        sources.add(source1);
        sources.add(source2);
        
        OrIterator orIterator = new OrIterator(sources);
        orIterator.initialize();
        
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
    
    // (A OR B OR (C AND D))
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
    
    // (E AND (A OR B OR (C AND D)))
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
    
    // (A AND B)
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
    
    // (A AND B)
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
    // ((A AND B) AND C)
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
    // ((A AND B AND (C AND D)) AND E)
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
    // ((A AND B) AND C)
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
    
    // X AND Y AND (A OR B OR C OR (E AND F AND G))
    @Test
    public void highDensityTest() {
        List<String> x = new ArrayList<>();
        List<String> y = new ArrayList<>();
        
        List<String> a = new ArrayList<>();
        List<String> b = new ArrayList<>();
        List<String> c = new ArrayList<>();
        
        double xCardinality = .75;
        double yCardinality = .99;
        
        double aCardinality = .2;
        double bCardinality = .5;
        double cCardinality = .1;
        
        Random r = new Random();
        
        List<String> expected = new ArrayList<>();
        
        for (int i = 0; i < 1000000; i++) {
            String parsed = Integer.toString(i);
            
            boolean addX = accept(r, xCardinality);
            boolean addY = accept(r, yCardinality);
            
            boolean addA = accept(r, aCardinality);
            boolean addB = accept(r, bCardinality);
            boolean addC = accept(r, cCardinality);
            
            if (addX) {
                x.add(parsed);
            }
            if (addY) {
                y.add(parsed);
            }
            
            if (addA) {
                a.add(parsed);
            }
            if (addB) {
                b.add(parsed);
            }
            if (addC) {
                c.add(parsed);
            }
            
            if (addX && addY && (addA || addB || addC)) {
                expected.add(parsed);
            }
        }
        
        Collections.sort(expected);
        
        // convert to ArrayIterators
        StatsWrappedIterator xSource = new StatsWrappedIterator("X", new ArrayIterator(x.toArray(new String[0])));
        StatsWrappedIterator ySource = new StatsWrappedIterator("Y", new ArrayIterator(y.toArray(new String[0])));
        
        List<NestedIterator<String>> sources = new ArrayList<>(3);
        
        StatsWrappedIterator aSource = new StatsWrappedIterator("A", new ArrayIterator(a.toArray(new String[0])));
        StatsWrappedIterator bSource = new StatsWrappedIterator("B", new ArrayIterator(b.toArray(new String[0])));
        StatsWrappedIterator cSource = new StatsWrappedIterator("C", new ArrayIterator(c.toArray(new String[0])));
        
        sources.add(aSource);
        sources.add(bSource);
        sources.add(cSource);
        
        StatsWrappedIterator or = new StatsWrappedIterator("A OR B OR C", new OrIterator<>(sources));
        
        sources = new ArrayList<>(3);
        sources.add(xSource);
        sources.add(ySource);
        sources.add(or);
        
        StatsWrappedIterator and = new StatsWrappedIterator("X AND Y AND (A OR B OR C)", new AndIterator<>(sources));
        and.initialize();
        
        int lastIndex = -1;
        while (and.hasNext()) {
            Comparable<String> next = and.next();
            int expectedIndex = -1;
            for (int i = lastIndex + 1; i < expected.size(); i++) {
                if (expected.get(i) == next) {
                    expectedIndex = i;
//                    System.out.println("found " + next);
                    break;
                } else if (next.compareTo(expected.get(i)) < 0) {
                    Assert.assertFalse( "unexpected result: " + next, true);
                }
            }
            
            if (expectedIndex - lastIndex > 1) {
                Assert.assertFalse( "skipped an expected result: " + next + " moved " + (expectedIndex - lastIndex) + " spots", true);
            }
            
            if (expectedIndex == -1) {
                Assert.assertFalse( "unexpected result: " + next, true);
            } else {
                lastIndex = expectedIndex;
            }
        }
        
        System.out.println("hits: " + and.getNextCount());
        System.out.println("source X next: " + xSource.getNextCount());
        System.out.println("source X move: " + xSource.getMoveCount());
        System.out.println("source Y next: " + ySource.getNextCount());
        System.out.println("source Y move: " + ySource.getMoveCount());
        System.out.println("(A OR B OR C) next: " + or.getNextCount());
        System.out.println("(A OR B OR C) move: " + or.getMoveCount());
        System.out.println("source A next: " + aSource.getNextCount());
        System.out.println("source A move: " + aSource.getMoveCount());
        System.out.println("source B next: " + bSource.getNextCount());
        System.out.println("source B move: " + bSource.getMoveCount());
        System.out.println("source C next: " + cSource.getNextCount());
        System.out.println("source C move: " + cSource.getMoveCount());
        Assert.assertEquals(expected.size(), and.getNextCount());
    }
    
    private boolean accept(Random r, double accept) {
        return r.nextDouble() <= accept;
    }
    
    public static class StatsWrappedIterator<T extends Comparable<T>> implements NestedIterator<T> {
        private String name;
        private NestedIterator<T> delegate;
        
        private long nextCount = 0;
        private long moveCount = 0;
        
        public StatsWrappedIterator(NestedIterator<T> delegate) {
            this(null, delegate);
        }
        
        public StatsWrappedIterator(String name, NestedIterator<T> delegate) {
            this.name = name;
            this.delegate = delegate;
        }
        
        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }
        
        @Override
        public T next() {
            nextCount++;
            T result = delegate.next();
            if (result == null) {
                System.out.println(this + " exhausted after next");
            }
            
            return result;
        }
        
        @Override
        public void initialize() {
            delegate.initialize();
        }
        
        @Override
        public T move(T minimum) {
            moveCount++;
            T result = delegate.move(minimum);
            if (result == null) {
                System.out.println(this + " exhausted after move " + minimum);
            }
            
            return result;
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
        
        @Override
        public String toString() {
            if (name != null) {
                return "[" + name + " next=" + peek() + "]";
            } else {
                return super.toString();
            }
        }
    }
}
