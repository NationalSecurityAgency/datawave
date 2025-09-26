package datawave.query.iterator.logic;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.iterators.YieldCallback;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.TestWaitWindowObserver;
import datawave.query.iterator.waitwindow.WaitWindowObserver;

public class AndIteratorYieldingIT extends BaseNestedIteratorYieldingTest {

    private static final Logger log = LoggerFactory.getLogger(AndIteratorYieldingIT.class);

    private final int maxIterations = 1000;

    @Test
    public void testSimpleIntersection() throws Exception {
        log.info("test simple intersection: A && B");
        for (int i = 0; i < maxIterations; i++) {
            rebuildUids();
            WaitWindowObserver observer = new TestWaitWindowObserver(100, 33);
            observer.setYieldCallback(new YieldCallback<>());
            drive(observer, this::createSimpleIntersection, this::getExpectedForSimpleIntersection);
        }
    }

    @Test
    public void testLargeIntersection() throws Exception {
        log.info("test large intersection: A && B && C && D");
        for (int i = 0; i < maxIterations; i++) {
            rebuildUids();
            WaitWindowObserver observer = new TestWaitWindowObserver(100, 33);
            observer.setYieldCallback(new YieldCallback<>());
            drive(observer, this::createLargeIntersection, this::getExpectedForLargeIntersection);
        }
    }

    @Test
    public void testAndNot() throws Exception {
        log.info("test and not: A && !B");
        for (int i = 0; i < maxIterations; i++) {
            rebuildUids();
            WaitWindowObserver observer = new TestWaitWindowObserver(100, 33);
            observer.setYieldCallback(new YieldCallback<>());
            drive(observer, this::createAndNot, this::getExpectedForAndNot);
        }
    }

    private NestedIterator<Key> createSimpleIntersection(WaitWindowObserver observer) {
        // A && B
        NestedIterator<Key> a = createIterator("A", new TreeSet<>(uidsA));
        NestedIterator<Key> b = createIterator("B", new TreeSet<>(uidsB));
        return new AndIterator<>(List.of(a, b), null, observer);
    }

    private NestedIterator<Key> createLargeIntersection(WaitWindowObserver observer) {
        // A && B && C && D
        NestedIterator<Key> a = createIterator("A", new TreeSet<>(uidsA));
        NestedIterator<Key> b = createIterator("B", new TreeSet<>(uidsB));
        NestedIterator<Key> c = createIterator("C", new TreeSet<>(uidsC));
        NestedIterator<Key> d = createIterator("D", new TreeSet<>(uidsD));
        return new AndIterator<>(List.of(a, b, c, d), null, observer);
    }

    private NestedIterator<Key> createAndNot(WaitWindowObserver observer) {
        // A && !B
        NestedIterator<Key> a = createIterator("A", new TreeSet<>(uidsA));
        NestedIterator<Key> b = createIterator("B", new TreeSet<>(uidsB));
        return new AndIterator<>(List.of(a), List.of(b), observer);
    }

    private SortedSet<String> getExpectedForSimpleIntersection() {
        // A && B
        SortedSet<String> a = new TreeSet<>(uidsA);
        a.retainAll(uidsB);
        return a;
    }

    private SortedSet<String> getExpectedForLargeIntersection() {
        // A && B && C && D
        SortedSet<String> a = new TreeSet<>(uidsA);
        a.retainAll(uidsB);
        a.retainAll(uidsC);
        a.retainAll(uidsD);
        return a;
    }

    private SortedSet<String> getExpectedForAndNot() {
        // A && !B
        SortedSet<String> a = new TreeSet<>(uidsA);
        a.removeAll(uidsB);
        return a;
    }
}
