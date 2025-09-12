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

public class OrIteratorYieldingIT extends BaseNestedIteratorYieldingTest {

    private static final Logger log = LoggerFactory.getLogger(OrIteratorYieldingIT.class);

    private final int maxIterations = 1000;

    @Test
    public void testSimpleUnion() throws Exception {
        log.info("test simple union: A || B");
        for (int i = 0; i < maxIterations; i++) {
            rebuildUids();
            WaitWindowObserver observer = new TestWaitWindowObserver(100, 33);
            observer.setYieldCallback(new YieldCallback<>());
            drive(observer, this::createSimpleUnion, this::getExpectedForSimpleUnion);
        }
    }

    @Test
    public void testLargeUnion() throws Exception {
        log.info("test large union: A || B || C || D");
        for (int i = 0; i < maxIterations; i++) {
            rebuildUids();
            WaitWindowObserver observer = new TestWaitWindowObserver(100, 33);
            observer.setYieldCallback(new YieldCallback<>());
            drive(observer, this::createLargeUnion, this::getExpectedForLargeUnion);
        }
    }

    private NestedIterator<Key> createSimpleUnion(WaitWindowObserver observer) {
        // A || B
        NestedIterator<Key> a = createIterator("A", new TreeSet<>(uidsA));
        NestedIterator<Key> b = createIterator("B", new TreeSet<>(uidsB));
        return new OrIterator<>(List.of(a, b), null, observer);
    }

    private NestedIterator<Key> createLargeUnion(WaitWindowObserver observer) {
        // A || B || C || D
        NestedIterator<Key> a = createIterator("A", new TreeSet<>(uidsA));
        NestedIterator<Key> b = createIterator("B", new TreeSet<>(uidsB));
        NestedIterator<Key> c = createIterator("C", new TreeSet<>(uidsC));
        NestedIterator<Key> d = createIterator("D", new TreeSet<>(uidsD));
        return new OrIterator<>(List.of(a, b, c, d), null, observer);
    }

    private SortedSet<String> getExpectedForSimpleUnion() {
        // A || B
        SortedSet<String> a = new TreeSet<>(uidsA);
        a.addAll(uidsB);
        return a;
    }

    private SortedSet<String> getExpectedForLargeUnion() {
        // A || B || C || D
        SortedSet<String> a = new TreeSet<>(uidsA);
        a.addAll(uidsB);
        a.addAll(uidsC);
        a.addAll(uidsD);
        return a;
    }
}
