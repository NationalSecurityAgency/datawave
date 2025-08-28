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

/**
 * A suite of tests that exercise the following:
 * <ul>
 * <li>single nested union</li>
 * <li>single nested intersection</li>
 * <li>double nested union</li>
 * <li>double nested intersection</li>
 * <li>nested intersection with negation</li>
 * </ul>
 */
public class AndOrIteratorYieldingIT extends BaseNestedIteratorYieldingTest {

    private static final Logger log = LoggerFactory.getLogger(AndOrIteratorYieldingIT.class);

    private final int maxIterations = 1000;

    // A && (B || C)
    @Test
    public void testNestedUnion() throws Exception {
        log.info("nested union: A && (B || C)");
        for (int i = 0; i < maxIterations; i++) {
            rebuildUids();
            WaitWindowObserver observer = new TestWaitWindowObserver(100, 20);
            observer.setYieldCallback(new YieldCallback<>());
            drive(observer, this::createNestedUnion, this::expectedForNestedUnion);
        }
    }

    // A || (B && C)
    @Test
    public void testNestedIntersection() throws Exception {
        log.info("nested intersection: A || (B && C)");
        for (int i = 0; i < maxIterations; i++) {
            rebuildUids();
            WaitWindowObserver observer = new TestWaitWindowObserver(100, 20);
            observer.setYieldCallback(new YieldCallback<>());
            drive(observer, this::createNestedIntersection, this::expectedForNestedIntersection);
        }
    }

    // (A || B) && (C || D)
    @Test
    public void testDoubleNestedUnion() throws Exception {
        log.info("double nested union: (A || B) && (C || D)");
        for (int i = 0; i < maxIterations; i++) {
            rebuildUids();
            WaitWindowObserver observer = new TestWaitWindowObserver(100, 20);
            observer.setYieldCallback(new YieldCallback<>());
            drive(observer, this::createDoubleNestedUnion, this::expectedForDoubleNestedUnion);
        }
    }

    // (A && B) || (C && D)
    @Test
    public void testDoubleNestedIntersection() throws Exception {
        log.info("double nested intersection: (A && B) || (C && D)");
        for (int i = 0; i < maxIterations; i++) {
            rebuildUids();
            WaitWindowObserver observer = new TestWaitWindowObserver(100, 20);
            observer.setYieldCallback(new YieldCallback<>());
            drive(observer, this::createDoubleNestedIntersection, this::expectedForDoubleNestedIntersection);
        }
    }

    // A || (B && !C)
    @Test
    public void testNestedPartiallyNegatedIntersection() throws Exception {
        log.info("nested partially negated intersection: A || (B && !C)");
        for (int i = 0; i < maxIterations; i++) {
            rebuildUids();
            WaitWindowObserver observer = new TestWaitWindowObserver(100, 25);
            observer.setYieldCallback(new YieldCallback<>());
            drive(observer, this::createNestedPartiallyNegatedIntersection, this::expectedForNestedPartiallyNegatedUnion);
        }
    }

    // A && (B || (C && D))
    @Test
    public void testDeeplyNestedQuery() throws Exception {
        log.info("deeply nested query: A || (B && (C || D)");
        for (int i = 0; i < maxIterations; i++) {
            rebuildUids();
            WaitWindowObserver observer = new TestWaitWindowObserver(100, 25);
            observer.setYieldCallback(new YieldCallback<>());
            drive(observer, this::createDeeplyNestedQuery, this::expectedForDeeplyNestedQuery);
        }
    }

    /**
     * Create a NestedIterator for a single nested union
     *
     * @param observer
     *            the WaitWindowObserver
     * @return a NestedIterator
     */
    private NestedIterator<Key> createNestedUnion(WaitWindowObserver observer) {
        // A && (B || C)
        NestedIterator<Key> anchor = createIterator("A", new TreeSet<>(uidsA));

        NestedIterator<Key> left = createIterator("B", new TreeSet<>(uidsB));
        NestedIterator<Key> right = createIterator("C", new TreeSet<>(uidsC));
        OrIterator<Key> union = new OrIterator<>(List.of(left, right), null, observer);

        return new AndIterator<>(List.of(anchor, union), null, observer);
    }

    /**
     * Create a NestedIterator for a single nested union
     *
     * @param observer
     *            the WaitWindowObserver
     * @return a NestedIterator
     */
    private NestedIterator<Key> createNestedIntersection(WaitWindowObserver observer) {
        // A || (B && C)
        NestedIterator<Key> anchor = createIterator("A", new TreeSet<>(uidsA));

        NestedIterator<Key> left = createIterator("B", new TreeSet<>(uidsB));
        NestedIterator<Key> right = createIterator("C", new TreeSet<>(uidsC));
        AndIterator<Key> intersection = new AndIterator<>(List.of(left, right), null, observer);

        return new OrIterator<>(List.of(anchor, intersection), null, observer);
    }

    /**
     * Create a NestedIterator for a double nested union
     *
     * @param observer
     *            the WaitWindowObserver
     * @return a NestedIterator
     */
    private NestedIterator<Key> createDoubleNestedUnion(WaitWindowObserver observer) {
        // (A || B) && (C || D)
        NestedIterator<Key> a = createIterator("A", new TreeSet<>(uidsA));
        NestedIterator<Key> b = createIterator("B", new TreeSet<>(uidsB));
        OrIterator<Key> leftUnion = new OrIterator<>(List.of(a, b), null, observer);

        NestedIterator<Key> c = createIterator("C", new TreeSet<>(uidsC));
        NestedIterator<Key> d = createIterator("D", new TreeSet<>(uidsD));
        OrIterator<Key> rightUnion = new OrIterator<>(List.of(c, d), null, observer);

        return new AndIterator<>(List.of(leftUnion, rightUnion), null, observer);
    }

    /**
     * Create a NestedIterator for a double nested union
     *
     * @param observer
     *            the WaitWindowObserver
     * @return a NestedIterator
     */
    private NestedIterator<Key> createDoubleNestedIntersection(WaitWindowObserver observer) {
        // (A && B) || (C && D)
        NestedIterator<Key> a = createIterator("A", new TreeSet<>(uidsA));
        NestedIterator<Key> b = createIterator("B", new TreeSet<>(uidsB));
        AndIterator<Key> leftIntersection = new AndIterator<>(List.of(a, b), null, observer);

        NestedIterator<Key> c = createIterator("C", new TreeSet<>(uidsC));
        NestedIterator<Key> d = createIterator("D", new TreeSet<>(uidsD));
        AndIterator<Key> rightIntersection = new AndIterator<>(List.of(c, d), null, observer);

        return new OrIterator<>(List.of(leftIntersection, rightIntersection), null, observer);
    }

    /**
     * Create a NestedIterator for a single nested union that contains a negation
     *
     * @param observer
     *            the WaitWindowObserver
     * @return a NestedIterator
     */
    private NestedIterator<Key> createNestedPartiallyNegatedIntersection(WaitWindowObserver observer) {
        // A || (B && !C)
        NestedIterator<Key> anchor = createIterator("A", new TreeSet<>(uidsA));

        NestedIterator<Key> left = createIterator("B", new TreeSet<>(uidsB));
        NestedIterator<Key> right = createIterator("C", new TreeSet<>(uidsC));
        AndIterator<Key> intersection = new AndIterator<>(List.of(left), List.of(right), observer);

        return new OrIterator<>(List.of(anchor, intersection), null, observer);
    }

    /**
     * Create a NestedIterator for a deeply nested query
     *
     * @param observer
     *            the WaitWindowObserver
     * @return a NestedIterator
     */
    private NestedIterator<Key> createDeeplyNestedQuery(WaitWindowObserver observer) {
        // A && (B || (C && D)
        NestedIterator<Key> a = createIterator("A", new TreeSet<>(uidsA));
        NestedIterator<Key> b = createIterator("B", new TreeSet<>(uidsB));
        NestedIterator<Key> c = createIterator("C", new TreeSet<>(uidsC));
        NestedIterator<Key> d = createIterator("D", new TreeSet<>(uidsD));

        AndIterator<Key> cd = new AndIterator<>(List.of(c, d), null, observer);
        OrIterator<Key> bcd = new OrIterator<>(List.of(b, cd), null, observer);
        return new AndIterator<>(List.of(a, bcd), null, observer);
    }

    private SortedSet<String> expectedForNestedUnion() {
        // A && (B || C)
        SortedSet<String> nested = new TreeSet<>(uidsB);
        nested.addAll(uidsC);

        SortedSet<String> expected = new TreeSet<>(uidsA);
        expected.retainAll(nested);
        return expected;
    }

    private SortedSet<String> expectedForNestedIntersection() {
        // A || (B && C)
        SortedSet<String> nested = new TreeSet<>(uidsB);
        nested.retainAll(uidsC);

        SortedSet<String> expected = new TreeSet<>(uidsA);
        expected.addAll(nested);
        return expected;
    }

    private SortedSet<String> expectedForDoubleNestedUnion() {
        // (A || B) && (C || D)
        SortedSet<String> left = new TreeSet<>(uidsA);
        left.addAll(uidsB);

        SortedSet<String> right = new TreeSet<>(uidsC);
        right.addAll(uidsD);

        left.retainAll(right);
        return left;
    }

    private SortedSet<String> expectedForDoubleNestedIntersection() {
        // (A && B) || (C && D)
        SortedSet<String> a = new TreeSet<>(uidsA);
        a.retainAll(uidsB);

        SortedSet<String> c = new TreeSet<>(uidsC);
        c.retainAll(uidsD);

        a.addAll(c);
        return a;
    }

    private SortedSet<String> expectedForNestedPartiallyNegatedUnion() {
        // A || (B && !C)
        SortedSet<String> a = new TreeSet<>(uidsA);

        SortedSet<String> b = new TreeSet<>(uidsB);
        b.removeAll(uidsC);

        a.addAll(b);
        return a;
    }

    private SortedSet<String> expectedForDeeplyNestedQuery() {
        // A && (B || (C && D)
        SortedSet<String> cd = new TreeSet<>(uidsC);
        cd.retainAll(uidsD);

        SortedSet<String> bcd = new TreeSet<>(uidsB);
        bcd.addAll(cd);

        SortedSet<String> abcd = new TreeSet<>(uidsA);
        abcd.retainAll(bcd);
        return abcd;
    }
}
