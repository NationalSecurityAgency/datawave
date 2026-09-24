package datawave.query.iterator.logic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.YieldCallback;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.query.exceptions.WaitWindowOverrunException;
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

    @Test
    public void testDeferredIncludeYieldRetriesCurrentContext() throws IOException {
        WaitWindowObserver observer = new TestWaitWindowObserver(100, 0);
        Range range = new Range();
        observer.setSeekRange(range);

        Key firstContext = createDocumentKey("a");
        Key nextContext = createDocumentKey("c");
        Key unsafeYield = createDocumentKey("z");

        NestedIterator<Key> include = createIterator("A", new TreeSet<>(List.of("f")));
        NestedIterator<Key> deferredInclude = new YieldingContextIterator(unsafeYield);
        NestedIterator<Key> matchingDeferredInclude = new MatchingContextIterator(nextContext);
        OrIterator<Key> iterator = new OrIterator<>(List.of(include, deferredInclude, matchingDeferredInclude), null, observer);
        iterator.setContext(firstContext);
        iterator.seek(range, Collections.emptySet(), false);
        iterator.initialize();

        iterator.setContext(nextContext);
        WaitWindowOverrunException exception = assertThrows(WaitWindowOverrunException.class, () -> iterator.move(nextContext));

        assertEquals(nextContext, WaitWindowObserver.removeMarkers(exception.getYieldKey().getLeft()));
    }

    @Test
    public void testDeferredIncludeYieldDuringInitializeRetriesCurrentContext() throws IOException {
        WaitWindowObserver observer = new TestWaitWindowObserver(100, 0);
        Range range = new Range();
        observer.setSeekRange(range);

        Key context = createDocumentKey("c");
        Key unsafeYield = createDocumentKey("z");
        NestedIterator<Key> include = createIterator("A", new TreeSet<>(List.of("f")));
        NestedIterator<Key> yieldingDeferredInclude = new YieldingContextIterator(unsafeYield, 0);
        NestedIterator<Key> matchingDeferredInclude = new MatchingContextIterator(context);
        OrIterator<Key> iterator = new OrIterator<>(List.of(include, yieldingDeferredInclude, matchingDeferredInclude), null, observer);
        iterator.setContext(context);
        iterator.seek(range, Collections.emptySet(), false);

        WaitWindowOverrunException exception = assertThrows(WaitWindowOverrunException.class, iterator::initialize);

        assertEquals(context, WaitWindowObserver.removeMarkers(exception.getYieldKey().getLeft()));
    }

    @Test
    public void testDeferredIncludeYieldInExactContextRetriesCurrentContext() throws IOException {
        WaitWindowObserver observer = new TestWaitWindowObserver(100, 0);
        Range range = new Range();
        observer.setSeekRange(range);

        Key firstContext = createDocumentKey("a");
        Key nextContext = createDocumentKey("c");
        Key unsafeYield = createDocumentKey("z");

        NestedIterator<Key> include = createIterator("A", new TreeSet<>(List.of("f")));
        NestedIterator<Key> deferredInclude = new YieldingContextIterator(unsafeYield);
        NestedIterator<Key> deferredExclude = new SeekableContextIterator(List.of(firstContext, nextContext));
        OrIterator<Key> iterator = new OrIterator<>(List.of(include, deferredInclude), List.of(deferredExclude), observer);
        iterator.setContext(firstContext);
        iterator.seek(range, Collections.emptySet(), false);
        iterator.initialize();

        iterator.setContext(nextContext);
        WaitWindowOverrunException exception = assertThrows(WaitWindowOverrunException.class, () -> iterator.moveContext(nextContext));

        assertEquals(nextContext, WaitWindowObserver.removeMarkers(exception.getYieldKey().getLeft()));
    }

    @Test
    public void testCachedIncludeAtContextRetainsItsDocumentKey() throws IOException {
        Key excludedContext = createDocumentKey("a");
        Key matchingContext = createDocumentKey("b");

        NestedIterator<Key> include = createIterator("A", new TreeSet<>(List.of("b")));
        NestedIterator<Key> deferredExclude = createIterator("C", new TreeSet<>(List.of("a")));
        OrIterator<Key> iterator = new OrIterator<>(List.of(include), List.of(deferredExclude));
        iterator.setContext(excludedContext);
        iterator.seek(new Range(), Collections.emptySet(), false);
        iterator.initialize();

        iterator.setContext(matchingContext);
        Key result = iterator.move(matchingContext);

        assertEquals(matchingContext, datawave.query.iterator.Util.keyTransformer().transform(result));
        assertEquals("A\0value", result.getColumnQualifier().toString());
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

    private Key createDocumentKey(String uid) {
        return new Key("20250606_0", "datatype\0" + uid);
    }

    private static class YieldingContextIterator extends NegationFilterTest.Itr<Key> {
        private final Key yieldKey;
        private final int successfulMoves;
        private int moves;

        private YieldingContextIterator(Key yieldKey) {
            this(yieldKey, 1);
        }

        private YieldingContextIterator(Key yieldKey, int successfulMoves) {
            super(Collections.emptyList(), true);
            this.yieldKey = yieldKey;
            this.successfulMoves = successfulMoves;
        }

        @Override
        public Key move(Key minimum) {
            if (moves++ < successfulMoves) {
                return null;
            }
            throw new WaitWindowOverrunException(Pair.of(yieldKey, "forced deferred-include yield"));
        }

        @Override
        public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) {}
    }

    private static class SeekableContextIterator extends NegationFilterTest.Itr<Key> {
        private SeekableContextIterator(List<Key> values) {
            super(values, true);
        }

        @Override
        public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) {}
    }

    private static class MatchingContextIterator extends NegationFilterTest.Itr<Key> {
        private final Key match;

        private MatchingContextIterator(Key match) {
            super(Collections.emptyList(), true);
            this.match = match;
        }

        @Override
        public Key move(Key minimum) {
            return match.equals(minimum) ? match : null;
        }

        @Override
        public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) {}
    }
}
