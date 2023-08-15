package datawave.query.iterator.logic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iteratorsImpl.system.IterationInterruptedException;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

import datawave.query.attributes.Document;
import datawave.query.jexl.JexlNodeFactory;

class IndexIteratorBridgeTest {

    private static final Logger log = Logger.getLogger(IndexIteratorBridgeTest.class);

    private final SortedSet<String> uids = new TreeSet<>(Arrays.asList("a", "b", "c", "d", "e"));

    @Test
    void testIteration() {
        IndexIteratorBridge itr = createIndexIteratorBridge("FIELD_A", uids);
        driveIterator(itr, "FIELD_A", uids);
    }

    // typical iterator can be exhausted by calling next
    @Test
    void testIterationViaNext() {
        IndexIteratorBridge itr = createIndexIteratorBridge("FIELD_A", uids);
        itr.seek(new Range(), Collections.emptySet(), false);
        assertNotNull(itr.next());
        assertNotNull(itr.next());
        assertNotNull(itr.next());
        assertNotNull(itr.next());
        assertNotNull(itr.next());
        assertFalse(itr.hasNext());
        assertNull(itr.next());
    }

    @Test
    void testIterationInterrupted() {
        IndexIteratorBridge itr = createInterruptibleIndexIteratorBridge("FIELD_A", uids, true, 1);
        assertThrows(IterationInterruptedException.class, () -> driveIterator(itr, "FIELD_A", uids));
    }

    @Test
    void testIterationInterruptedOnThirdNext() {
        IndexIteratorBridge itr = createInterruptibleIndexIteratorBridge("FIELD_A", uids, true, 2);

        itr.seek(new Range(), Collections.emptyList(), false);
        assertTrue(itr.hasNext());
        itr.next();

        assertTrue(itr.hasNext());
        assertThrows(IterationInterruptedException.class, itr::next);
    }

    @Test
    void testIteratorWithIteration() {
        SortedSet<String> uids = TestUtil.randomUids(100, 50);
        IndexIteratorBridge itr = createIndexIteratorBridge("FIELD_A", uids);
        driveIterator(itr, "FIELD_A", uids);
    }

    @Test
    void testIteratorWithContext() {
        for (int i = 0; i < 100; i++) {
            SortedSet<String> uids = TestUtil.randomUids(100, 30);
            SortedSet<String> context = TestUtil.randomUids(100, 30);
            driveIteratorWithContext(uids, context);
        }
    }

    @Test
    void testCase01() {
        SortedSet<String> uids = new TreeSet<>(List.of("1", "10", "9"));
        SortedSet<String> context = new TreeSet<>(List.of("10", "7", "9"));
        driveIteratorWithContext(uids, context); // expected [10, 9]
    }

    @Test
    void testCase02() {
        SortedSet<String> uids = new TreeSet<>(List.of("1", "3", "9"));
        SortedSet<String> context = new TreeSet<>(List.of("1", "2", "4"));
        driveIteratorWithContext(uids, context); // expected [1]
    }

    @Test
    void testCase03() {
        SortedSet<String> uids = new TreeSet<>(List.of("2", "3", "9"));
        SortedSet<String> context = new TreeSet<>(List.of("10", "2", "8"));
        driveIteratorWithContext(uids, context); // expected [2]
    }

    // === assert methods ===

    private void driveIterator(IndexIteratorBridge itr, String field, SortedSet<String> uids) {

        itr.seek(new Range(), Collections.emptyList(), false);

        SortedSet<String> foundUids = new TreeSet<>();

        while (itr.hasNext()) {

            Key tk = itr.next();
            String uid = TestUtil.uidFromKey(tk);
            foundUids.add(uid);

            Document d = itr.document();
            if (itr.isNonEventField()) {
                IndexIteratorTest.assertDocumentField(field, d);
                IndexIteratorTest.assertDocumentUid(uid, d);
            } else {
                assertEquals(0, d.size());
            }
        }

        assertFalse(itr.hasNext());
        assertEquals(uids, foundUids);
    }

    private void driveIteratorWithContext(SortedSet<String> uids, SortedSet<String> context) {
        IndexIteratorBridge itr = createIndexIteratorBridge("FIELD_A", uids, true);
        SortedSet<String> expected = new TreeSet<>(Sets.intersection(uids, context));

        // log.info("uids : " + uids);
        // log.info("context : " + context);
        // log.info("expected: " + expected);

        driveIteratorWithContext(itr, context, expected);
    }

    private void driveIteratorWithContext(IndexIteratorBridge itr, SortedSet<String> context, SortedSet<String> expected) {
        itr.seek(new Range(), Collections.emptyList(), false);

        SortedSet<String> foundUids = new TreeSet<>();

        for (String ctx : context) {

            Key contextKey = TestUtil.createContextKey(ctx);
            Key key = itr.move(contextKey);

            if (key == null) {
                log.trace("move: " + ctx + " result: null");
                assertFalse(itr.hasNext());
                assertEquals(expected, foundUids);
                continue;
            }

            // uid does not need to equal the context for the IIB
            log.trace("move: " + ctx + " result: " + TestUtil.uidFromKey(key));
            String uid = TestUtil.uidFromKey(key);
            if (ctx.equals(uid)) {
                foundUids.add(uid);
            }

            // document uid must be consistent with what was returned from move()
            Document document = itr.document();
            assertTrue(document.containsKey(itr.getField()));
            TestUtil.assertDocumentUids(uid, document);
        }

        // hasNext() is not checked because the context may end prior to iterator exhaustion
        assertEquals(expected, foundUids);
    }

    // === helper methods ===

    protected static IndexIteratorBridge createIndexIteratorBridge(String field, SortedSet<String> uids) {
        return createIndexIteratorBridge(field, uids, false, -1);
    }

    protected static IndexIteratorBridge createIndexIteratorBridge(String field, SortedSet<String> uids, boolean buildDocument) {
        return createIndexIteratorBridge(field, uids, buildDocument, -1);
    }

    protected static IndexIteratorBridge createInterruptibleIndexIteratorBridge(String field, SortedSet<String> uids, int maxIterations) {
        return createIndexIteratorBridge(field, uids, false, maxIterations);
    }

    protected static IndexIteratorBridge createInterruptibleIndexIteratorBridge(String field, SortedSet<String> uids, boolean buildDocument,
                    int maxIterations) {
        return createIndexIteratorBridge(field, uids, buildDocument, maxIterations);
    }

    private static IndexIteratorBridge createIndexIteratorBridge(String field, SortedSet<String> uids, boolean buildDocument, int maxIterations) {
        IndexIterator indexIterator;
        if (maxIterations == -1) {
            indexIterator = IndexIteratorTest.createIndexIterator(field, uids, buildDocument);
        } else {
            indexIterator = IndexIteratorTest.createInterruptibleIndexIterator(field, uids, buildDocument, maxIterations);
        }

        JexlNode node = JexlNodeFactory.buildEQNode(field, "value");
        IndexIteratorBridge bridge = new IndexIteratorBridge(indexIterator, node, field);
        if (buildDocument) {
            // building documents for index only fields, must set the non-event field flag
            bridge.setNonEventField(true);
        }
        return bridge;
    }
}
