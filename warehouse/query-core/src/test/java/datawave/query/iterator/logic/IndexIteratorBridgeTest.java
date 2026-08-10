package datawave.query.iterator.logic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iteratorsImpl.system.IterationInterruptedException;
import org.apache.commons.jexl3.parser.JexlNode;
import org.junit.jupiter.api.Test;

import datawave.query.attributes.Document;
import datawave.query.jexl.JexlNodeFactory;

public class IndexIteratorBridgeTest {

    private final SortedSet<String> uids = new TreeSet<>(Arrays.asList("a", "b", "c", "d", "e"));

    @Test
    void testIteration() {
        IndexIteratorBridge itr = createIndexIteratorBridge("FIELD_A", uids);
        driveIterator(itr, "FIELD_A", uids);
    }

    @Test
    void testIterationInterrupted() {
        IndexIteratorBridge itr = createInterruptibleIndexIteratorBridge("FIELD_A", uids, true, 2);
        assertThrows(IterationInterruptedException.class, () -> driveIterator(itr, "FIELD_A", uids));
    }

    @Test
    void testIterationInterruptedOnThirdNext() {
        IndexIteratorBridge itr = createInterruptibleIndexIteratorBridge("FIELD_A", uids, true, 3);

        itr.seek(new Range(), Collections.emptyList(), false);
        assertTrue(itr.hasNext());
        itr.next();

        assertTrue(itr.hasNext());
        assertThrows(IterationInterruptedException.class, itr::next);
    }

    // === assert methods ===

    private void driveIterator(IndexIteratorBridge itr, String field, SortedSet<String> uids) {

        itr.seek(new Range(), Collections.emptyList(), false);

        for (String uid : uids) {
            assertTrue(itr.hasNext());

            Key tk = itr.next();
            IndexIteratorTest.assertTopKey(tk, uid);

            Document d = itr.document();
            if (itr.isNonEventField()) {
                IndexIteratorTest.assertDocumentField(field, d);
                IndexIteratorTest.assertDocumentUid(uid, d);
            } else {
                assertEquals(0, d.size());
            }
        }

        assertFalse(itr.hasNext());
    }

    // === helper methods ===

    public static IndexIteratorBridge createIndexIteratorBridge(String field, SortedSet<String> uids) {
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
