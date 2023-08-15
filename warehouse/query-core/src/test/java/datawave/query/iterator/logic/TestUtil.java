package datawave.query.iterator.logic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.log4j.Logger;

import com.google.common.collect.Sets;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.iterator.NestedIterator;

/**
 * Collection of utility methods used to test the field index iterators
 */
public class TestUtil {

    private static final Logger log = Logger.getLogger(TestUtil.class);

    private static final Random rand = new Random();

    /**
     * Drives a nested iterator, asserting against the expected uids
     *
     * @param itr
     *            the nested iterator
     * @param uids
     *            expected uids
     */
    public static void driveIterator(NestedIterator<Key> itr, SortedSet<String> uids) {
        driveIterator(itr, uids, Collections.emptyMap());
    }

    /**
     * Drives a nested iterator, asserting against the expected uids and expected field counts
     *
     * @param itr
     *            the nested iterator
     * @param uids
     *            the expected uids
     * @param indexOnlyCounts
     *            the expected index only field counts
     */
    public static void driveIterator(NestedIterator<Key> itr, SortedSet<String> uids, Map<String,Set<String>> indexOnlyCounts) {
        try {
            itr.seek(new Range(), Collections.emptySet(), true);
        } catch (IOException e) {
            fail("Failed to seek OrIterator during test setup");
        }

        int count = 0;
        Set<String> foundUids = new TreeSet<>();
        Map<String,Set<String>> counts = new HashMap<>();

        while (itr.hasNext()) {
            count++;

            Key key = itr.next();
            String uid = uidFromKey(key);
            foundUids.add(uid);
            log.trace("uid: " + uid);

            if (!indexOnlyCounts.isEmpty()) {
                Document document = itr.document();
                assertDocumentUids(uid, document);

                for (String indexOnlyField : indexOnlyCounts.keySet()) {
                    if (document.containsKey(indexOnlyField)) {
                        Set<String> fieldUids = counts.getOrDefault(indexOnlyField, new HashSet<>());
                        fieldUids.add(uid);
                        counts.put(indexOnlyField, fieldUids);
                    }
                }
            }
        }

        assertFalse(itr.hasNext(), "iterator had more elements");
        if (uids.size() > foundUids.size()) {
            log.warn("fewer uids found than expected: " + Sets.difference(uids, foundUids));
        } else if (uids.size() < foundUids.size()) {
            log.warn("more uids found than expected: " + Sets.difference(foundUids, uids));
        } else {
            assertEquals(uids, foundUids);
        }

        assertEquals(uids.size(), count, "expected next count did not match");
        assertEquals(indexOnlyCounts, counts);
    }

    // helper method
    public static Key createContextKey(String uid) {
        return new Key("20220314_17", "datatype\0" + uid);
    }

    /**
     * Extract the uid from a key with a structure like <code>row datatype\0uid</code>
     *
     * @param key
     *            the key
     * @return a uid
     */
    public static String uidFromKey(Key key) {
        String cf = key.getColumnFamily().toString();
        return cf.split("\0")[1];
    }

    /**
     * Assert that all document attributes match the provided uid
     *
     * @param uid
     *            a uid
     * @param document
     *            a document
     */
    public static void assertDocumentUids(String uid, Document document) {
        for (Map.Entry<String,Attribute<? extends Comparable<?>>> entry : document.entrySet()) {
            String field = entry.getKey();
            Attribute<?> attr = entry.getValue();
            assertAttributeUid(uid, field, attr);
        }
    }

    /**
     * Asserts that a single attribute matches the provided uid.
     *
     * @param uid
     *            a uid
     * @param attr
     *            an {@link Attribute} that may be an instance of {@link Attributes}
     */
    private static void assertAttributeUid(String uid, String field, Attribute<?> attr) {
        if (attr instanceof Attributes) {
            Attributes attributes = (Attributes) attr;
            for (Attribute<?> attribute : attributes.getAttributes()) {
                assertAttributeUid(uid, field, attribute);
            }
        }

        String docUid = uidFromKey(attr.getMetadata());
        assertEquals(uid, docUid, "expected " + uid + " but found " + docUid + " for field " + field);
    }

    /**
     * Generate a number of random values, used to mock out uids in the field index
     *
     * @param numUids
     *            the number of elements to generate
     * @return a sorted set of random elements
     */
    public static SortedSet<String> randomUids(int numUids) {
        if (numUids > 100) {
            throw new IllegalArgumentException("Number of elements to generate must be less than 100");
        }
        return randomUids(100, numUids);
    }

    /**
     * Generate a number of random values equal to the provided bound. Used to mock out uids in the field index.
     *
     * @param bound
     *            the maximum size of the generated element
     * @param numUids
     *            the number of elements
     * @return a sorted set of random elements
     */
    public static SortedSet<String> randomUids(int bound, int numUids) {
        if (numUids > bound) {
            throw new IllegalArgumentException("Number of elements to generate must be less than the bound");
        }

        SortedSet<String> uids = new TreeSet<>();

        while (uids.size() < numUids) {
            int i = 1 + rand.nextInt(bound);
            uids.add(String.valueOf(i));
        }

        return uids;
    }
}
