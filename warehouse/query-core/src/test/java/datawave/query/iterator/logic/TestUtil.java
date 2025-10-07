package datawave.query.iterator.logic;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;

/**
 * Collection of utility methods used to test the field index iterators
 */
public class TestUtil {

    private static final Random rand = new Random();

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
            Attribute<?> attr = entry.getValue();
            assertAttributeUid(uid, attr);
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
    private static void assertAttributeUid(String uid, Attribute<?> attr) {
        if (attr instanceof Attributes) {
            Attributes attributes = (Attributes) attr;
            for (Attribute<?> attribute : attributes.getAttributes()) {
                assertAttributeUid(uid, attribute);
            }
        }

        String docUid = uidFromKey(attr.getMetadata());
        assertEquals(uid, docUid, "expected " + uid + " but found " + docUid);
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
