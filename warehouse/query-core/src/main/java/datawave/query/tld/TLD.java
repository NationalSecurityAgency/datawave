package datawave.query.tld;

import static datawave.data.hash.UIDConstants.DEFAULT_SEPARATOR;
import static datawave.query.Constants.MAX_UNICODE_STRING;
import static datawave.query.Constants.NULL;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;

/**
 * Provides a collection of utility methods for operating with Top Level Document keys.
 * <ul>
 * <li>Parses field and value pairs from keys</li>
 * <li>Parses root pointers from keys</li>
 * <li>Parses parent pointers from keys</li>
 * <li>Builds parent keys for seeking</li>
 * </ul>
 */
public class TLD {
    private static final char NULL_BYTE = '\u0000';
    private static final char DOT = '.';

    private TLD() {}

    /**
     * Parses the datatype and uid from the local Field Index key's ColumnQualifier
     * <p>
     * <br>
     * FI Key Structure (row, cf='fi\0field', cq='value\0datatype\0uid')
     * <p>
     * <br>
     * We reverse traverse the ColumnQualifier until we find the second null byte and then return everything from that second null byte until the end.
     *
     * @param cq
     *            - a ByteSequence representing the Key's ColumnQualifier
     * @return - The documents datatype and uid: datatype\0uid
     */
    public static ByteSequence parseDatatypeUidFromFI(ByteSequence cq) {
        return cq.subSequence(findSecondNullReverse(cq) + 1, cq.length());
    }

    /**
     * Parse the parent pointer from a document id.
     * <p>
     * For example
     * <ul>
     * <li>parses 'parent.doc.id' from 'parent.doc.id'</li>
     * <li>parses 'parent.doc.id' from 'parent.doc.id.child'</li>
     * <li>parses 'parent.doc.id.child' from 'parent.doc.id.child.grandchild'</li>
     * </ul>
     *
     * We traverse the id and save the index each time we find a dot. Once we reach the end, if the number of dots is greater than 2, we return everything up to
     * the last dot. If the number of dots is 2 or less, we return the unchanged id because it is not a child.
     *
     * @param id
     *            - a ByteSequence containing the document id
     * @return - the parent id
     */
    public static ByteSequence parseParentPointerFromId(ByteSequence id) {
        int count = 0;
        int index = 0;
        for (int i = 0; i < id.length(); ++i) {
            if (id.byteAt(i) == DOT) {
                count++;
                index = i;
            }
        }

        if (count > 2) {
            return id.subSequence(0, index);
        }
        return id;
    }

    /**
     * Parses the FIELD and VALUE from a local Field Index
     * <p>
     * <br>
     * FI Key Structure (row, cf='fi\0field', cq='value\0datatype\0uid')
     * <p>
     * <br>
     * We first traverse the ColumnFamily until we find the null byte and save its index.
     * <p>
     * We then reverse traverse the ColumnQualifier until we find it's second null byte and save that index.
     * <p>
     * Finally, we copy the field and value into a new ByteSequence and return it.
     *
     * @param cf
     *            - the field index key's ColumnFamily
     * @param cq
     *            - the field index key's ColumnQualifier
     * @return - a byte sequence that is the field and value: fieldvalue
     */
    public static ByteSequence parseFieldAndValueFromFI(ByteSequence cf, ByteSequence cq) {
        // find null in ColumnFamily
        int cfIndex = findFirstNull(cf) + 1;

        int cqIndex = findSecondNullReverse(cq);

        int fieldSize = cf.length() - cfIndex;
        // create and return new ByteSequence of field and value
        byte[] fieldValue = new byte[fieldSize + 1 + cqIndex];
        System.arraycopy(cf.getBackingArray(), cfIndex + cf.offset(), fieldValue, 0, fieldSize);
        System.arraycopy(cq.getBackingArray(), cq.offset(), fieldValue, fieldSize + 1, cqIndex);
        return new ArrayByteSequence(fieldValue);
    }

    /**
     * Parses the FIELD and VALUE from a Term Frequency key.
     * <p>
     * <br>
     * TF Key Structure (row, cf='tf', cq='datatype\0uid\0value\0field')
     * <p>
     * <br>
     * We first traverse the ColumnQualifier to find the second null byte and save its index (start of value).
     * <p>
     * We then reverse traverse and save the index of the first null byte we find (split of value and field).
     * <p>
     * Finally, we create and return a new ByteSequence that has the field first and then value second.
     *
     * @param cq
     *            - the term frequency key's ColumnQualifier
     * @return - a byte sequence that is the field and value separated by a null byte: field\0value
     */
    public static ByteSequence parseFieldAndValueFromTF(ByteSequence cq) {
        // find 2nd null
        int frontIndex = findSecondNull(cq) + 1;

        int backIndex = findFirstNullReverse(cq);

        int fieldSize = cq.length() - (backIndex + 1);
        int valueSize = backIndex - frontIndex;
        // create and return new ByteSequence of field and value
        byte[] fieldValue = new byte[fieldSize + 1 + valueSize];
        System.arraycopy(cq.getBackingArray(), (backIndex + 1) + cq.offset(), fieldValue, 0, fieldSize);
        System.arraycopy(cq.getBackingArray(), frontIndex + cq.offset(), fieldValue, (fieldSize + 1), valueSize);
        return new ArrayByteSequence(fieldValue);
    }

    /**
     * Parses the datatype and root uid from the local Field Index key's ColumnQualifier
     * <p>
     * <br>
     * FI Key Structure (row, cf='fi\0field', cq='value\0datatype\0uid')
     * <p>
     * <br>
     * The uid starts at the ColumnQualifier's second null byte and ends at the 3rd dot (if it has children) or end of sequence (if no children).
     * <p>
     * <br>
     * We first reverse traverse until we find the second null byte and save its location.
     * <p>
     * We then start looking for dots from the location of that second null byte.
     * <p>
     * If we find 3 dots, we return everything from the location of the second null byte up to the third dot. If not then we return everything from the location
     * of the second null byte until the end.
     *
     * @param cq
     *            - a ByteSequence representing the Key's ColumnQualifier
     * @return - the parent document id
     */
    public static ByteSequence parseRootPointerFromFI(ByteSequence cq) {
        return parseRootPointerFromId(cq.subSequence(findSecondNullReverse(cq) + 1, cq.length()));
    }

    /**
     * Parses the datatype and root uid from a local Event Data key
     * <p>
     * <br>
     * Event Data Key Structure (row, cf='datatype\0uid', cq='field\0value')
     * <p>
     * <br>
     * We first traverse the ColumnFamily until we find the null byte and save its index.
     * <p>
     * We then continue to traverse from the index and if we find a third dot, we return everything up to the current index. If not, we return the unchanged
     * ColumnFamily.
     *
     * @param cf
     *            - the Event Data key's ColumnFamily
     * @return - a byte sequence that is the ColumnFamily with the root uid
     */
    public static ByteSequence parseDatatypeAndRootUidFromEvent(ByteSequence cf) {
        int count = 0;
        // if we find a third dot, return everything up to the third dot
        for (int i = 0; i < cf.length(); ++i) {
            if (cf.byteAt(i) == DOT && ++count == 3) {
                return cf.subSequence(0, i);
            }
        }
        // if less than 3 dots, return the same passed in id
        return cf;
    }

    public static int findFirstNull(ByteSequence sequence) {
        for (int i = 0; i < sequence.length(); ++i) {
            if (sequence.byteAt(i) == NULL_BYTE) {
                return i;
            }
        }
        return -1;
    }

    public static int findSecondNull(ByteSequence sequence) {
        int nullCount = 0;
        for (int i = 0; i < sequence.length(); ++i) {
            if (sequence.byteAt(i) == NULL_BYTE && ++nullCount == 2) {
                return i;
            }
        }
        return -1;
    }

    public static int findFirstNullReverse(ByteSequence sequence) {
        for (int i = sequence.length() - 1; i >= 0; --i) {
            if (sequence.byteAt(i) == NULL_BYTE) {
                return i;
            }
        }
        return -1;
    }

    public static int findSecondNullReverse(ByteSequence sequence) {
        int nullCount = 0;
        for (int i = sequence.length() - 1; i >= 0; --i) {
            if (sequence.byteAt(i) == NULL_BYTE && ++nullCount == 2) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Parse a root pointer (parent document id) from a ByteSequence
     *
     * @param id
     *            - a ByteSequence containing a dot-delimited document id.
     * @return - a ByteSequence containing the parent document id.
     */
    public static ByteSequence parseRootPointerFromId(ByteSequence id) {
        int count = 0;
        // if we find a third dot, return everything up to the third dot
        for (int i = 0; i < id.length(); ++i) {
            if (id.byteAt(i) == DOT && ++count == 3) {
                return id.subSequence(0, i);
            }
        }
        // if less than 3 dots, return the same passed in id
        return id;
    }

    /**
     * Parse the root pointer (parent document id) from a document id
     *
     * @param id
     *            - string representation of a document id.
     * @return - the parent document id
     */
    public static String parseRootPointerFromId(String id) {
        int index = 0;
        // if we find a third dot, return everything up to the third dot
        for (int i = 0; i < 3; i++) {
            index = id.indexOf(DOT, index);
            if (index == -1) {
                return id;
            }
            index += 1;
        }
        // if less than 3 dots, return the same passed in id
        return id.substring(0, index - 1);
    }

    /**
     * Extracts an estimated root pointer from the provided id.
     * <p>
     * NOTE: This method is non-deterministic. If you require certainty when parsing out the root pointer use {@link #parseRootPointerFromId(ByteSequence)}
     *
     * @param id
     *            the sequence id
     * @return an estimated root pointer
     */
    public static ByteSequence estimateRootPointerFromId(ByteSequence id) {
        if (id.length() > 21) {
            for (int i = 21; i < id.length(); ++i) {
                if (id.byteAt(i) == DOT) {
                    return id.subSequence(0, i);
                }
            }
            return id.subSequence(0, id.length());
        }
        return parseRootPointerFromFI(id);
    }

    /**
     * Method to get the root pointer from an uid, if it exists.
     *
     * @param uid
     *            a uid
     * @return the root uid, or the original uid
     */
    public static String getRootUid(String uid) {

        int dotCount = 0;
        for (int i = 0; i < uid.length(); i++) {
            if (uid.charAt(i) == DOT && ++dotCount == 3) {
                return uid.substring(0, i);
            }
        }

        return uid; // no root uid detected, return the original uid
    }

    public static ByteSequence fromString(String s) {
        return new ArrayByteSequence(s.getBytes());
    }

    public static Key buildParentKey(Text shard, ByteSequence id, ByteSequence cq, Text cv, long ts) {
        Text ptr = new Text();
        ptr.set(id.getBackingArray(), id.offset(), id.length());
        Text cqt = new Text();
        cqt.set(cq.getBackingArray(), cq.offset(), cq.length());
        return new Key(shard, ptr, cqt, cv, ts);
    }

    /**
     * In a rebuild situation build the start key for the next TLD.
     *
     * @param docKey
     *            - start key for the current range
     * @return - a key usable for a new start range.
     */
    public static Key getNextParentKey(Key docKey) {
        Text startCF = docKey.getColumnFamily();
        if (startCF.find(NULL) != -1) {
            // we have a start key with a document uid, add to the end of the cf to ensure we go to the next doc
            // parse out the uid
            String cf = startCF.toString();
            int index = cf.indexOf(NULL_BYTE);
            if (index >= 0) {
                String uid = cf.substring(index + 1);
                int index2 = uid.indexOf(NULL_BYTE);
                if (index2 >= 0) {
                    uid = uid.substring(0, index2);
                }
                // if we do not have an empty uid
                if (!uid.isEmpty()) {
                    uid = parseRootPointerFromId(uid);
                    // to get to the next doc, add the separator for the UID 'extra' (child doc) portion and then the max unicode string
                    Text nextDoc = new Text(cf.substring(0, index) + NULL_BYTE + uid + DEFAULT_SEPARATOR + MAX_UNICODE_STRING);
                    docKey = new Key(docKey.getRow(), nextDoc, docKey.getColumnQualifier(), docKey.getColumnVisibility(), docKey.getTimestamp());
                }
            }
        }
        return docKey;
    }
}
