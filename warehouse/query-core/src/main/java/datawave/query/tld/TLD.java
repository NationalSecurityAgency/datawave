package datawave.query.tld;

import com.google.common.collect.Lists;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;

import static datawave.data.hash.UIDConstants.DEFAULT_SEPARATOR;
import static datawave.query.Constants.MAX_UNICODE_STRING;
import static datawave.query.Constants.NULL;

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
    
    private TLD() {}
    
    /**
     * Parses the pointer (document id) from the local Field Index key's ColumnQualifier
     *
     * FI Key Structure (row, cf='fi\0field', cq='value\0datatype\0uid')
     *
     * The uid is starts at the ColumnQualifier's second null byte, ends at the end of sequence.
     *
     * @param cq
     *            - a ByteSequence representing the Key's ColumnQualifier
     * @return - the document id
     */
    public static ByteSequence parsePointerFromFI(ByteSequence cq) {
        ArrayList<Integer> nulls = lastInstancesOf(0, cq, 2);
        final int start = nulls.get(1) + 1, stop = cq.length();
        return cq.subSequence(start, stop);
    }
    
    /**
     * Parse the parent pointer from a document id.
     *
     * For example
     * <ul>
     * <li>parses 'parent.doc.id' from 'parent.doc.id'</li>
     * <li>parses 'parent.doc.id' from 'parent.doc.id.child'</li>
     * <li>parses 'parent.doc.id.child' from 'parent.doc.id.child.grandchild'</li>
     * </ul>
     *
     * @param id
     *            - a ByteSequence containing the document id
     * @return - the parent id
     */
    public static ByteSequence parseParentPointerFromId(ByteSequence id) {
        ArrayList<Integer> dots = instancesOf('.', id);
        int stop;
        if (dots.size() > 2) {
            stop = dots.get(Math.max(2, dots.size() - 1));
        } else {
            stop = id.length();
        }
        return id.subSequence(0, stop);
    }
    
    /**
     * Parses the FIELD and VALUE from a local Field Index
     *
     * FI Key Structure (row, cf='fi\0field', cq='value\0datatype\0uid')
     * 
     * @param cf
     *            - the field index key's ColumnFamily
     * @param cq
     *            - the field index key's ColumnQualifier
     * @return - a byte sequence that is the field and value separated by a null byte
     */
    public static ByteSequence parseFieldAndValueFromFI(ByteSequence cf, ByteSequence cq) {
        ArrayList<Integer> nulls = instancesOf(0, cf, 1);
        final int startFn = nulls.get(0) + 1, stopFn = cf.length();
        
        nulls = lastInstancesOf(0, cq, 2);
        final int startFv = 0, stopFv = nulls.get(1);
        
        byte[] fnFv = new byte[stopFn - startFn + 1 + stopFv - startFv];
        
        System.arraycopy(cf.getBackingArray(), startFn + cf.offset(), fnFv, 0, stopFn - startFn);
        System.arraycopy(cq.getBackingArray(), startFv + cq.offset(), fnFv, stopFn - startFn + 1, stopFv - startFv);
        return new ArrayByteSequence(fnFv);
    }
    
    /**
     * Parses the FIELD and VALUE from a Term Frequency key.
     *
     * @param cq
     *            - the term frequency key's ColumnQualifier
     * @return - a byte sequence that is the field and value separated by a null byte
     */
    public static ByteSequence parseFieldAndValueFromTF(ByteSequence cq) {
        ArrayList<Integer> nulls = instancesOf(0, cq, 2);
        ArrayList<Integer> otherNulls = lastInstancesOf(0, cq, 1);
        final int startFn = otherNulls.get(0) + 1, stopFn = cq.length();
        final int startFv = nulls.get(1) + 1, stopFv = otherNulls.get(0);
        
        byte[] fnFv = new byte[stopFn - startFn + 1 + stopFv - startFv];
        
        System.arraycopy(cq.getBackingArray(), startFn + cq.offset(), fnFv, 0, stopFn - startFn);
        System.arraycopy(cq.getBackingArray(), startFv + cq.offset(), fnFv, stopFn - startFn + 1, stopFv - startFv);
        return new ArrayByteSequence(fnFv);
    }
    
    /**
     * Parses the root pointer (parent document id) from the local Field Index key's ColumnQualifier
     *
     * FI Key Structure (row, cf='fi\0field', cq='value\0datatype\0uid')
     *
     * The uid is starts at the ColumnQualifier's second null byte, ends at the 3rd dot or end of sequence.
     *
     * @param cq
     *            - a ByteSequence representing the Key's ColumnQualifier
     * @return - the parent document id
     */
    public static ByteSequence parseRootPointerFromFI(ByteSequence cq) {
        ArrayList<Integer> nulls = lastInstancesOf(0, cq, 2);
        final int start = nulls.get(1) + 1, stop = cq.length();
        return parseRootPointerFromId(cq.subSequence(start, stop));
    }
    
    /**
     * Parse a root pointer (parent document id) from a ByteSequence
     * 
     * @param id
     *            - a ByteSequence containing a dot-delimited document id.
     * @return - a ByteSequence containing the parent document id.
     */
    public static ByteSequence parseRootPointerFromId(ByteSequence id) {
        ArrayList<Integer> dots = instancesOf('.', id);
        int stop;
        if (dots.size() > 2) {
            stop = dots.get(2);
        } else {
            stop = id.length();
        }
        return id.subSequence(0, stop);
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
        for (int i = 0; i < 3; i++) {
            index = id.indexOf('.', index);
            if (index == -1) {
                return id;
            }
            index += 1;
        }
        return id.substring(0, index - 1);
    }
    
    /**
     * Extracts an estimated root pointer from the provided id.
     *
     * NOTE: This method is non-deterministic. If you require certainty when parsing out the root pointer use {@link #parseRootPointerFromId(ByteSequence)}
     *
     * @param id
     *            the sequence id
     * @return an estimated root pointer
     */
    public static ByteSequence estimateRootPointerFromId(ByteSequence id) {
        if (id.length() > 21) {
            for (int i = 21; i < id.length(); ++i) {
                if (id.byteAt(i) == '.') {
                    return id.subSequence(0, i);
                }
            }
            return id.subSequence(0, id.length());
        }
        return parseParentPointerFromId(id);
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
            if (uid.charAt(i) == '.' && ++dotCount == 3) {
                return uid.substring(0, i);
            }
        }
        
        return uid; // no root uid detected, return the original uid
    }
    
    /**
     * Determines if the provided ByteSequence contains an id with a root pointer (parent document id)
     *
     * @param id
     *            - a ByteSequence containing an id
     * @return - true if the provided id is a parent document id
     */
    public static boolean isRootPointer(ByteSequence id) {
        ArrayList<Integer> dots = instancesOf('.', id);
        return dots.size() <= 2;
    }
    
    public static ByteSequence fromString(String s) {
        return new ArrayByteSequence(s.getBytes());
    }
    
    public static ArrayList<Integer> instancesOf(int b, ByteSequence sequence) {
        return instancesOf(b, sequence, -1);
    }
    
    public static ArrayList<Integer> instancesOf(int b, ByteSequence sequence, int repeat) {
        ArrayList<Integer> positions = Lists.newArrayList();
        for (int i = 0; i < sequence.length() && positions.size() != repeat; ++i) {
            if (sequence.byteAt(i) == b) {
                positions.add(i);
            }
        }
        return positions;
    }
    
    public static ArrayList<Integer> lastInstancesOf(int b, ByteSequence sequence, int repeat) {
        ArrayList<Integer> positions = Lists.newArrayList();
        for (int i = sequence.length() - 1; i >= 0 && positions.size() != repeat; --i) {
            if (sequence.byteAt(i) == b) {
                positions.add(i);
            }
        }
        return positions;
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
            int index = cf.indexOf('\0');
            if (index >= 0) {
                String uid = cf.substring(index + 1);
                int index2 = uid.indexOf('\0');
                if (index2 >= 0) {
                    uid = uid.substring(0, index2);
                }
                // if we do not have an empty uid
                if (!uid.isEmpty()) {
                    uid = TLD.parseRootPointerFromId(uid);
                    // to get to the next doc, add the separator for the UID 'extra' (child doc) portion and then the max unicode string
                    Text nextDoc = new Text(cf.substring(0, index) + NULL + uid + DEFAULT_SEPARATOR + MAX_UNICODE_STRING);
                    docKey = new Key(docKey.getRow(), nextDoc, docKey.getColumnQualifier(), docKey.getColumnVisibility(), docKey.getTimestamp());
                }
            }
        }
        return docKey;
    }
}
