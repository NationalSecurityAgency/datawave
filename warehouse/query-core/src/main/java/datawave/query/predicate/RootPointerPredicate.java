package datawave.query.predicate;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;

import com.google.common.base.Predicate;

import datawave.query.Constants;

/**
 * This class will test a key and determine if references the root pointer (top level document (TLD)) or not. The "SansContext" mechanism will try to
 * efficiently determine this without knowing the actual root pointer and hence may get it wrong if the UID is too smaller or bigger than expected. This will
 * only work with the HashUID mechanism. The "WithContext" mechanism however in which the root document is preconfigured is much more reliable and even more
 * efficient. To use the "WithContext" mechanism one merely needs to provide the root document pointer via the startNewDocument(Key documentKey) method.
 */
public class RootPointerPredicate implements Predicate<Key> {

    public static final byte[] FI_CF = Constants.FI_PREFIX.getBytes();
    public static final byte[] TF_CF = Constants.TERM_FREQUENCY_COLUMN_FAMILY.getBytes();

    private Text dtAndUid = null;

    public void startNewDocument(Key documentKey) {
        dtAndUid = documentKey.getColumnFamily();
    }

    @Override
    public boolean apply(Key input) {
        if (dtAndUid == null) {
            return isRootPointerSansContext(input);
        } else {
            return isRootPointerWithContext(input);
        }
    }

    /**
     * Determine if a key is the for the document key denoted with startNewDocument or a child there of. Note this is very efficient and assumes that the key is
     * in the same document tree as the document key denoted with startNewDocument.
     *
     * @param k
     *            The shard table key in question
     * @return true if for the root document
     */
    private boolean isRootPointerWithContext(Key k) {
        ByteSequence cf = k.getColumnFamilyData();

        if (WritableComparator.compareBytes(cf.getBackingArray(), 0, 2, FI_CF, 0, 2) == 0) {
            ByteSequence seq = k.getColumnQualifierData();

            if (seq.length() <= dtAndUid.getLength()) {
                // something does not compute here... assume not the root pointer we are looking for
                return false;
            }

            // if the character before the dt/uid is a null byte, then this is our root pointer
            if (seq.byteAt(seq.length() - dtAndUid.getLength() - 1) == 0x00) {
                return true;
            } else {
                return false;
            }
        } else if (WritableComparator.compareBytes(cf.getBackingArray(), 0, 2, TF_CF, 0, 2) == 0) {
            ByteSequence seq = k.getColumnQualifierData();

            if (seq.length() <= dtAndUid.getLength()) {
                // something does not compute here... assume not the root pointer we are looking for
                return false;
            }

            if (seq.byteAt(dtAndUid.getLength()) == 0x00) {
                return true;
            } else {
                return false;
            }
        } else {
            return cf.length() == dtAndUid.getLength();
        }
    }

    /**
     * Determine is a key contains a UID for the root (tld) document. This is very efficient but makes assumptions about the length if a UID which for almost
     * 100% of the time is correct.
     *
     * @param k
     *            The shard table key in question
     * @return true if for the root document
     */
    private boolean isRootPointerSansContext(Key k) {
        ByteSequence cf = k.getColumnFamilyData();

        if (WritableComparator.compareBytes(cf.getBackingArray(), 0, 2, FI_CF, 0, 2) == 0) {
            ByteSequence seq = k.getColumnQualifierData();
            int i = seq.length() - 19;
            for (; i >= 0; i--) {

                if (seq.byteAt(i) == '.') {
                    return false;
                } else if (seq.byteAt(i) == 0x00) {
                    break;
                }
            }

            for (i += 20; i < seq.length(); i++) {
                if (seq.byteAt(i) == '.') {
                    return false;
                }
            }
            return true;

        } else if (WritableComparator.compareBytes(cf.getBackingArray(), 0, 2, TF_CF, 0, 2) == 0) {
            ByteSequence seq = k.getColumnQualifierData();

            // work front to back, just in case the TF value includes a null byte
            boolean foundStart = false;
            int dotCount = 0;
            for (int i = 0; i < seq.length(); i++) {
                if (!foundStart && seq.byteAt(i) == 0x00) {
                    foundStart = true;
                } else if (foundStart && seq.byteAt(i) == 0x00) {
                    // end of uid, got here, is root
                    return true;
                } else if (foundStart && seq.byteAt(i) == '.') {
                    dotCount++;
                    if (dotCount > 2) {
                        return false;
                    }
                }
            }

            // can't parse
            return false;
        } else {
            int i = 0;
            for (i = 0; i < cf.length(); i++) {

                if (cf.byteAt(i) == 0x00) {
                    break;
                }
            }

            for (i += 20; i < cf.length(); i++) {

                if (cf.byteAt(i) == '.') {
                    return false;
                } else if (cf.byteAt(i) == 0x00) {
                    return true;
                }
            }
            return true;
        }

    }

}
