package datawave.query.function;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;

import datawave.query.tld.TLD;

/**
 * A key equality implementation that compares to the root pointers of two doc Ids together.
 *
 * For example, two IDs `h1.h2.h3.a.b.c.d` and `h1.h2.h3.e.f` would be considered equal by this check.
 */
public class TLDEquality implements Equality {

    /**
     * Determines if two keys are equal by checking for a common root pointer.
     *
     * @param key
     *            - current key
     * @param other
     *            - candidate for equality check
     * @return - true if the provided key shares a common root pointer with the other key
     */
    @Override
    public boolean partOf(Key key, Key other) {
        ByteSequence docCF = TLD.estimateRootPointerFromId(key.getColumnFamilyData());
        ByteSequence otherCF = TLD.estimateRootPointerFromId(other.getColumnFamilyData());
        return otherCF.equals(docCF);
    }
}
