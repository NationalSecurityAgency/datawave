package datawave.query.function;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;

/**
 * A key equality implementation that compares to the root pointers of two doc Ids together.
 * <p>
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
        ByteSequence keyCf = key.getColumnFamilyData();
        ByteSequence otherCf = other.getColumnFamilyData();

        int dotCount = 0;
        int len = Math.min(keyCf.length(), otherCf.length());
        for (int i = 0; i < len; i++) {
            byte a = keyCf.byteAt(i);
            byte b = otherCf.byteAt(i);
            if (a != b) {
                return false;
            } else if (a == '.' && ++dotCount == 3) {
                return true;
            }
        }
        return true;
    }
}
