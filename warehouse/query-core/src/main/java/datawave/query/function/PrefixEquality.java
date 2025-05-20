package datawave.query.function;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;

/**
 * A reference key equality implementation that compares keys using a PartialKey.
 *
 */
public class PrefixEquality implements Equality {
    final PartialKey prefix;

    public PrefixEquality(PartialKey prefix) {
        this.prefix = prefix;
    }

    public boolean partOf(Key docKey, Key test) {
        return docKey.equals(test, prefix);
    }
}
