package datawave.query.function;

import org.apache.accumulo.core.data.Key;

/**
 * A hook to compare whether one key is part of a document specified by docKey. This layer of indirection is needed when doing things like TLD and Ancestor
 * queries, where we want to compare subsections of keys.
 */
public interface Equality {
    boolean partOf(Key docKey, Key test);
}
