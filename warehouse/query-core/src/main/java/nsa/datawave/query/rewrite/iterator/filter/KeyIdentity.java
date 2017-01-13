package nsa.datawave.query.rewrite.iterator.filter;

import org.apache.accumulo.core.data.Key;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

public class KeyIdentity {
    public static final Predicate<Key> Function = Predicates.alwaysTrue();
}
