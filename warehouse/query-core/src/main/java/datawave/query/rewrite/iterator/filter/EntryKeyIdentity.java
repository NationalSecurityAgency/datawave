package datawave.query.rewrite.iterator.filter;

import java.util.Map.Entry;

import datawave.query.rewrite.iterator.aggregation.DocumentData;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

public class EntryKeyIdentity {
    public static final Predicate<Entry<DocumentData,?>> Function = Predicates.alwaysTrue();
}
