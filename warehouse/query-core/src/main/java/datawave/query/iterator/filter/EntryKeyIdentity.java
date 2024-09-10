package datawave.query.iterator.filter;

import java.util.Map.Entry;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

import datawave.query.iterator.aggregation.DocumentData;

public class EntryKeyIdentity {
    public static final Predicate<Entry<DocumentData,?>> Function = Predicates.alwaysTrue();
}
