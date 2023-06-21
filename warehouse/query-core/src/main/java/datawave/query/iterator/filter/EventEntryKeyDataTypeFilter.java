package datawave.query.iterator.filter;

import java.util.Map.Entry;

import datawave.query.iterator.aggregation.DocumentData;

import com.google.common.base.Predicate;

public class EventEntryKeyDataTypeFilter implements Predicate<Entry<DocumentData,?>> {

    protected EventKeyDataTypeFilter keyFilter;

    public EventEntryKeyDataTypeFilter(@SuppressWarnings("rawtypes") Iterable datatypes) {
        keyFilter = new EventKeyDataTypeFilter(datatypes);
    }

    @Override
    public boolean apply(Entry<DocumentData,?> input) {
        return keyFilter.apply(input.getKey().getKey());
    }

}
