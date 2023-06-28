package datawave.query.iterator.filter;

import java.util.Map.Entry;

import com.google.common.base.Predicate;

import datawave.query.iterator.aggregation.DocumentData;

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
