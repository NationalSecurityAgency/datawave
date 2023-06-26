package datawave.query.iterator.filter;

import java.util.Map.Entry;

import com.google.common.base.Predicate;

import datawave.query.iterator.aggregation.DocumentData;

public class FieldIndexEntryKeyDataTypeFilter implements Predicate<Entry<DocumentData,?>> {

    protected FieldIndexKeyDataTypeFilter keyFilter;

    public FieldIndexEntryKeyDataTypeFilter(@SuppressWarnings("rawtypes") Iterable datatypes) {
        keyFilter = new FieldIndexKeyDataTypeFilter(datatypes);
    }

    @Override
    public boolean apply(Entry<DocumentData,?> input) {
        return keyFilter.apply(input.getKey().getKey());
    }

}
