package datawave.query.iterator.builder;

import java.util.LinkedList;
import java.util.List;

import datawave.query.iterator.NestedIterator;

import com.google.common.collect.HashMultimap;

/**
 * Provides semantics for adding sources to a nested iterator but deferring the creation of iterator. This is meant to be used in a visitor.
 * 
 */
@SuppressWarnings("rawtypes")
public abstract class AbstractIteratorBuilder implements IteratorBuilder {
    
    protected String field;
    
    protected String value;
    
    protected boolean inANot;
    
    protected boolean sortedUIDs;
    
    public boolean isSortedUIDs() {
        return sortedUIDs;
    }
    
    public void setSortedUIDs(boolean sortedUIDs) {
        this.sortedUIDs = sortedUIDs;
    }
    
    public boolean isInANot() {
        return inANot;
    }
    
    public void setInANot(boolean inANot) {
        this.inANot = inANot;
    }
    
    protected LinkedList<NestedIterator> includes = new LinkedList<>(), excludes = new LinkedList<>();
    
    protected HashMultimap<String,String> observedFieldValues = HashMultimap.create();
    
    protected boolean forceDocumentBuild = false;
    
    public void addInclude(NestedIterator itr) {
        includes.add(itr);
    }
    
    public void canBuildDocument(boolean forceDocumentBuild) {
        this.forceDocumentBuild = forceDocumentBuild;
    }
    
    public void addExclude(NestedIterator itr) {
        excludes.add(itr);
    }
    
    public List<NestedIterator> includes() {
        return includes;
    }
    
    public List<NestedIterator> excludes() {
        return excludes;
    }
    
    public void setValue(String value) {
        this.value = value;
    }
    
    public String getField() {
        return this.field;
    }
    
    public void setField(String field) {
        this.field = field;
    }
    
    public String getValue() {
        return this.value;
    }
    
    /**
     * Checks to see if a given {@code <field, value>} mapping has been observed before by this builder. This method is intended to only be called when a
     * visitor is building an IndexIterator.
     * 
     * This method will return <code>true</code> if the mapping has been observed before. It will return <code>false</code> if not and update the internal
     * mapping so that future calls with the same arguments return <code>true</code>.
     */
    public boolean hasSeen(String field, String value) {
        if (observedFieldValues.containsEntry(field, value)) {
            return true;
        } else {
            observedFieldValues.put(field, value);
            return false;
        }
    }
    
    public abstract <T> NestedIterator<T> build();
}
