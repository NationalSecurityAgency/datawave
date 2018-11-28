package datawave.query.iterator.builder;

import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.logic.AndIterator;
import org.apache.commons.jexl2.parser.JexlNode;

import java.util.HashSet;
import java.util.Set;

public class AndIteratorBuilder extends AbstractIteratorBuilder {
    
    Set<JexlNode> compositePredicates = new HashSet<>();
    
    public Set<JexlNode> getCompositePredicates() {
        return compositePredicates;
    }
    
    public void addCompositePredicate(JexlNode compositePredicate) {
        this.compositePredicates.add(compositePredicate);
    }
    
    public void setCompositePredicates(Set<JexlNode> compositePredicates) {
        this.compositePredicates = compositePredicates;
    }
    
    @SuppressWarnings({"rawtypes", "unchecked"})
    public <T> NestedIterator<T> build() {
        if (includes.isEmpty()) {
            throw new IllegalStateException("AndIterator has no inclusive sources!");
        }
        return new AndIterator(includes, excludes);
    }
}
