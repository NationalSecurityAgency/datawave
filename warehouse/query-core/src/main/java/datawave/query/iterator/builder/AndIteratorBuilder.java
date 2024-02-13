package datawave.query.iterator.builder;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.jexl3.parser.JexlNode;

import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.logic.AndIterator;

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
        // we can build an intersection of all excludes on the field index
        // IFF there is a parent context out there to drive it
        // Z && (A || (!B && !C))
        return new AndIterator(includes, excludes);
    }
}
