package datawave.query.iterator.filter.composite;

import org.apache.commons.jexl2.parser.JexlNode;

import java.util.Set;

/**
 * This interface is intended to be used with the index iterators in order to determine how the composite predicates found in the query should be handled.
 * Generally, these composite predicates are passed down to the lowest level iterator to be used by the CompositePredicateFilter.
 */
public interface CompositePredicateFilterer {
    void addCompositePredicates(Set<JexlNode> compositePredicates);
}
