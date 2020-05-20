package datawave.query.iterator;

import java.util.Collection;
import java.util.Iterator;

import datawave.query.attributes.Document;

/**
 * An interface that allows nested iterators (such as ANDs, and ORs) to return references to their leaf nodes. NestedIterators can be visualized as a tree. The
 * top level NestedIterator forms the root, and its children form the next layer all the way down to the leaf nodes. During normal evaluation all leaf nodes are
 * initialized and then candidates bubble up to the root node. When a NestedIterator requires context its means that it cannot be independently, but requires
 * outside context for its evaluation. This is most often the case when checking for the absence of a specific value. While checking for a specific value is
 * quick, checking against all values that aren't the specific value without context would equate to a near full table scan (both things that aren't that value
 * or just don't have that field). By providing a context of a specific candidate to be tested, the lookup becomes tractable.
 */
public interface NestedIterator<T> extends Iterator<T> {
    /**
     * A hook to allow lazy initialization of iterators. This is necessary because we want to set up the Accumulo iterator tree inside of init(), but can't
     * actually organize the iterators by value until after seek() is called.
     */
    void initialize();
    
    /**
     * Tells the underlying iterator to return the first element that is greater than or equal to <code>minimum</code>.
     * 
     * @param minimum
     * @return the first Key in the iterator greater than or equal to minimum or null if no Key exists
     * @throws IllegalStateException
     *             if the iterator is already at or beyond minimum
     */
    T move(T minimum);
    
    /**
     * Returns a reference to all of the leaf nodes at or below <code>this</code>. This is useful when we need to call <code>seek</code> on leaf nodes that are
     * <code>SortedKeyValueIterators</code>.
     * 
     * @return
     */
    Collection<NestedIterator<T>> leaves();
    
    /**
     * Returns a reference to all of the children of <code>this</code>.
     * 
     * @return
     */
    Collection<NestedIterator<T>> children();
    
    /**
     * Returns a <code>Document</code> object that is composed of attributes read in by the leaf nodes of this sub-tree.
     */
    Document document();
    
    /**
     * Returns true if the NestedIterator requires context for evaluation or false if it can be evaluated without context
     * 
     * @return
     */
    boolean isContextRequired();
    
    /**
     * Sets the context for evaluation
     * 
     * @param context
     *            non-null context for evaluation
     */
    void setContext(T context);
}
