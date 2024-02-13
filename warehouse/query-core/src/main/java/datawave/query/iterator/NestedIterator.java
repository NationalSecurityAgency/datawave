package datawave.query.iterator;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.IteratorEnvironment;

import datawave.query.attributes.Document;

/**
 * An interface that allows nested iterators (such as ANDs, and ORs) to return references to their leaf nodes. NestedIterators can be visualized as a tree. The
 * top level NestedIterator forms the root, and its children form the next layer all the way down to the leaf nodes. During normal evaluation all leaf nodes are
 * initialized and then candidates bubble up to the root node. When a NestedIterator requires context it cannot be independently evaluated, but requires outside
 * context for its evaluation. This is most often the case when checking for the absence of a specific value. While checking for a specific value is quick,
 * checking against all values that aren't the specific value without context would equate to a near full table scan (both things that aren't that value or just
 * don't have that field). By providing a context of a specific candidate to be tested, the lookup becomes tractable.
 * <p>
 * The call flow for an iterator that does not require context is simple
 * <ol>
 * <li>seek() - initial call to setup the iterator and any child iterators</li>
 * <li>hasNext() - to determine if a next element exists</li>
 * <li>next() - return the top element and calculate the next top element</li>
 * </ol>
 * <p>
 * The call flow for an iterator that requires context is a little different
 * <ol>
 * <li>seek() - initial call to setup the iterator and any child iterators</li>
 * <li>move(context) - moves the iterator up to the specified element, a hit may or may not exist</li>
 * <li>next() - return the top element</li>
 * </ol>
 */
public interface NestedIterator<T> extends Iterator<T> {

    /**
     * Seek the nested iterator and all child iterators. Called once at the beginning.
     * <p>
     * Future seeks happen via calls to {@link #move(Object minimum)}
     *
     * @param range
     *            the range
     * @param columnFamilies
     *            the column families
     * @param inclusive
     *            the inclusive flag
     * @throws IOException
     *             if an underlying iterator encounters a problem
     */
    void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException;

    /**
     * The simplest way to understand the move method is that child iterators are trying to match the provided minimum.
     * <p>
     * Sub methods may handle the move differently, such as includes vs. excludes vs. context includes vs. context excludes. However, the move method will
     * interpret the output of these sub methods to adhere to the matching principle.
     * <p>
     * If context is not required and a move is called but not matched, then the next-highest element is returned. This allows for efficient seeking across
     * intersections.
     * <p>
     * If context is required and a move is called but not matched, then a null element is returned.
     *
     * @param minimum
     *            the minimum
     * @return the first Key in the iterator greater than or equal to minimum or null if no Key exists
     * @throws IllegalStateException
     *             if the iterator is already at or beyond minimum
     */
    T move(T minimum);

    /**
     * Returns a reference to all of the leaf nodes at or below <code>this</code>. This is useful when we need to call <code>seek</code> on leaf nodes that are
     * <code>SortedKeyValueIterators</code>.
     *
     * @return a collection of iterators
     */
    Collection<NestedIterator<T>> leaves();

    /**
     * Returns a reference to all of the children of <code>this</code>.
     *
     * @return a collection of iterators
     */
    Collection<NestedIterator<T>> children();

    /**
     * Returns a <code>Document</code> object that is composed of attributes read in by the leaf nodes of this sub-tree.
     *
     * @return Document
     */
    Document document();

    /**
     * Provides configuration information to the Iterator before initializing and seeking. Default does nothing.
     *
     * @param env
     *            env
     */
    default void setEnvironment(IteratorEnvironment env) {}

    /**
     * Returns true if the NestedIterator requires context for evaluation or false if it can be evaluated without context
     *
     * @return boolean
     */
    boolean isContextRequired();

    /**
     * This information is required for safely dropping terms from an intersection in the AndIterator
     *
     * @return true if the field is non-event
     */
    boolean isNonEventField();

}
