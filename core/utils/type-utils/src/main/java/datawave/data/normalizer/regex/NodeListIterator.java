package datawave.data.normalizer.regex;

import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * An iterator for traversing over a list of {@link Node} instances, with functionality for skipping over nodes that meet certain conditions.
 */
public class NodeListIterator {
    
    /**
     * The list.
     */
    private final List<Node> nodes;
    
    /**
     * The current index.
     */
    private int index;
    
    public NodeListIterator(List<Node> nodes) {
        this.nodes = nodes;
    }
    
    /**
     * Return the current iterator index.
     * 
     * @return the index
     */
    public int index() {
        return index;
    }
    
    /**
     * Set the current index for the iterator.
     * 
     * @param index
     *            the index
     */
    public void setIndex(int index) {
        this.index = index;
    }
    
    /**
     * Return true if there are more nodes to return from the list.
     * 
     * @return true if there are a next node to return
     */
    public boolean hasNext() {
        return this.index < this.nodes.size();
    }
    
    /**
     * Return the next node from the list.
     * 
     * @return the next node
     * @throws NoSuchElementException
     *             if there is no next node
     */
    public Node next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return nodes.get(index++);
    }
    
    /**
     * Return the next node from the list without modifying the current iterator index.
     * 
     * @return the next node
     * @throws NoSuchElementException
     *             if there is no next node
     */
    public Node peekNext() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return nodes.get((index));
    }
    
    /**
     * Return whether the next node is an instance of the given type.
     * 
     * @param type
     *            the type
     * @return true if the next node is an instance of the type, or false otherwise
     * @throws NoSuchElementException
     *             if there is no next node
     */
    public boolean isNextInstanceOf(Class<? extends Node> type) {
        return type.isInstance(peekNext());
    }
    
    /**
     * Return whether the next node is an instance of one of the given types.
     * 
     * @param types
     *            the types
     * @return true if the next node is an instance of one of the given types, or false otherwise
     * @throws NoSuchElementException
     *             if there is no next node
     */
    public boolean isNextInstanceOfAny(Collection<Class<? extends Node>> types) {
        Node previous = peekNext();
        return types.stream().anyMatch((type) -> type.isInstance(previous));
    }
    
    /**
     * Update the iterator so that the next call to {@link #next()} will return the first node is not a regex element that can match against the character '0',
     * starting from the iterator's current position in the list. If no such node is found, the iterator will be moved to the end of the list,
     * {@link #hasNext()} will return false and any call to {@link #next()} will result in a {@link NoSuchElementException}.
     */
    public void seekPastZeroMatchingElements() {
        while (hasNext()) {
            // Peek at the next node.
            Node next = peekNext();
            // We have a leading zero. Skip it.
            if (RegexUtils.matchesZero(next)) {
                // Explicitly call next so that we increment the iterator index.
                next();
                // Seek past a succeeding quantifier and question mark if present.
                seekPastQuantifiers();
                seekPastQuestionMarks();
            } else {
                return;
            }
        }
    }
    
    /**
     * Update the iterator so that the next call to {@link #next()} will return the first node is not a regex element that can match only the character '0',
     * starting from the iterator's current position in the list. If no such node is found, the iterator will be moved to the end of the list,
     * {@link #hasNext()} will return false and any call to {@link #next()} will result in a {@link NoSuchElementException}.
     */
    public void seekPastZeroOnlyElements() {
        while (hasNext()) {
            // Peek at the next node.
            Node next = peekNext();
            // We have a leading zero. Skip it.
            if (RegexUtils.matchesZeroOnly(next)) {
                // Explicitly call next so that we increment the iterator index.
                next();
                // Seek past a succeeding quantifier and question mark if present.
                seekPastQuantifiers();
                seekPastQuestionMarks();
            } else {
                return;
            }
        }
    }
    
    /**
     * Update the iterator so that the next call to {@link #next()} will return the first node that is not a {@link ZeroOrMoreNode}, {@link OneOrMoreNode}, or
     * {@link RepetitionNode}, starting from the iterator's current position in the list. If no such node is found, the iterator will be moved to the end of the
     * list, {@link #hasNext()} will return false and any call to {@link #next()} will result in a {@link NoSuchElementException}.
     */
    public void seekPastQuantifiers() {
        while (isNextQuantifier()) {
            next();
        }
    }
    
    /**
     * Update the iterator so that the next call to {@link #next()} will return the first node that is not an {@link QuestionMarkNode}, starting from the
     * iterator's current position in the list. If no such node is found, the iterator will be moved to the end of the list, {@link #hasNext()} will return
     * false and any call to {@link #next()} will result in a {@link NoSuchElementException}.
     */
    public void seekPastQuestionMarks() {
        while (isNextQuestionMark()) {
            next();
        }
    }
    
    /**
     * Return whether the next node in the list is a {@link ZeroOrMoreNode}, {@link OneOrMoreNode}, or a {@link RepetitionNode}.
     * 
     * @return true if the next node in the list is a quantifier type, or false otherwise
     */
    public boolean isNextQuantifier() {
        return hasNext() && isNextInstanceOfAny(RegexConstants.QUANTIFIER_TYPES);
    }
    
    /**
     * Return whether the next node in the list is a {@link QuestionMarkNode}.
     * 
     * @return true if the next node in the list is an {@link QuestionMarkNode}, or false otherwise
     */
    public boolean isNextQuestionMark() {
        return hasNext() && isNextInstanceOf(QuestionMarkNode.class);
    }
}
