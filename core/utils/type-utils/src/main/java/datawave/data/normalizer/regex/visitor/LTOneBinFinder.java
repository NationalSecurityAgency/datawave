package datawave.data.normalizer.regex.visitor;

import org.apache.commons.lang3.tuple.Pair;

import datawave.data.normalizer.regex.AnyCharNode;
import datawave.data.normalizer.regex.Node;
import datawave.data.normalizer.regex.RegexUtils;

/**
 * Implementation of {@link BinFinder} that finds the range of exponential bins that a regex pattern should match against for numbers less than one.
 */
public class LTOneBinFinder extends BinFinder {
    
    private static final int MAX_BIN = 26;
    private static final int MIN_BIN = 1;
    private static final int INITIAL_ENDPOINT_VALUE = 0;
    
    public static Pair<Integer,Integer> binRangeOf(Node node) {
        LTOneBinFinder calculator = new LTOneBinFinder(node);
        return calculator.getBinRange();
    }
    
    protected LTOneBinFinder(Node node) {
        super(node, MIN_BIN, MAX_BIN, INITIAL_ENDPOINT_VALUE);
    }
    
    @Override
    protected Pair<Integer,Integer> getBinRange() {
        if (decimalPointIndex == -1) {
            calculateRangeWithoutDecimalPoint();
        } else {
            calculateRangeWithDecimalPoint();
        }
        normalizeRange();
        
        // When retrieving bins for numbers less than one, the bin values must be negative. Negate the endpoints.
        lower = -lower;
        upper = -upper;
        
        return getEndpoints();
    }
    
    /**
     * Calculate the bin range for a pattern that has no decimal point specified in it.
     */
    private void calculateRangeWithoutDecimalPoint() {
        // Get the index of the first wildcard in the regex, if present.
        int firstWildcardIndex = node.indexOf(AnyCharNode.class);
        
        // If there is no wildcard present in the regex, the regex does not need a bin range for numbers less than one.
        if (firstWildcardIndex == -1) {
            return;
        }
        
        // If there are any elements before the wildcard, they must all be able to possibly be a leading zero up to the wildcard. If not, the pattern will not
        // match against numbers less than one and does not need a bin range for numbers less than one.
        while (childrenIter.index() != firstWildcardIndex) {
            Node next = childrenIter.peekNext();
            // We found an element that cannot match zero before the wildcard. Return early.
            if (!RegexUtils.matchesZero(next)) {
                return;
            } else {
                // We found an element that can match zero. Move the iterator forward, and skip any quantifiers or question marks.
                childrenIter.next();
                childrenIter.seekPastQuantifiers();
                childrenIter.seekPastQuestionMarks();
            }
        }
        
        // Skip over the first wildcard, capture any quantifier if present, and skip past any question marks.
        childrenIter.next();
        Node quantifier = childrenIter.isNextQuantifier() ? childrenIter.next() : null;
        childrenIter.seekPastQuestionMarks();
        
        // If there are no elements after the wildcard, and the wildcard did not have a quantifier, there is nothing more to do.
        if (!childrenIter.hasNext() && quantifier == null) {
            return;
        }
        
        // Otherwise we will at least have the minimum bin range possible.
        incrementLower();
        incrementUpper();
        
        // If the first wildcard had a quantifier, lock the lower bound and update the upper bound based on the quantifier.
        if (quantifier != null) {
            lockLower();
            updateRangeWithQuantifier(quantifier);
        }
        
        // Process the remaining children.
        processRemainingChildren();
    }
    
    /**
     * Calculate the bin range for a pattern with a decimal point in it.
     */
    private void calculateRangeWithDecimalPoint() {
        // Seek past children that can match the character '0'. If the next child after this is not the decimal point, then the regex expression will not
        // match against numbers less than one.
        childrenIter.seekPastZeroMatchingElements();
        if (childrenIter.index() != decimalPointIndex) {
            return;
        }
        
        // Skip over the decimal point to the next character.
        childrenIter.next();
        // We will at least have the minimum bin range possible.
        incrementUpper();
        incrementLower();
        
        // Process the remaining children.
        processRemainingChildren();
    }
    
    /**
     * Iterate over the remaining children in the children iterator and update the bin range.
     */
    private void processRemainingChildren() {
        // For each possible leading zero after the decimal point, update the bin range.
        while (childrenIter.hasNext()) {
            Node next = childrenIter.next();
            // If next can be a leading zero, update the range.
            if (RegexUtils.matchesZero(next)) {
                // If next can possible be not a zero, lock the lower bound.
                if (!RegexUtils.matchesZeroOnly(next)) {
                    lockLower();
                }
                
                // If the element has a quantifier, increment the upper and lower bound based on the quantifier.
                if (childrenIter.isNextQuantifier()) {
                    updateRangeWithNextQuantifier();
                } else {
                    // Otherwise increment the upper and lower bound by one.
                    incrementLower();
                    incrementUpper();
                }
            } else {
                // If next cannot possibly be a leading zero, there is nothing more to do.
                return;
            }
        }
    }
}
