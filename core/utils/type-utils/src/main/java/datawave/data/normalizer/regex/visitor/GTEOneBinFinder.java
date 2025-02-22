package datawave.data.normalizer.regex.visitor;

import org.apache.commons.lang3.tuple.Pair;

import datawave.data.normalizer.regex.Node;
import datawave.data.normalizer.regex.NodeType;
import datawave.data.normalizer.regex.RegexUtils;

/**
 * Implementation of {@link BinFinder} that finds the range of exponential bins that a regex pattern should match against for numbers equal to or greater than
 * one.
 */
public class GTEOneBinFinder extends BinFinder {
    
    private static final int MIN_BIN = 0;
    private static final int MAX_BIN = 25;
    private static final int INITIAL_ENDPOINT_VALUE = -1;
    
    public static Pair<Integer,Integer> binRangeOf(Node node) {
        GTEOneBinFinder calculator = new GTEOneBinFinder(node);
        return calculator.getBinRange();
    }
    
    protected GTEOneBinFinder(Node node) {
        super(node, MIN_BIN, MAX_BIN, INITIAL_ENDPOINT_VALUE);
    }
    
    @Override
    protected Pair<Integer,Integer> getBinRange() {
        calculateRange();
        normalizeRange();
        return getEndpoints();
    }
    
    /**
     * Calculate the bin range.
     */
    private void calculateRange() {
        // Skip any leading zero elements that only match a zero character.
        childrenIter.seekPastZeroOnlyElements();
        
        // If a decimal point is present, and we have reached it after skipping zero-only elements, there's nothing further to do.
        if (childrenIter.index() == decimalPointIndex) {
            return;
        }
        
        boolean lockedAtWildcard = false;
        boolean nonLeadingZeroSeen = false;
        
        // Iterate through the remaining children up to the decimal point (if present).
        while (childrenIter.hasNext() && !(childrenIter.index() == decimalPointIndex)) {
            Node next = childrenIter.next();
            if (lockedAtWildcard) {
                // If we have previously locked the lower bound at a wildcard, we do not need to make further evaluations on the current element. Update the
                // bin range with it.
                updateBinRange();
            } else if (nonLeadingZeroSeen) {
                // If the current node is a wildcard, and an explicit decimal point is not present in the regex, lock the lower bound. This will ensure we match
                // against numbers that had a decimal point that would match against this wildcard.
                if (decimalPointIndex == -1 && next.getType() == NodeType.ANY_CHAR) {
                    lockLower();
                    lockedAtWildcard = true;
                }
                // If any non-leading zero elements were seen, update the bin range with the current element. We must still check for a wildcard.
                updateBinRange();
            } else if (RegexUtils.matchesZeroOnly(next)) {
                // The current element matches zero only, e.g. '0' or [0], and is part of a leading zero. Update the bin range with the current element.
                updateBinRange();
            } else if (RegexUtils.matchesZero(next)) {
                // The current element can match zero and at least one other number. Reset the lower bound, and seek ahead to determine if we should lock the
                // lower bound.
                setLowerToInitialEndpointValue();
                // If this leading zero is the last element that can match against any other number until the end of the regex, or until the decimal point, we
                // must lock the lower bound here.
                if (isRemainingZeroOnlyUntilEndOrDecimalPoint()) {
                    // The current element must occur at least once, so increment lower by one before locking it.
                    incrementLower();
                    
                    // We want to update the bin range without modifying the lower bound, so lock the lower bound, update the bin range, and then unlock the
                    // lower bound. The lower bound must be unlocked afterwards to allow for any subsequent zero-only characters to be counted if seen.
                    lockLower();
                    updateBinRange();
                    unlockLower();
                } else {
                    // Update the bin range.
                    updateBinRange();
                }
            } else {
                // We've seen our first non-leading zero. Mark it so.
                nonLeadingZeroSeen = true;
                // Reset the lower bound before updating the bin range. Any elements we saw before this were leading zeros that can be disregarded.
                setLowerToInitialEndpointValue();
                updateBinRange();
            }
        }
    }
    
    /**
     * Return whether, if skipping all elements that can only match zero, there are no more elements or the next element is a decimal point.
     * 
     * @return true if the remaining regex pattern will match zero either until the end or a decimal point, or false otherwise
     */
    private boolean isRemainingZeroOnlyUntilEndOrDecimalPoint() {
        // Make a note of the iterator's current index so that we can reset it later.
        int originalIndex = childrenIter.index();
        
        // Skip past any quantifiers or question marks the current element may have had.
        childrenIter.seekPastQuantifiers();
        childrenIter.seekPastQuestionMarks();
        
        // Find the next node that does not only match the character '0'.
        Node nextNonZeroOnlyNode = null;
        while (childrenIter.hasNext()) {
            Node next = childrenIter.next();
            
            // If the current element does not match zero only, we've found our target node. Stop looping.
            if (!RegexUtils.matchesZeroOnly(next)) {
                nextNonZeroOnlyNode = next;
                break;
            }
            childrenIter.seekPastQuantifiers();
            childrenIter.seekPastQuestionMarks();
        }
        // Reset the iterator to the original index.
        childrenIter.setIndex(originalIndex);
        
        return nextNonZeroOnlyNode == null || RegexUtils.isDecimalPoint(nextNonZeroOnlyNode);
    }
    
    /**
     * Update the bin range with the current element, taking into account any specified quantifiers.
     */
    private void updateBinRange() {
        if (childrenIter.hasNext() && childrenIter.isNextQuantifier()) {
            // If a quantifier was specified, increment the upper and lower bound based on the quantifier type.
            updateRangeWithNextQuantifier();
        } else {
            // If no quantifier was specified, increment the upper and lower bound by one.
            incrementUpper();
            incrementLower();
        }
    }
}
