package datawave.data.normalizer.regex.visitor;

import org.apache.commons.lang3.tuple.Pair;

import datawave.data.normalizer.regex.IntegerNode;
import datawave.data.normalizer.regex.IntegerRangeNode;
import datawave.data.normalizer.regex.Node;
import datawave.data.normalizer.regex.NodeListIterator;
import datawave.data.normalizer.regex.RegexConstants;
import datawave.data.normalizer.regex.RegexUtils;

/**
 * Abstract class for {@link LTOneBinFinder} and {@link GTEOneBinFinder} with common properties and functionality.
 */
abstract class BinFinder {
    
    // The original node.
    protected final Node node;
    
    // An iterator for the node's children.
    protected final NodeListIterator childrenIter;
    
    // The index of the decimal point in the node's children, possibly -1.
    protected final int decimalPointIndex;
    
    // The smallest bin value.
    protected final int minBin;
    
    // The highest bin value.
    protected final int maxBin;
    
    // The initial value for the lower and upper endpoints.
    protected final int initialEndpointValue;
    
    // The current lower end of the bin range.
    protected int lower;
    
    // The current upper end of the bin range.
    protected int upper;
    
    protected boolean lowerLocked;
    
    protected BinFinder(Node node, int minBin, int maxBin, int initialEndpointValue) {
        this.node = node;
        this.decimalPointIndex = RegexUtils.getDecimalPointIndex(node);
        this.minBin = minBin;
        this.maxBin = maxBin;
        this.initialEndpointValue = initialEndpointValue;
        this.childrenIter = node.getChildrenIterator();
        
        // Set the initial end point values.
        this.lower = initialEndpointValue;
        this.upper = initialEndpointValue;
        
        // If the first child is a hyphen, skip over it and start at the next child.
        if (RegexUtils.isChar(node.getFirstChild(), RegexConstants.HYPHEN)) {
            childrenIter.next();
        }
    }
    
    protected abstract Pair<Integer,Integer> getBinRange();
    
    /**
     * Increment lower by one.
     */
    protected void incrementLower() {
        if (!lowerLocked) {
            lower++;
        }
    }
    
    /**
     * Increment lower by the given value.
     * 
     * @param value
     *            the value
     */
    protected void incrementLower(int value) {
        if (!lowerLocked) {
            lower += value;
        }
    }
    
    /**
     * Lock modifications to the lower bound. Any subsequent calls to {@link #incrementLower()} or {@link #incrementLower(int)} will not modify the lower bound.
     */
    protected void lockLower() {
        this.lowerLocked = true;
    }
    
    /**
     * Unlock modifications to the lower bound. Any subsequent calls to {@link #incrementLower()} or {@link #incrementLower(int)} will modify the lower bound.
     */
    protected void unlockLower() {
        this.lowerLocked = false;
    }
    
    /**
     * Set lower to the initial endpoint value.
     */
    protected void setLowerToInitialEndpointValue() {
        this.lower = initialEndpointValue;
    }
    
    /**
     * Increment upper by one.
     */
    protected void incrementUpper() {
        upper++;
    }
    
    /**
     * Increment upper by the given value.
     *
     * @param value
     *            the value
     */
    protected void incrementUpper(int value) {
        upper += value;
    }
    
    /**
     * Set upper to the max bin value.
     */
    protected void setUpperToMax() {
        upper = maxBin;
    }
    
    /**
     * Normalize the endpoints to be within the min and max bin if they were updated.
     */
    protected void normalizeRange() {
        // Do not normalize if both the upper and lower are the initial endpoint value. This indicates that a valid bin range was not found.
        if (lower != initialEndpointValue || upper != initialEndpointValue) {
            // Normalize the bin range to be within a valid bin range. If the lower bound is less than the min bin, set it to the min bin. If it is greater than
            // the max bin, set it to the max bin.
            if (lower < minBin) {
                lower = minBin;
            } else if (lower > maxBin) {
                lower = maxBin;
            }
            
            // If the upper bound is greater than the max bin, set it to the max bin.
            if (upper > maxBin) {
                upper = maxBin;
            }
        }
    }
    
    /**
     * Return a {@link Pair} with the lower and upper bin range endpoints, or null if no valid bin range was found.
     *
     * @return the bin range
     */
    protected Pair<Integer,Integer> getEndpoints() {
        if (lower != initialEndpointValue || upper != initialEndpointValue) {
            return Pair.of(lower, upper);
        } else {
            return null;
        }
    }
    
    /**
     * Update lower and upper based on the quantities read from the next quantifier in the iterator.
     */
    protected void updateRangeWithNextQuantifier() {
        // Update the range.
        updateRangeWithQuantifier(childrenIter.next());
        // If the node after the quantifier node is an question mark, skip over it.
        childrenIter.seekPastQuestionMarks();
    }
    
    /**
     * Update lower and upper based off the quantities read from the next quantifier.
     */
    protected void updateRangeWithQuantifier(Node quantifier) {
        switch (quantifier.getType()) {
            case REPETITION:
                // In the case of a repetition node, we may have an IntegerNode or IntegerRangeNode child.
                Node child = quantifier.getFirstChild();
                if (child instanceof IntegerNode) {
                    // Increment both the upper and lower bound by the repetition value.
                    int value = ((IntegerNode) child).getValue();
                    incrementLower(value);
                    incrementUpper(value);
                } else {
                    IntegerRangeNode rangeNode = (IntegerRangeNode) child;
                    // Increment the lower bound by the range start value.
                    incrementLower(rangeNode.getStart());
                    // If the end of the range has a bound, increment the upper bound by the end bound. Otherwise, set the upper bound to the max.
                    if (rangeNode.isEndBounded()) {
                        incrementUpper(rangeNode.getEnd());
                    } else {
                        setUpperToMax();
                    }
                }
                break;
            case ZERO_OR_MORE:
                // Set the upper to the max. Do not modify the lower bound.
                setUpperToMax();
                break;
            case ONE_OR_MORE:
                // Set the upper bound to the max.
                setUpperToMax();
                // Increment the lower bound by one.
                incrementLower();
                break;
        }
    }
}
