package datawave.data.normalizer.regex.visitor;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;

import datawave.data.normalizer.regex.CharClassNode;
import datawave.data.normalizer.regex.CharRangeNode;
import datawave.data.normalizer.regex.EncodedPatternNode;
import datawave.data.normalizer.regex.EscapedSingleCharNode;
import datawave.data.normalizer.regex.Node;
import datawave.data.normalizer.regex.RegexConstants;
import datawave.data.normalizer.regex.RegexUtils;
import datawave.data.normalizer.regex.SingleCharNode;
import datawave.data.type.util.NumericalEncoder;

/**
 * Implementation of {@link CopyVisitor} that will return a copy of the tree where all non-simple number patterns are enriched with bin information.
 */
public class ExponentialBinAdder extends SubExpressionVisitor {
    
    /**
     * Return a copy of the given tree with all regex patterns enriched with exponential bin information.
     * 
     * @param node
     *            the node
     * @return the enriched node
     */
    public static Node addBins(Node node) {
        if (node == null) {
            return null;
        }
        ExponentialBinAdder visitor = new ExponentialBinAdder();
        return (Node) node.accept(visitor, null);
    }
    
    // Retrieves bins for negative numbers.
    private static final Function<Integer,Character> NEGATIVE_BIN_FUNCTION = NumericalEncoder::getNegativeBin;
    
    // Retrieves bins for positive numbers.
    private static final Function<Integer,Character> POSITIVE_BIN_FUNCTION = NumericalEncoder::getPositiveBin;
    
    @Override
    protected Object visitSubExpression(Node node) {
        List<Node> binNodes = new ArrayList<>();
        boolean negative = RegexUtils.isNegativeRegex(node);
        
        // The bin information consist of:
        // 1. The lead sign that indicates whether the range covers positive (\+) or negative numbers (!).
        binNodes.add(getLeadSign(negative));
        // 2. The range of exponential bin letters. This may either be a single bin letter, or a character class of multiple bin letters.
        binNodes.add(getBinRange(node, negative));
        // 3. An 'E' to separate the bin information from the beginning of the numeric regex pattern.
        binNodes.add(new SingleCharNode(RegexConstants.CAPITAL_E));
        
        // Return an EncodedPatternNode copy rather than an ExpressionNode.
        EncodedPatternNode encodedPattern = new EncodedPatternNode(copy(node).getChildren());
        
        // If we had a negative sign, remove it. We will have ! (negative) and \+ (positive) going forward.
        if (negative) {
            encodedPattern.removeFirstChild();
        }
        
        // Insert the bin information at the beginning of the pattern.
        int insertIndex = 0;
        for (Node binNode : binNodes) {
            encodedPattern.addChild(binNode, insertIndex);
            insertIndex++;
        }
        return encodedPattern;
    }
    
    /**
     * Return {@code "\+"} if negative is false, or {@code "!"} if negative is true.
     * 
     * @param negative
     *            whether the regex pattern matches against negative numbers.
     * @return the lead sign
     */
    private Node getLeadSign(boolean negative) {
        return negative ? new SingleCharNode(RegexConstants.EXCLAMATION_POINT) : new EscapedSingleCharNode(RegexConstants.PLUS);
    }
    
    /**
     * Get the range of exponential bins that the regex pattern should cover.
     * 
     * @param node
     *            the regex pattern
     * @param negative
     *            whether the pattern matches against negative numbers
     * @return the bin range, either a single bin letter or a character class of bin ranges
     */
    private Node getBinRange(Node node, boolean negative) {
        // Determine what exponential bins should be included in the encoded expression.
        // Get the bin range for numbers equal to or greater than one that the pattern can match against.
        Pair<Integer,Integer> gteOneBinRange = GTEOneBinFinder.binRangeOf(node);
        // Get the bin range for numbers less than one that the pattern can match against.
        Pair<Integer,Integer> ltOneBinRange = LTOneBinFinder.binRangeOf(node);
        
        // The target bin retrieval function depends on whether the pattern matches against negative numbers.
        Function<Integer,Character> binFunction = negative ? NEGATIVE_BIN_FUNCTION : POSITIVE_BIN_FUNCTION;
        
        if (gteOneBinRange == null) {
            // If the regex pattern cannot match against numbers equal to or greater than one, return the bin info for numbers less than one only.
            return buildBinFromSingleRange(ltOneBinRange, binFunction);
        } else if (ltOneBinRange == null) {
            // If the regex pattern cannot match against numbers less than one, return the bin info for numbers equal to or greater than one only.
            return buildBinFromSingleRange(gteOneBinRange, binFunction);
        } else {
            // Otherwise, merge the bin ranges and return them.
            CharClassNode charClass = new CharClassNode();
            Node onePlusBin = buildBinFromSingleRange(gteOneBinRange, binFunction);
            // If a single character was returned, add it to the character class. Otherwise, a character class with a range was returned. Add the range.
            charClass.addChild(onePlusBin instanceof SingleCharNode ? onePlusBin : onePlusBin.getFirstChild());
            
            Node subOneBin = buildBinFromSingleRange(ltOneBinRange, binFunction);
            // If a single character was returned, add it to the character class. Otherwise, a character class with a range was returned. Add the range.
            charClass.addChild(subOneBin instanceof SingleCharNode ? subOneBin : subOneBin.getFirstChild());
            return charClass;
        }
    }
    
    /**
     * Return a bin info node for a single bin range.
     * 
     * @param binRange
     *            the
     * @param binFunction
     *            the delegate bin retrieval function
     * @return the bin info
     */
    private Node buildBinFromSingleRange(Pair<Integer,Integer> binRange, Function<Integer,Character> binFunction) {
        if (binRange.getLeft().equals(binRange.getRight())) {
            // We have a single bin to cover in this range. Return a single char node.
            return new SingleCharNode(binFunction.apply(binRange.getLeft()));
        } else {
            // We have a range of bins to cover. Create a character class.
            CharClassNode charClass = new CharClassNode();
            char left = binFunction.apply(binRange.getLeft());
            char right = binFunction.apply(binRange.getRight());
            int compare = Character.compare(left, right);
            // It's possible for the left sided-bin to be alphabetically higher than the right side. If so, flip them around in the character class range.
            if (compare < 0) {
                charClass.addChild(new CharRangeNode(left, right));
            } else {
                charClass.addChild(new CharRangeNode(right, left));
            }
            return charClass;
        }
    }
    
}
