package datawave.data.normalizer.regex.visitor;

import static datawave.data.normalizer.regex.RegexUtils.toChar;
import static datawave.data.normalizer.regex.RegexUtils.toInt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import datawave.data.normalizer.regex.AlternationNode;
import datawave.data.normalizer.regex.CharClassNode;
import datawave.data.normalizer.regex.CharRangeNode;
import datawave.data.normalizer.regex.EncodedPatternNode;
import datawave.data.normalizer.regex.ExpressionNode;
import datawave.data.normalizer.regex.GroupNode;
import datawave.data.normalizer.regex.Node;
import datawave.data.normalizer.regex.NodeListIterator;
import datawave.data.normalizer.regex.NodeType;
import datawave.data.normalizer.regex.OneOrMoreNode;
import datawave.data.normalizer.regex.RegexConstants;
import datawave.data.normalizer.regex.RegexUtils;
import datawave.data.normalizer.regex.RepetitionNode;
import datawave.data.normalizer.regex.SingleCharNode;
import datawave.data.normalizer.regex.ZeroOrMoreNode;

/**
 * Implementation of {@link CopyVisitor} that will return a copy of a regex tree with all patterns that are meant to match negative numbers inverted such that
 * they will match against negative numbers that were encoded by {@link datawave.data.type.util.NumericalEncoder}. The numerical encoder encodes negative
 * numbers such that the mantissa equals ten minus the mantissa of scientific notation.
 *
 * @see datawave.data.type.util.NumericalEncoder
 */
public class NegativeNumberPatternInverter extends CopyVisitor {
    
    private static final int TEN = 10;
    private static final int NINE = 9;
    
    public static Node invert(Node node) {
        if (node == null) {
            return null;
        }
        
        NegativeNumberPatternInverter visitor = new NegativeNumberPatternInverter();
        return (Node) node.accept(visitor, null);
    }
    
    @Override
    public Object visitEncodedPattern(EncodedPatternNode node, Object data) {
        // Operate on a copy of the pattern tree.
        Node copy = copy(node);
        
        // If the first character is not !, this is not a negative number pattern. Return the copy.
        if (!RegexUtils.isChar(copy.getFirstChild(), RegexConstants.EXCLAMATION_POINT)) {
            return copy;
        }
        
        // Create an initial encoded pattern node with all the leading bin info.
        EncodedPatternNode encodedPattern = new EncodedPatternNode();
        List<Node> children = copy.getChildren();
        int startOfNodesToInvert = 0;
        for (Node child : children) {
            startOfNodesToInvert++;
            encodedPattern.addChild(child);
            if (RegexUtils.isChar(child, RegexConstants.CAPITAL_E)) {
                break;
            }
        }
        
        // Invert the remaining nodes and add them to the encoded pattern node.
        List<Node> nodesToInvert = new ArrayList<>(children.subList(startOfNodesToInvert, children.size()));
        encodedPattern.addChildren(new PatternInverter(nodesToInvert).invert());
        return encodedPattern;
    }
    
    private static class PatternInverter {
        
        // The node iterator.
        protected final NodeListIterator iter;
        
        // The currently inverted nodes.
        protected final List<Node> inverted = new ArrayList<>();
        
        // The most recent element.
        protected Node currentElement;
        
        // The most recent quantifier.
        protected Node currentQuantifier;
        
        // The most recent question mark.
        protected Node currentQuestionMark;
        
        public PatternInverter(List<Node> nodes) {
            Collections.reverse(nodes);
            this.iter = new NodeListIterator(nodes);
        }
        
        public List<Node> invert() {
            invertEndingPermutations();
            while (iter.hasNext()) {
                captureNext();
                inverted.addAll(subtractCurrentFromNine(false));
            }
            Collections.reverse(inverted);
            return inverted;
        }
        
        private void invertEndingPermutations() {
            // Fetch the first element.
            captureNext();
            
            // If the first element can occur zero times, e.g. it could match the '0' character (which would not show up in an encoded number), or it has a
            // quantifier that allows for zero occurrences, e.g. {0,4}, then we must identify all possible trailing elements that may not occur, and create
            // ending permutations that allow for the possibility of each successive element not occurring. The last element of each permutation must be
            // inverted with a minuend of 10, and any preceding elements must be inverted with a minuend of 9.
            if (currentCanOccurZeroTimes()) {
                List<List<Node>> permutations = new ArrayList<>();
                // Add a permutation of the first element inverted with a minuend of 10.
                permutations.add(subtractCurrentFromTen());
                // Examine all remaining elements until we find one that must occur at least once.
                while (iter.hasNext()) {
                    captureNext();
                    // Add a variant of the current element inverted with a minuend of 9 to all existing permutations.
                    List<Node> subtractedFromNine = subtractCurrentFromNine(true);
                    for (List<Node> permutation : permutations) {
                        permutation.addAll(subtractedFromNine);
                    }
                    // If the current element does not match only the '0' character, add a new permutation with a variant of the current element inverted with a
                    // minuend of 10.
                    if (!currentMatchesZeroOnly()) {
                        permutations.add(0, subtractCurrentFromTen());
                    }
                    if (!currentCanOccurZeroTimes()) {
                        break;
                    }
                }
                if (permutations.size() == 1) {
                    // If we only have one permutation, the pattern was only one element long, e.g. "\d". Add the sole permutation to the inverted nodes list.
                    inverted.addAll(permutations.get(0));
                } else {
                    // If we have multiple permutations, we need to create alternations of these permutations, and wrap them in a group.
                    // Sort the alternations from shortest to longest.
                    AlternationNode alternation = new AlternationNode();
                    for (List<Node> permutation : permutations) {
                        // Reverse the nodes in the permutation to restore the correct order.
                        Collections.reverse(permutation);
                        // Add the permutation as an expression to the alternation node.
                        alternation.addChild(new ExpressionNode(permutation));
                    }
                    // Wrap the alternation in a group before adding it to the inverted nodes list.
                    inverted.add(new GroupNode(alternation));
                }
                
            } else {
                // The last-most element must occur at least once, and cannot match the character '0'. Invert it with a minuend of 10, and add it to the
                // inverted nodes list.
                inverted.addAll(subtractCurrentFromTen());
            }
        }
        
        /**
         * Return whether the current element represents something that may match against a trailing zero, or may occur zero times.
         * 
         * @return whether the current element could occur zero times in target matches
         */
        private boolean currentCanOccurZeroTimes() {
            if (currentElement.getType() != NodeType.GROUP) {
                return RegexUtils.matchesZero(currentElement) || (currentQuantifier != null && RegexUtils.canOccurZeroTimes(currentQuantifier));
            } else {
                NodeListIterator groupIter = currentElement.getChildrenIterator();
                Node targetElement = groupIter.next();
                if (RegexUtils.matchesZero(targetElement)) {
                    return true;
                } else {
                    if (groupIter.isNextQuantifier()) {
                        return RegexUtils.canOccurZeroTimes(groupIter.next());
                    }
                    return false;
                }
            }
        }
        
        private boolean currentMatchesZeroOnly() {
            if (currentElement.getType() != NodeType.GROUP) {
                return RegexUtils.matchesZeroOnly(currentElement);
            } else {
                return RegexUtils.matchesZeroOnly(currentElement.getFirstChild());
            }
        }
        
        /**
         * Return the current element inverted with a minuend of 10.
         * 
         * @return the inverted nodes.
         */
        private List<Node> subtractCurrentFromTen() {
            return ElementInverter.forType(currentElement).subtractFromTen(currentElement, currentQuantifier, currentQuestionMark, true);
        }
        
        /**
         * Return the current element inverted with a minuend of 9.
         * 
         * @param endingElement
         *            whether the current element is an ending permutation element
         * @return the inverted nodes
         */
        private List<Node> subtractCurrentFromNine(boolean endingElement) {
            return ElementInverter.forType(currentElement).subtractFromNine(currentElement, currentQuantifier, currentQuestionMark, endingElement);
        }
        
        /**
         * Capture the next element, quantifier, and current question mark.
         */
        protected void captureNext() {
            // Reset the current elements to null.
            setCurrentToNull();
            
            // Extract the next element, quantifier, and question mark if present.
            while (iter.hasNext()) {
                if (iter.isNextQuestionMark()) {
                    currentQuestionMark = iter.next();
                } else if (iter.isNextQuantifier()) {
                    currentQuantifier = iter.next();
                } else {
                    currentElement = iter.next();
                    break;
                }
            }
        }
        
        /**
         * Set the current element, quantifier, and question mark to null.
         */
        protected void setCurrentToNull() {
            currentElement = null;
            currentQuantifier = null;
            currentQuestionMark = null;
        }
    }
    
    private interface ElementInverter {
        
        ElementInverter NON_MODIFYING_INVERTER = new NonModifyingInverter();
        ElementInverter SINGLE_CHAR_INVERTER = new SingleCharInverter();
        ElementInverter CHAR_CLASS_INVERTER = new CharClassInverter();
        ElementInverter GROUP_INVERTER = new GroupInverter();
        
        /**
         * Return the appropriate {@link ElementInverter} for the element's type.
         * 
         * @param element
         *            the element
         * @return the inverter
         */
        static ElementInverter forType(Node element) {
            switch (element.getType()) {
                case ESCAPED_SINGLE_CHAR:
                case ANY_CHAR:
                case DIGIT_CHAR_CLASS:
                    return NON_MODIFYING_INVERTER;
                case SINGLE_CHAR:
                    return SINGLE_CHAR_INVERTER;
                case CHAR_CLASS:
                    return CHAR_CLASS_INVERTER;
                case GROUP:
                    return GROUP_INVERTER;
                default:
                    throw new IllegalArgumentException("Unhandled element type " + element.getType());
            }
        }
        
        List<Node> subtractFromNine(Node element, Node quantifier, Node questionMark, boolean endingElement);
        
        List<Node> subtractFromTen(Node element, Node quantifier, Node questionMark, boolean endingElement);
    }
    
    /**
     * Abstract implementation of {@link ElementInverter} with some shared functionality.
     */
    private static abstract class AbstractInverter implements ElementInverter {
        
        protected List<Node> asList(Node... nodes) {
            List<Node> list = new ArrayList<>();
            for (Node node : nodes) {
                if (node != null) {
                    list.add(node);
                }
            }
            return list;
        }
        
        protected SingleCharNode subtractSingleCharFrom(SingleCharNode node, int minuend) {
            char digit = node.getCharacter();
            int value = minuend - RegexUtils.toInt(digit);
            return value < 10 ? new SingleCharNode(RegexUtils.toChar(value)) : null;
        }
        
    }
    
    /**
     * Handles elements that do not need to go through inversion, like wildcards or the digit character class {@code \d}.
     */
    private static class NonModifyingInverter extends AbstractInverter {
        
        @Override
        public List<Node> subtractFromNine(Node element, Node quantifier, Node questionMark, boolean endingElement) {
            // If this is an ending permutation element, and the element is marked optional, make it non-optional.
            if (endingElement && quantifier == null && questionMark != null) {
                return asList(element);
            }
            // Return the elements in reverse order.
            return asList(questionMark, quantifier, element);
        }
        
        @Override
        public List<Node> subtractFromTen(Node element, Node quantifier, Node questionMark, boolean endingElement) {
            // If this is an ending permutation element, and the element is marked optional, make it non-optional.
            if (endingElement && quantifier == null && questionMark != null) {
                return asList(element);
            }
            // If the quantifier is *, change it to + to require at least one occurrence.
            if (quantifier != null && quantifier.getType() == NodeType.ZERO_OR_MORE) {
                quantifier = new OneOrMoreNode();
            }
            // Return the elements in reverse order.
            return asList(questionMark, quantifier, element);
        }
        
    }
    
    /**
     * Handles inverting single characters.
     */
    private static class SingleCharInverter extends AbstractInverter {
        
        /**
         * Return the given element inverted with a minuend of nine.
         */
        @Override
        public List<Node> subtractFromNine(Node element, Node quantifier, Node questionMark, boolean endingElement) {
            // Subtract the given digit char from 9.
            Node newElement = subtractSingleCharFrom((SingleCharNode) element, NINE);
            // If this is an ending permutation element, and the element is marked optional, make it non-optional.
            if (endingElement && quantifier == null && questionMark != null) {
                return asList(newElement);
            }
            // Return the elements in reverse order.
            return asList(questionMark, quantifier, newElement);
        }
        
        /**
         * Return the given char inverted with a minuend of ten.
         */
        @Override
        public List<Node> subtractFromTen(Node element, Node quantifier, Node questionMark, boolean endingElement) {
            Node fromTen = subtractSingleCharFrom((SingleCharNode) element, TEN);
            // If the element does not have a quantifier, return the question mark and element in reverse order.
            if (quantifier == null) {
                // If this is an ending permutation element, and the element is marked optional, make it non-optional.
                if (endingElement && questionMark != null) {
                    return asList(fromTen);
                } else {
                    return asList(questionMark, fromTen);
                }
            } else {
                // If the element has a quantifier, we must precede the version of the element subtracted from 10 with a version of the element subtracted from
                // 9, and followed by the quantifier with one fewer occurrence.
                Node fromNine = subtractSingleCharFrom((SingleCharNode) element, NINE);
                switch (quantifier.getType()) {
                    case ZERO_OR_MORE:
                    case ONE_OR_MORE:
                        // The new quantifier should be *. Return the elements in reverse order.
                        return asList(fromTen, questionMark, new ZeroOrMoreNode(), fromNine);
                    case REPETITION:
                        // Get the repetition as a range, and subtract 1 from it.
                        Pair<Integer,Integer> range = RegexUtils.getRepetitionAsRange((RepetitionNode) quantifier);
                        range = RegexUtils.subtractOneFrom(range);
                        if (range.getRight() == null) {
                            // The new range is {x,}. Create a new repetition from the range and use that.
                            RepetitionNode fromNineQuantifier = RegexUtils.createRepetition(range);
                            return asList(fromTen, questionMark, fromNineQuantifier, fromNine);
                        } else if (range.getLeft() == 0 && range.getRight() == 0) {
                            // The new range is {0,0}, so zero occurrences. Do not include a version of the element subtracted from 9.
                            return asList(fromTen);
                        } else if (range.getLeft() == 1 && range.getRight() == 1) {
                            // The new range is {1,1}, exactly one occurrence. Include a version of the element subtracted from 9, but do not include a
                            // quantifier.
                            return asList(fromTen, fromNine);
                        } else {
                            // The new range is {x,y}. Create a new repetition from the range and use that.
                            RepetitionNode fromNineQuantifier = RegexUtils.createRepetition(range);
                            return asList(fromTen, questionMark, fromNineQuantifier, fromNine);
                        }
                    default:
                        throw new IllegalArgumentException("Unhandled quantifier type " + quantifier.getType());
                }
            }
        }
    }
    
    /**
     * Handles inverting character classes.
     */
    private static class CharClassInverter extends AbstractInverter {
        
        @Override
        public List<Node> subtractFromNine(Node element, Node quantifier, Node questionMark, boolean endingElement) {
            // Subtract each element in the character class from 9 and return the elements in reverse order.
            Node newElement = subtractFrom((CharClassNode) element, NINE);
            // If this is an ending permutation element, and the element is marked optional, make it non-optional.
            if (endingElement && quantifier == null && questionMark != null) {
                return asList(newElement);
            }
            return asList(questionMark, quantifier, newElement);
        }
        
        @Override
        public List<Node> subtractFromTen(Node element, Node quantifier, Node questionMark, boolean endingElement) {
            Node fromTen = subtractFrom((CharClassNode) element, TEN);
            // If the element does not have a quantifier, return the question mark and element in reverse order.
            if (quantifier == null) {
                // If this is an ending permutation element, and the element is marked optional, make it non-optional.
                if (endingElement && questionMark != null) {
                    return asList(fromTen);
                }
                return asList(questionMark, fromTen);
            } else {
                // If the element has a quantifier, we must precede the version of the element subtracted from 10 with a version of the element subtracted from
                // 9, and followed by the quantifier with one fewer occurrence.
                Node fromNine = subtractFrom((CharClassNode) element, NINE);
                switch (quantifier.getType()) {
                    case ZERO_OR_MORE:
                    case ONE_OR_MORE:
                        // The new quantifier should be *. Return the elements in reverse order.
                        return asList(fromTen, questionMark, new ZeroOrMoreNode(), fromNine);
                    case REPETITION:
                        // Get the repetition as a range, and subtract 1 from it.
                        Pair<Integer,Integer> range = RegexUtils.getRepetitionAsRange((RepetitionNode) quantifier);
                        range = RegexUtils.subtractOneFrom(range);
                        if (range.getRight() == null) {
                            // The new range is {x,}. Create a new repetition from the range and use that.
                            RepetitionNode fromNineQuantifier = RegexUtils.createRepetition(range);
                            return asList(fromTen, questionMark, fromNineQuantifier, fromNine);
                        } else if (range.getLeft() == 0 && range.getRight() == 0) {
                            // The new range is {0,0}, so zero occurrences. Do not include a version of the element subtracted from 9.
                            return asList(fromTen);
                        } else if (range.getLeft() == 1 && range.getRight() == 1) {
                            // The new range is {1,1}, exactly one occurrence. Include a version of the element subtracted from 9, but do not include a
                            // quantifier.
                            return asList(fromTen, fromNine);
                        } else {
                            // The new range is {x,y}. Create a new repetition from the range and use that.
                            RepetitionNode fromNineQuantifier = RegexUtils.createRepetition(range);
                            return asList(fromTen, questionMark, fromNineQuantifier, fromNine);
                        }
                    default:
                        throw new IllegalArgumentException("Unhandled quantifier type " + quantifier.getType());
                }
            }
        }
        
        private Node subtractFrom(CharClassNode node, int minuend) {
            List<Node> children = new ArrayList<>();
            for (Node child : node.getChildren()) {
                // The child is a single char.
                if (child instanceof SingleCharNode) {
                    // Invert the child as long as we are not trying to subtract 0 from 10. Otherwise, do not retain the child.
                    if (minuend != TEN || !RegexUtils.isChar(child, RegexConstants.ZERO)) {
                        children.add(subtractSingleCharFrom((SingleCharNode) child, minuend));
                    }
                } else {
                    // The child is a range.
                    CharRangeNode range = (CharRangeNode) child;
                    int rangeStart = toInt(range.getStart());
                    // If the current minuend is 10 and the start of the range is 0, adjust the range to start from 1 instead so that we're not subtracting 0
                    // from 10.
                    if (minuend == TEN && rangeStart == 0) {
                        rangeStart = 1;
                    }
                    int startValue = minuend - rangeStart;
                    int endValue = minuend - toInt(range.getEnd());
                    // If the start value is equal to or less than the end value, return the range as (start-end). Otherwise, return the range as (end-start).
                    if (startValue <= endValue) {
                        children.add(new CharRangeNode(toChar(startValue), toChar(endValue)));
                    } else {
                        children.add(new CharRangeNode(toChar(endValue), toChar(startValue)));
                    }
                }
            }
            // If after inverting the character class, we only have a single character in it, and the character class is not negated, return the single
            // character rather than a character class.
            if (children.size() == 1 && children.get(0).getType() == NodeType.SINGLE_CHAR && !node.isNegated()) {
                return children.get(0);
            } else {
                // Otherwise, return a character class. Make a shallow copy in order to also copy over whether the char class is negated.
                CharClassNode charClass = node.shallowCopy();
                charClass.addChildren(children);
                return charClass;
            }
        }
    }
    
    /**
     * Handles inverting groups that were inserted into the pattern by {@link ZeroTrimmer}.
     */
    private static class GroupInverter extends AbstractInverter {
        
        @Override
        public List<Node> subtractFromNine(Node element, Node quantifier, Node questionMark, boolean endingElement) {
            List<Node> children = invertGroup(element, NINE, endingElement);
            // If this is an ending permutation element, return the group flattened.
            if (endingElement) {
                return children;
            } else {
                // Otherwise return a new group.
                return createGroup(children, quantifier, questionMark);
            }
        }
        
        @Override
        public List<Node> subtractFromTen(Node element, Node quantifier, Node questionMark, boolean endingElement) {
            List<Node> children = invertGroup(element, TEN, endingElement);
            // If this is an ending permutation element, and the element is marked optional, make it non-optional.
            if (endingElement) {
                return children;
            } else {
                // Otherwise return a new group.
                return createGroup(children, quantifier, questionMark);
            }
        }
        
        // Return the children of the given group inverted.
        private List<Node> invertGroup(Node group, int minuend, boolean endingElement) {
            // Any group seen here was created by the ZeroTrimmer visitor, and will have at most one element, one quantifier, and one question mark. Fetch them
            // from the group.
            NodeListIterator iter = group.getChildrenIterator();
            Node element = iter.next();
            Node quantifier = iter.hasNext() && iter.isNextQuantifier() ? iter.next() : null;
            Node questionMark = iter.hasNext() && iter.isNextQuestionMark() ? iter.next() : null;
            
            // Fetch the appropriate inverter for the element type.
            ElementInverter inverter = ElementInverter.forType(element);
            
            // Invert the elements based on the minuend.
            List<Node> inverted;
            switch (minuend) {
                case NINE:
                    inverted = inverter.subtractFromNine(element, quantifier, questionMark, endingElement);
                    break;
                case TEN:
                    inverted = inverter.subtractFromTen(element, quantifier, questionMark, endingElement);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid minuend " + minuend);
            }
            
            // Return the inverted nodes. We do not need to return them as groups, but can flatten it instead.
            return inverted;
        }
        
        private List<Node> createGroup(List<Node> children, Node quantifier, Node questionMark) {
            Collections.reverse(children);
            return asList(questionMark, quantifier, new GroupNode(children));
        }
    }
}
