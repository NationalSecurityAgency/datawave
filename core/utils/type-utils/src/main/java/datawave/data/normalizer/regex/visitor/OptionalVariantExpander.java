package datawave.data.normalizer.regex.visitor;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import datawave.data.normalizer.regex.AlternationNode;
import datawave.data.normalizer.regex.ExpressionNode;
import datawave.data.normalizer.regex.Node;
import datawave.data.normalizer.regex.OneOrMoreNode;
import datawave.data.normalizer.regex.QuestionMarkNode;
import datawave.data.normalizer.regex.RegexUtils;
import datawave.data.normalizer.regex.RepetitionNode;
import datawave.data.normalizer.regex.ZeroOrMoreNode;

/**
 * Implementation of {@link CopyVisitor} that will return a copy of the tree where elements marked as optional are expanded such each optional character results
 * in an alternation variant with the optional character present, and an alternation variant not present. This does not apply to optional found after a star,
 * plus, or repetition quantifier, or any optionals applying to a character that occur after an escaped decimal point; e.g. in the cases of {@code ".*?111"},
 * {@code ".+?111"}, {@code "14{3}?1"}, or {@code "12\.4?"}. See the following examples of cases where an optional will result in variants.
 * <ul>
 * <li>Input {@code "2?"} will return {@code "2"}</li>
 * <li>Input {@code "2.?5"} will return {@code "25|2.5"}</li>
 * <li>Input {@code "2[3-9]?5"} will return {@code "25|2[2-9]5"}</li>
 * <li>Input {@code "27?5"} will return {@code "25|275"}</li>
 * <li>Input {@code "2(45.*)?5"} will return {@code "25|2(45.*)5"}</li>
 * <li>Input {@code "2\.?5"} will return {@code "25|2\.5"}</li>
 * <li>Input {@code "-?25"} will return {@code "25|-25"}</li>
 * </ul>
 */
public class OptionalVariantExpander extends SubExpressionVisitor {
    
    public static Node expand(Node node) {
        if (node == null) {
            return null;
        }
        OptionalVariantExpander visitor = new OptionalVariantExpander();
        return (Node) node.accept(visitor, null);
    }
    
    @Override
    protected Object visitSubExpression(Node node) {
        if (node.isAnyChildOf(QuestionMarkNode.class)) {
            return expandOptionals(node);
        } else {
            return copy(node);
        }
    }
    
    /**
     * Return an expression that contains the expanded variants of each expanded optional.
     * 
     * @param node
     *            the expression to expand
     * @return the expanded expression
     */
    private Node expandOptionals(Node node) {
        List<Node> expansions = new ArrayList<>();
        expansions.add(new ExpressionNode());
        
        int startIndex = 0;
        int optionalPos = node.indexOf(QuestionMarkNode.class);
        int posBeforeOptional = optionalPos - 1;
        int decimalPoint = RegexUtils.getDecimalPointIndex(node);
        
        // If the first optional found is after an escaped decimal point, there is no need to do any expansion. Return a copy of the copy.
        if (decimalPoint != -1 && decimalPoint < posBeforeOptional) {
            return copy(node);
        }
        
        do {
            // Children from the start index (inclusive) to the position before optional (not inclusive) can be added to each expansion.
            expansions = addChildrenToExpansions(expansions, node, startIndex, posBeforeOptional);
            // Move the start index to the position before the optional.
            startIndex = posBeforeOptional;
            
            // If the optional is not a modifier to make a quantifier match in lazy mode, add expansions for each variant.
            Node childBeforeOptional = node.getChildAt(posBeforeOptional);
            if (!(isOptionalLazyModifierFor(childBeforeOptional))) {
                expansions = addOptionalElement(expansions, childBeforeOptional);
                startIndex = optionalPos + 1;
            }
            
            // Determine the position of the next optional node, and the child before it.
            optionalPos = node.indexOf(QuestionMarkNode.class, (optionalPos + 1));
            posBeforeOptional = optionalPos - 1;
            
            // If there is an escaped decimal point in the regex, and the next optional is for a character after it, there is no need to do any further
            // expansion.
            if (decimalPoint != -1 && decimalPoint < posBeforeOptional) {
                break;
            }
        } while (optionalPos != -1);
        
        // If we have any remaining children to copy to each expansion, do so.
        if (startIndex < (node.getChildCount())) {
            expansions = addChildrenToExpansions(expansions, node, startIndex, node.getChildCount());
        }
        
        // Remove any expansions that are leafs without children.
        expansions = expansions.stream().filter((ex) -> !ex.isLeaf()).collect(Collectors.toList());
        
        // If we only have one expression after expansion, return the expression.
        if (expansions.size() == 1) {
            return expansions.get(0);
        } else {
            // Otherwise return an expression containing each expansion as an alternation.
            return new ExpressionNode(new AlternationNode(expansions));
        }
    }
    
    /**
     * Return whether the given node is a *, +, or a repetition quantifier.
     * 
     * @param node
     *            the node
     * @return true if the node is a *, +, or a repetition quantifier, or false otherwise.
     */
    private boolean isOptionalLazyModifierFor(Node node) {
        return node instanceof ZeroOrMoreNode || node instanceof OneOrMoreNode || node instanceof RepetitionNode;
    }
    
    /**
     * Add the children of the given node from the start index (inclusive) to the end index (not inclusive) to each expansion in the list.
     * 
     * @param expansions
     *            the expansions
     * @param node
     *            the node
     * @param startIndex
     *            the start index of children to copy (inclusive)
     * @param endIndex
     *            the end index of children to copy (not inclusive)
     * @return an updated list of expansions
     */
    private List<Node> addChildrenToExpansions(List<Node> expansions, Node node, int startIndex, int endIndex) {
        List<Node> newExpansions = new ArrayList<>();
        for (Node expansion : expansions) {
            Node newExpansion = copy(expansion);
            for (int index = startIndex; index < endIndex; index++) {
                newExpansion.addChild(copy(node.getChildAt(index)));
            }
            newExpansions.add(newExpansion);
        }
        return newExpansions;
    }
    
    /**
     * Add the given optional element to each expansion, preserving a copy of each original expansion.
     * 
     * @param expansions
     *            the expansions
     * @param optionalElement
     *            the optional element
     * @return an updated list of expansions
     */
    private List<Node> addOptionalElement(List<Node> expansions, Node optionalElement) {
        List<Node> newExpansions = new ArrayList<>();
        for (Node expansion : expansions) {
            newExpansions.add(copy(expansion));
            Node newExpansion = copy(expansion);
            newExpansion.addChild(copy(optionalElement));
            newExpansions.add(newExpansion);
        }
        return newExpansions;
    }
}
