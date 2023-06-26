package datawave.test;

import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.JexlNode;

import java.util.Comparator;

/**
 * Performs a shallow comparison of two nodes. It is meant to be used by the {@link DeepJexlNodeComparator} to quickly identify simplistic differences between
 * two nodes before performing a more costly comparison. This comparator should be used only for equality checks, and should not be used for any order-dependent
 * assertions.
 */
public class ShallowJexlNodeComparator implements Comparator<JexlNode> {

    /**
     * Returns whether the two nodes are either the same, or have equal types, number of children, images, and values.
     *
     * @param first
     *            the first node to compare
     * @param second
     *            the second node to compare
     * @return 0 if the two nodes are considered equal, or 1 otherwise
     */
    @Override
    public int compare(JexlNode first, JexlNode second) {
        if (first == second) {
            return 0;
        }
        if (first == null || second == null) {
            return 1;
        }
        if (first.jjtGetNumChildren() != second.jjtGetNumChildren()) {
            return 1;
        }
        if (!first.getClass().equals(second.getClass())) {
            return 1;
        }
        if (first instanceof ASTIdentifier && second instanceof ASTIdentifier) {
            ASTIdentifier firstIdentifier = (ASTIdentifier) first;
            ASTIdentifier secondIdentifier = (ASTIdentifier) second;
            if (!(firstIdentifier.getNamespace().equals(secondIdentifier.getNamespace()) && firstIdentifier.getName().equals(secondIdentifier.getName()))) {
                return 1;
            }
        }
        if (first instanceof JexlNode.Constant && second instanceof JexlNode.Constant) {
            JexlNode.Constant<?> firstLiteral = (JexlNode.Constant<?>) first;
            JexlNode.Constant<?> secondLiteral = (JexlNode.Constant<?>) second;
            if (!firstLiteral.getLiteral().equals(secondLiteral.getLiteral())) {
                return 1;
            }
        }
        return 0;
    }
}
