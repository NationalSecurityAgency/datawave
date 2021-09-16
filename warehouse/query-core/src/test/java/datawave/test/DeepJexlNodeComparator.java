package datawave.test;

import datawave.query.jexl.visitors.PrintingVisitor;
import datawave.query.jexl.visitors.TreeEqualityVisitor;
import org.apache.commons.jexl2.parser.JexlNode;

import java.util.Comparator;

/**
 * Performs a full comparison of two nodes. This comparator is intended to be used by classes such as {@link JexlNodeIterableAssert} and
 * {@link JexlNodeListAssert} to ensure AssertJ will determine equality correctly when performing assertions against collections of {@link JexlNode} instances.
 * This comparator should be used only for equality checks, and should not be used for any order-dependent assertions.
 */
public class DeepJexlNodeComparator implements Comparator<JexlNode> {
    
    private static final ShallowJexlNodeComparator SHALLOW_COMPARATOR = new ShallowJexlNodeComparator();
    
    @Override
    public int compare(JexlNode first, JexlNode second) {
        // Perform shallow comparisons first of the nodes and their parents to find any immediate differences before performing a deeper comparison.
        int result = SHALLOW_COMPARATOR.compare(first, second);
        if (result == 0) {
            result = SHALLOW_COMPARATOR.compare(first.jjtGetParent(), second.jjtGetParent());
        }
        if (result == 0) {
            // A shallow comparison revealed no difference, perform a full comparison of the two query trees.
            TreeEqualityVisitor.Comparison comparison = TreeEqualityVisitor.checkEquality(first, second);
            if (!comparison.isEqual()) {
                PrintingVisitor.printQuery(first);
                PrintingVisitor.printQuery(second);
                result = 1;
            }
        }
        return result;
    }
}
