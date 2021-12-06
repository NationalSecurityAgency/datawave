package datawave.test;

import org.apache.commons.jexl2.parser.JexlNode;

import java.util.List;

/**
 * A utility class for providing a single point of entry to multiple types of assertions on {@link JexlNode} instances.
 */
public class JexlNodeAssertions {
    
    /**
     * Return a new {@link JexlNodeAssert} for the given node.
     * 
     * @param node
     *            the node to perform assertions on
     * @return the new {@link JexlNodeAssert}
     */
    public static JexlNodeAssert assertThat(JexlNode node) {
        return JexlNodeAssert.assertThat(node);
    }
    
    /**
     * Return a new {@link JexlNodeIterableAssert} for the given iterable.
     * 
     * @param iterable
     *            the iterable to perform assertions on
     * @return the new {@link JexlNodeIterableAssert}
     */
    public static JexlNodeIterableAssert assertThat(Iterable<JexlNode> iterable) {
        return new JexlNodeIterableAssert(iterable);
    }
    
    /**
     * Return a new {@link JexlNodeListAssert} for the given list.
     * 
     * @param list
     *            the list to perform assertions on
     * @return the new {@link JexlNodeListAssert}
     */
    public static JexlNodeListAssert assertThat(List<JexlNode> list) {
        return new JexlNodeListAssert(list);
    }
    
    private JexlNodeAssertions() {
        throw new UnsupportedOperationException();
    }
}
