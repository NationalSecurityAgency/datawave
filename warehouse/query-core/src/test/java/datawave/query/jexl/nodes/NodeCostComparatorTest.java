package datawave.query.jexl.nodes;

import static org.junit.Assert.assertEquals;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.jexl2.parser.JexlNode;
import org.junit.Test;

import datawave.query.jexl.JexlNodeFactory;

public class NodeCostComparatorTest {

    @Test
    public void testCompareTwoEq() {
        JexlNode left = JexlNodeFactory.buildEQNode("FOO", "bar");
        JexlNode right = JexlNodeFactory.buildEQNode("FOO", "baz");

        List<JexlNode> nodes = new LinkedList<>();
        nodes.add(left);
        nodes.add(right);

        Iterator<JexlNode> iter = nodes.iterator();
        assertEquals(left, iter.next());
        assertEquals(right, iter.next());

        nodes.sort(new NodeCostComparator());

        // Order should not have changed
        iter = nodes.iterator();
        assertEquals(left, iter.next());
        assertEquals(right, iter.next());
    }

    @Test
    public void testCompareEqAndRe() {
        JexlNode left = JexlNodeFactory.buildEQNode("FOO", "bar");
        JexlNode right = JexlNodeFactory.buildERNode("FOO", "baz.*");

        List<JexlNode> nodes = new LinkedList<>();
        nodes.add(right);
        nodes.add(left);

        // Assert insert order
        Iterator<JexlNode> iter = nodes.iterator();
        assertEquals(right, iter.next());
        assertEquals(left, iter.next());

        nodes.sort(new NodeCostComparator());

        // Assert proper sort order, EQ before ER
        iter = nodes.iterator();
        assertEquals(left, iter.next());
        assertEquals(right, iter.next());
    }

    @Test
    public void testCompareEqAndFunction() {
        JexlNode left = JexlNodeFactory.buildEQNode("FOO", "bar");
        JexlNode right = JexlNodeFactory.buildFunctionNode("content", "phrase", "FOO", "baz");

        List<JexlNode> nodes = new LinkedList<>();
        nodes.add(right);
        nodes.add(left);

        // Assert insert order
        Iterator<JexlNode> iter = nodes.iterator();
        assertEquals(right, iter.next());
        assertEquals(left, iter.next());

        nodes.sort(new NodeCostComparator());

        // Assert proper sort order, EQ before ER
        iter = nodes.iterator();
        assertEquals(left, iter.next());
        assertEquals(right, iter.next());
    }
}
