package org.apache.commons.jexl3.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The node-stack behavior of JEXL 3.5's {@link JJTParserState}, using primitive scope marks.
 * <p>
 * In a wide junction, marks grow with the number of preceding operands. Boxing those marks allocates several Integer objects for each operand after the Integer
 * cache is exhausted, even though the number of open scopes remains small.
 */
final class DatawaveParserState extends JJTParserState {
    private static final long serialVersionUID = 1L;

    private final List<Node> nodes = new ArrayList<>();
    private int[] marks = new int[16];
    private int markCount;
    private int stackSize;
    private int mark;
    private boolean nodeCreated;

    @Override
    public boolean nodeCreated() {
        return nodeCreated;
    }

    @Override
    public void reset() {
        nodes.clear();
        markCount = 0;
        stackSize = 0;
        mark = 0;
    }

    @Override
    public Node rootNode() {
        return nodes.get(0);
    }

    @Override
    public void pushNode(Node node) {
        nodes.add(node);
        stackSize++;
    }

    @Override
    public Node popNode() {
        if (--stackSize < mark) {
            mark = marks[--markCount];
        }
        return nodes.remove(nodes.size() - 1);
    }

    @Override
    public Node peekNode() {
        return nodes.get(nodes.size() - 1);
    }

    @Override
    public int nodeArity() {
        return stackSize - mark;
    }

    @Override
    public void clearNodeScope(Node node) {
        while (stackSize > mark) {
            popNode();
        }
        mark = marks[--markCount];
    }

    @Override
    public void openNodeScope(Node node) {
        if (markCount == marks.length) {
            marks = Arrays.copyOf(marks, marks.length * 2);
        }
        marks[markCount++] = mark;
        mark = stackSize;
        node.jjtOpen();
    }

    @Override
    public void closeNodeScope(Node node, int children) {
        mark = marks[--markCount];
        while (children-- > 0) {
            Node child = popNode();
            child.jjtSetParent(node);
            node.jjtAddChild(child, children);
        }
        node.jjtClose();
        pushNode(node);
        nodeCreated = true;
    }

    @Override
    public void closeNodeScope(Node node, boolean condition) {
        if (condition) {
            closeNodeScope(node, nodeArity());
        } else {
            mark = marks[--markCount];
            nodeCreated = false;
        }
    }
}
