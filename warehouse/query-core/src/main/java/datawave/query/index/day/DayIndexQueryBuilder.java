package datawave.query.index.day;

import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTFalseNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.ASTTrueNode;
import org.apache.commons.jexl3.parser.JexlNode;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.BaseVisitor;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;

/**
 * Builds the query string given the index and a map of terms to bitset shards
 */
public class DayIndexQueryBuilder extends BaseVisitor {

    private Map<String,BitSet> shards;
    private int index;

    /**
     * This is the cleanest way to start coding this visitor. In the future avoid setting the script and shard every time.
     *
     * @param script
     *            the query
     * @param shards
     *            the map of terms to shards
     * @param index
     *            the index to build the query for
     * @return a built query
     */
    public JexlNode buildQuery(ASTJexlScript script, Map<String,BitSet> shards, int index) {
        this.shards = shards;
        this.index = index;
        return (JexlNode) script.jjtAccept(this, null);
    }

    /**
     * If the key exists in the shard map then the index must match. Otherwise, assume the node is deferred.
     *
     * @param key
     *            the node key
     * @return true if the node key is present in the shard map and matches the current index
     */
    private boolean keyMatchesIndex(String key) {
        return !shards.containsKey(key) || shards.get(key).get(index);
    }

    @Override
    public Object visit(ASTJexlScript node, Object data) {
        return node.jjtGetChild(0).jjtAccept(this, data);
    }

    @Override
    public Object visit(ASTReference node, Object data) {
        return node.jjtGetChild(0).jjtAccept(this, data);
    }

    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        return node.jjtGetChild(0).jjtAccept(this, data);
    }

    @Override
    public Object visit(ASTNotNode node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTFalseNode node, Object data) {
        // prune false nodes to keep same behavior of RangeStream
        return null;
    }

    @Override
    public Object visit(ASTTrueNode node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        String key = JexlStringBuildingVisitor.buildQuery(node);

        if (keyMatchesIndex(key)) {
            return node;
        }

        return data;
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {

        if (QueryPropertyMarker.findInstance(node).isAnyType()) {
            return node;
        }

        List<JexlNode> children = new LinkedList<>();

        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode child = JexlASTHelper.dereference(node.jjtGetChild(i));

            if (QueryPropertyMarker.findInstance(child).isAnyType()) {
                children.add(node.jjtGetChild(i));
                continue;
            }

            JexlNode result = (JexlNode) child.jjtAccept(this, data);
            // determine indexed status
            if (result != null) {
                children.add(result);
            } else {
                // short circuit intersection
                return null;
            }
        }

        switch (children.size()) {
            case 0:
                return null;
            case 1:
                return children.iterator().next();
            default:
                return JexlNodeFactory.createAndNode(children);
        }
    }

    @Override
    public Object visit(ASTOrNode node, Object data) {
        List<JexlNode> children = new LinkedList<>();

        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode child = JexlASTHelper.dereference(node.jjtGetChild(i));

            if (QueryPropertyMarker.findInstance(child).isAnyType()) {
                children.add(node.jjtGetChild(i));
                continue;
            }

            JexlNode result = (JexlNode) child.jjtAccept(this, data);

            if (result != null) {
                children.add(result);
            }
        }

        switch (children.size()) {
            case 0:
                return null;
            case 1:
                return children.iterator().next();
            default:
                return JexlNodeFactory.createOrNode(children);
        }
    }

}
