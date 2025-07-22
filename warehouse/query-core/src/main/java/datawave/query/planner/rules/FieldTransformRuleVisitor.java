package datawave.query.planner.rules;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.commons.jexl3.parser.JexlNodes.newInstanceOfType;
import static org.apache.commons.jexl3.parser.JexlNodes.setChildren;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
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
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.RebuildingVisitor;
import datawave.query.util.MetadataHelper;

public class FieldTransformRuleVisitor extends NodeTransformVisitor {

    public FieldTransformRuleVisitor(ShardQueryConfiguration config, MetadataHelper helper, List<NodeTransformRule> rules) {
        super(config, helper, rules);
    }

    public static ASTJexlScript transform(ASTJexlScript tree, List<NodeTransformRule> rules, ShardQueryConfiguration config, MetadataHelper helper) {
        FieldTransformRuleVisitor visitor = new FieldTransformRuleVisitor(config, helper, rules);
        return visitor.apply(tree);
    }

    private <T extends JexlNode> T copy(T node, Object data) {
        T newNode = newInstanceOfType(node);
        // keep lineage
        newNode.jjtSetParent(node.jjtGetParent());
        ArrayList<JexlNode> children = newArrayList();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode copiedChild = (JexlNode) node.jjtGetChild(i).jjtAccept(this, data);
            if (copiedChild != null) {
                children.add(copiedChild);
            }
        }
        return setChildren(newNode, children.toArray(new JexlNode[children.size()]));
    }

    @Override
    public Object visit(ASTOrNode node, Object data) {
        return applyTransforms(copy(node, data));
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        // do not recurse on a marker node
        if (QueryPropertyMarker.findInstance(node).isAnyType()) {
            return applyTransforms(RebuildingVisitor.copy(node));
        } else {
            return applyTransforms(copy(node, data));
        }
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        return applyTransforms(copy(node, data));
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        return applyTransforms(copy(node, data));
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        return applyTransforms(copy(node, data));
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        return applyTransforms(copy(node, data));
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        return applyTransforms(copy(node, data));
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        return applyTransforms(copy(node, data));
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        return applyTransforms(copy(node, data));
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        return applyTransforms(copy(node, data));
    }

    @Override
    public Object visit(ASTNotNode node, Object data) {
        return applyTransforms(copy(node, data));
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        return applyTransforms(copy(node, data));
    }

    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        return applyTransforms(copy(node, data));
    }
}
