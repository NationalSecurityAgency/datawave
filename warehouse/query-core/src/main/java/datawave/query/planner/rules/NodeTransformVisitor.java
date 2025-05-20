package datawave.query.planner.rules;

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

import com.google.common.base.Preconditions;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.RebuildingVisitor;
import datawave.query.util.MetadataHelper;

public class NodeTransformVisitor extends RebuildingVisitor {

    private final ShardQueryConfiguration config;
    private final List<NodeTransformRule> rules;
    private final MetadataHelper helper;

    public NodeTransformVisitor(ShardQueryConfiguration config, MetadataHelper helper, List<NodeTransformRule> rules) {
        Preconditions.checkNotNull(rules, "Must supply non-null rules to NodeTransformVisitor");
        this.helper = helper;
        this.config = config;
        this.rules = rules;
    }

    public static ASTJexlScript transform(ASTJexlScript tree, List<NodeTransformRule> rules, ShardQueryConfiguration config, MetadataHelper helper) {
        NodeTransformVisitor visitor = new NodeTransformVisitor(config, helper, rules);
        return visitor.apply(tree);
    }

    @Override
    public Object visit(ASTOrNode node, Object data) {
        return applyTransforms(super.visit(node, data));
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        // do not recurse on a marker node
        if (QueryPropertyMarker.findInstance(node).isAnyType()) {
            return applyTransforms(RebuildingVisitor.copy(node));
        } else {
            return applyTransforms(super.visit(node, data));
        }
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        return applyTransforms(super.visit(node, data));
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        return applyTransforms(super.visit(node, data));
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        return applyTransforms(super.visit(node, data));
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        return applyTransforms(super.visit(node, data));
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        return applyTransforms(super.visit(node, data));
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        return applyTransforms(super.visit(node, data));
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        return applyTransforms(super.visit(node, data));
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        return applyTransforms(super.visit(node, data));
    }

    @Override
    public Object visit(ASTNotNode node, Object data) {
        return applyTransforms(super.visit(node, data));
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        return applyTransforms(super.visit(node, data));
    }

    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        return applyTransforms(super.visit(node, data));
    }

    private Object applyTransforms(Object node) {
        for (NodeTransformRule rule : rules) {
            node = rule.apply((JexlNode) node, config, helper);
        }
        return node;
    }
}
