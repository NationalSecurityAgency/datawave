package datawave.query.jexl.visitors;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.BOUNDED_RANGE;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.JexlNode;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.LiteralRange;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.util.MetadataHelper;

/**
 * Visitor determines if any bounded ranges exist for non-event fields
 */
public class BoundedRangeDetectionVisitor extends ShortCircuitBaseVisitor {

    ShardQueryConfiguration config;
    MetadataHelper helper;

    private Set<String> nonEventFields;

    @Deprecated(since = "7.35.0", forRemoval = true)
    public BoundedRangeDetectionVisitor(ShardQueryConfiguration config, MetadataHelper metadataHelper) {
        this.config = config;
        this.helper = metadataHelper;
    }

    public BoundedRangeDetectionVisitor(Set<String> nonEventFields) {
        this.nonEventFields = nonEventFields;
    }

    @Deprecated(since = "7.35.0", forRemoval = true)
    public static boolean mustExpandBoundedRange(ShardQueryConfiguration config, MetadataHelper metadataHelper, JexlNode script) {
        try {
            Set<String> nonEventFields = metadataHelper.getNonEventFields(config.getDatatypeFilter());
            return mustExpandBoundedRange(script, nonEventFields);
        } catch (TableNotFoundException e) {
            throw new DatawaveFatalQueryException("Metadata table not found", e);
        }
    }

    public static boolean mustExpandBoundedRange(JexlNode node, Set<String> nonEventFields) {
        BoundedRangeDetectionVisitor visitor = new BoundedRangeDetectionVisitor(nonEventFields);
        AtomicBoolean hasBounded = new AtomicBoolean(false);
        node.jjtAccept(visitor, hasBounded);
        return hasBounded.get();
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        if (QueryPropertyMarker.findInstance(node).isType(BOUNDED_RANGE)) {
            LiteralRange<?> range = JexlASTHelper.findRange().getRange(node);
            if (data != null && nonEventFields.contains(range.getFieldName())) {
                AtomicBoolean hasBounded = (AtomicBoolean) data;
                hasBounded.set(true);
            }
            return false;
        } else {
            return super.visit(node, data);
        }
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        if (data != null && nonEventFields.contains(JexlASTHelper.getIdentifier(node))) {
            AtomicBoolean hasBounded = (AtomicBoolean) data;
            hasBounded.set(true);
        }
        return false;
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        if (data != null) {
            AtomicBoolean hasBounded = (AtomicBoolean) data;
            hasBounded.set(true);
        }
        return false;
    }

    // Ensure we short circuit on the following nodes that make up a bounded range
    // We can short circuit recursion at the leaf nodes to help speed up query planning time
    @Override
    public Object visit(ASTLTNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        return data;
    }

    // We don't expect to see a bounded range inside a function
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        return data;
    }

}
