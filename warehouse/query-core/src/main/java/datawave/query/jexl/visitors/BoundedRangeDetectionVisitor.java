package datawave.query.jexl.visitors;

import datawave.marking.MarkingFunctions;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.LiteralRange;
import datawave.query.jexl.nodes.BoundedRange;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.util.MetadataHelper;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.JexlNode;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public class BoundedRangeDetectionVisitor extends ShortCircuitBaseVisitor {

    ShardQueryConfiguration config;
    MetadataHelper helper;

    public BoundedRangeDetectionVisitor(ShardQueryConfiguration config, MetadataHelper metadataHelper) {
        this.config = config;
        this.helper = metadataHelper;
    }

    @SuppressWarnings("unchecked")
    public static boolean mustExpandBoundedRange(ShardQueryConfiguration config, MetadataHelper metadataHelper, JexlNode script) {
        BoundedRangeDetectionVisitor visitor = new BoundedRangeDetectionVisitor(config, metadataHelper);

        AtomicBoolean hasBounded = new AtomicBoolean(false);
        script.jjtAccept(visitor, hasBounded);

        return hasBounded.get();
    }

    @Override
    public Object visit(ASTReference node, Object data) {
        if (QueryPropertyMarker.findInstance(node).isType(BoundedRange.class)) {
            LiteralRange<?> range = JexlASTHelper.findRange().getRange(node);
            try {
                if (null != data && helper.getNonEventFields(config.getDatatypeFilter()).contains(range.getFieldName())) {
                    AtomicBoolean hasBounded = (AtomicBoolean) data;
                    hasBounded.set(true);
                }
            } catch (TableNotFoundException e) {
                throw new DatawaveFatalQueryException("Cannot access metadata", e);
            }

            return false;
        } else {
            return super.visit(node, data);
        }
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        try {
            if (null != data && helper.getNonEventFields(config.getDatatypeFilter()).contains(JexlASTHelper.getIdentifier(node))) {
                AtomicBoolean hasBounded = (AtomicBoolean) data;
                hasBounded.set(true);
            }
        } catch (TableNotFoundException e) {
            throw new DatawaveFatalQueryException("Cannot access metadata", e);
        }

        return false;
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        if (null != data) {
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
