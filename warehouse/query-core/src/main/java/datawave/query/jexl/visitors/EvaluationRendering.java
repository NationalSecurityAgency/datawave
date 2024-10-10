package datawave.query.jexl.visitors;

import static datawave.core.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_OR;
import static datawave.core.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_TERM;
import static datawave.core.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_VALUE;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

import datawave.core.query.jexl.nodes.QueryPropertyMarker;
import datawave.core.query.jexl.visitors.BaseVisitor;
import datawave.core.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.util.MetadataHelper;

/**
 *
 */
public class EvaluationRendering extends BaseVisitor {
    private static final Logger log = Logger.getLogger(EvaluationRendering.class);

    protected final ShardQueryConfiguration config;
    protected final MetadataHelper helper;

    protected boolean allowRange;

    public EvaluationRendering(ShardQueryConfiguration config, MetadataHelper helper) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(helper);

        this.config = config;
        this.helper = helper;
    }

    public static boolean canDisableEvaluation(JexlNode script, ShardQueryConfiguration config, MetadataHelper helper) {
        return canDisableEvaluation(script, config, helper, false);
    }

    public static boolean canDisableEvaluation(JexlNode script, ShardQueryConfiguration config, MetadataHelper helper, boolean allowRange) {
        Preconditions.checkNotNull(script);

        AtomicBoolean res = new AtomicBoolean(true);
        if (log.isTraceEnabled()) {
            log.trace(JexlStringBuildingVisitor.buildQuery(script));
        }
        EvaluationRendering visitor = new EvaluationRendering(config, helper);

        visitor.allowRange = allowRange;

        script.jjtAccept(visitor, res);

        return res.get();
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        if (!allowRange)
            ((AtomicBoolean) data).set(false);
        return data;
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        if (!allowRange)
            ((AtomicBoolean) data).set(false);
        return data;
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        if (!allowRange)
            ((AtomicBoolean) data).set(false);
        return data;
    }

    protected boolean isDelayedPredicate(JexlNode currNode) {
        return QueryPropertyMarker.findInstance(currNode).isAnyTypeOf(EXCEEDED_TERM, EXCEEDED_OR, EXCEEDED_VALUE);
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        // Recurse only if not delayed
        if (isDelayedPredicate(node)) {
            ((AtomicBoolean) data).set(false);
            return data;
        }

        return super.visit(node, data);
    }

}
