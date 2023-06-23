package datawave.query.jexl.visitors;

import com.google.common.base.Preconditions;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.nodes.ExceededOrThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededTermThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededValueThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.util.MetadataHelper;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;

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
        return QueryPropertyMarker.findInstance(currNode).isAnyTypeOf(ExceededTermThresholdMarkerJexlNode.class, ExceededOrThresholdMarkerJexlNode.class,
                        ExceededValueThresholdMarkerJexlNode.class);
    }

    @Override
    public Object visit(ASTReference node, Object data) {
        // Recurse only if not delayed
        if (isDelayedPredicate(node)) {
            ((AtomicBoolean) data).set(false);
            return data;
        }

        return super.visit(node, data);
    }

}
