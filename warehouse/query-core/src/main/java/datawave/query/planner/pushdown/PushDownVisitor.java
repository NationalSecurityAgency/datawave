package datawave.query.planner.pushdown;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.DELAYED;

import java.util.Collection;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.RebuildingVisitor;
import datawave.query.planner.pushdown.rules.DelayedPredicatePushDown;
import datawave.query.planner.pushdown.rules.PushDownRule;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.MetadataHelper;

/**
 * The PushDownVisitor will decide what nodes are "delayed" in that they do NOT require a global index lookup due to cost or other reasons.
 *
 */
public class PushDownVisitor extends RebuildingVisitor {

    private static final Logger log = Logger.getLogger(PushDownVisitor.class);
    private final Collection<PushDownRule> pushDownRules;
    private final ShardQueryConfiguration config;
    private final ScannerFactory scannerFactory;
    private final MetadataHelper helper;

    public PushDownVisitor(ShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelper helper, Collection<PushDownRule> pushDownRules) {
        this.config = config;
        this.scannerFactory = scannerFactory;
        this.helper = helper;
        this.pushDownRules = pushDownRules;
    }

    public MetadataHelper getHelper() {
        return helper;
    }

    public ScannerFactory getScannerFactory() {
        return scannerFactory;
    }

    public ShardQueryConfiguration getConfiguration() {
        return config;
    }

    public ASTJexlScript applyRules(ASTJexlScript script) {

        return (ASTJexlScript) script.jjtAccept(this, this);
    }

    @Override
    public Object visit(ASTJexlScript script, Object data) {
        ASTJexlScript rewrittenScript = script;

        if (log.isTraceEnabled()) {
            log.trace("Adding script " + rewrittenScript);
        }
        for (PushDownRule rule : pushDownRules) {
            rewrittenScript = (ASTJexlScript) rule.visit(rewrittenScript, this);
        }

        if (allDelayed(rewrittenScript)) {
            if (log.isTraceEnabled()) {
                log.trace("All predicates are delayed");
            }
            DelayedPredicatePushDown predicatePushDown = new DelayedPredicatePushDown();
            rewrittenScript = (ASTJexlScript) predicatePushDown.visit(rewrittenScript, this);
        }

        return rewrittenScript;
    }

    /**
     * Returns whether or not all top level children are delayed. if this is the case
     *
     * @param jexlScript
     *            the jexl script
     * @return whether or not all top level children are delayed
     */
    private boolean allDelayed(ASTJexlScript jexlScript) {
        if (jexlScript.jjtGetNumChildren() == 0) {
            return false;
        }
        int delayedCount = 0;
        for (int i = 0; i < jexlScript.jjtGetNumChildren(); i++) {
            JexlNode child = jexlScript.jjtGetChild(i);
            if (isDelayedPredicate(child)) {
                delayedCount++;
            }
        }
        return delayedCount == jexlScript.jjtGetNumChildren();
    }

    private boolean isDelayedPredicate(JexlNode node) {
        Preconditions.checkNotNull(node);
        return QueryPropertyMarker.findInstance(node).isType(DELAYED);
    }
}
