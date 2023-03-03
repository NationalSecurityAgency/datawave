package datawave.query.planner.pushdown;

import java.util.Collection;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.visitors.RebuildingVisitor;
import datawave.query.planner.pushdown.rules.DelayedPredicatePushDown;
import datawave.query.planner.pushdown.rules.PushDownRule;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.MetadataHelper;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.log4j.Logger;

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
        IsType delayedCheck = new IsType(ASTDelayedPredicate.class);
        for (int i = 0; i < jexlScript.jjtGetNumChildren(); i++) {
            JexlNode child = jexlScript.jjtGetChild(i);
            if (delayedCheck.apply(child)) {
                delayedCount++;
            }
        }
        return delayedCount == jexlScript.jjtGetNumChildren();
    }
    
}
