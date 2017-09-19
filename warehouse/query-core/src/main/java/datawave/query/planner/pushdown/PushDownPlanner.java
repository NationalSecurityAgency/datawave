package datawave.query.planner.pushdown;

import java.util.Collection;

import datawave.query.config.ShardQueryConfiguration;
import org.apache.commons.jexl2.parser.ASTJexlScript;

import datawave.query.planner.pushdown.rules.PushDownRule;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.MetadataHelper;

/**
 * 
 */
public interface PushDownPlanner {
    
    public void setRules(Collection<PushDownRule> rules);
    
    public Collection<PushDownRule> getRules();
    
    /**
     * Returns a re-written tree that reflects pushed down Predicates
     * 
     * @param queryTree
     * @param scannerFactory
     * @param metadataHelper
     * @param config
     * @return
     */
    ASTJexlScript applyRules(ASTJexlScript queryTree, ScannerFactory scannerFactory, MetadataHelper metadataHelper, ShardQueryConfiguration config);
}
