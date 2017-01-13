package nsa.datawave.query.rewrite.planner.pushdown;

import java.util.Collection;

import org.apache.commons.jexl2.parser.ASTJexlScript;

import nsa.datawave.query.rewrite.config.RefactoredShardQueryConfiguration;
import nsa.datawave.query.rewrite.planner.pushdown.rules.PushDownRule;
import nsa.datawave.query.tables.ScannerFactory;
import nsa.datawave.query.util.MetadataHelper;

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
    ASTJexlScript applyRules(ASTJexlScript queryTree, ScannerFactory scannerFactory, MetadataHelper metadataHelper, RefactoredShardQueryConfiguration config);
}
