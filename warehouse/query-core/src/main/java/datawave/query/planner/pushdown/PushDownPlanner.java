package datawave.query.planner.pushdown;

import java.util.Collection;

import datawave.query.config.ShardQueryConfiguration;
import org.apache.commons.jexl3.parser.ASTJexlScript;

import datawave.query.planner.pushdown.rules.PushDownRule;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.MetadataHelper;

/**
 *
 */
public interface PushDownPlanner {

    void setRules(Collection<PushDownRule> rules);

    Collection<PushDownRule> getRules();

    /**
     * Returns a re-written tree that reflects pushed down Predicates
     *
     * @param queryTree
     *            the query tree
     * @param scannerFactory
     *            the scanner factory
     * @param metadataHelper
     *            the metadata helper
     * @param config
     *            the shard configuration
     * @return an adjusted query tree
     */
    ASTJexlScript applyRules(ASTJexlScript queryTree, ScannerFactory scannerFactory, MetadataHelper metadataHelper, ShardQueryConfiguration config);
}
