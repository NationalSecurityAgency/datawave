package datawave.query.planner.rules;

import org.apache.commons.jexl3.parser.JexlNode;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.util.MetadataHelper;

public interface NodeTransformRule {
    /**
     * Basically every planning rule will get applied to every node. If nothing is to be modified, then simply return the passed in node. If something is to be
     * done, then rebuild the node with modifications and return. Returning null will remove this node from the tree altogether.
     *
     * @param node
     *            a node
     * @param config
     *            the config
     * @param helper
     *            the metadata helper
     * @return the replacement node
     */
    default JexlNode apply(JexlNode node, ShardQueryConfiguration config, MetadataHelper helper) {
        return node;
    }
}
