package datawave.query.jexl.lookups;

import datawave.core.query.jexl.lookups.IndexLookupMap;
import datawave.query.config.ShardQueryConfiguration;

/**
 * An index lookup which does no work and returns an empty IndexLookupMap
 */
public class EmptyIndexLookup extends IndexLookup {

    /**
     *
     * @param config
     *            the shard query configuration, not null
     */
    public EmptyIndexLookup(ShardQueryConfiguration config) {
        super(config, null);
    }

    @Override
    public IndexLookupMap lookup() {
        return new IndexLookupMap(config.getMaxUnfieldedExpansionThreshold(), config.getMaxValueExpansionThreshold());
    }
}
