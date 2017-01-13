package nsa.datawave.query.config;

import nsa.datawave.query.tables.shard.ShardQueryTable;
import nsa.datawave.webservice.query.Query;

/**
 * Thin wrapper around the GenericShardQueryConfiguration class for use by the ShardQueryTable
 *
 * 
 *
 */
@Deprecated
public class ShardQueryConfiguration extends GenericShardQueryConfiguration {
    /**
     *
     */
    private static final long serialVersionUID = -4354990715046146110L;
    
    public ShardQueryConfiguration() {
        super();
    }
    
    public ShardQueryConfiguration(ShardQueryTable configuredLogic, Query query) {
        super(configuredLogic, query);
    }
}
