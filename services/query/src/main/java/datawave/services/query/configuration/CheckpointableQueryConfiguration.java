package datawave.services.query.configuration;

import datawave.services.query.logic.QueryCheckpoint;
import datawave.services.query.logic.QueryKey;

import java.util.Collection;

public interface CheckpointableQueryConfiguration {
    
    QueryCheckpoint checkpoint(QueryKey queryKey, Collection<QueryData> ranges);
    
    QueryCheckpoint checkpoint(QueryKey queryKey);
    
}
