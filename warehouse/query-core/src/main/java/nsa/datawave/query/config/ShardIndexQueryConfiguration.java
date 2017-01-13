package nsa.datawave.query.config;

import java.util.Collections;
import java.util.List;

import nsa.datawave.query.tables.shard.ShardIndexQueryTable;

/**
 * Thin wrapper around the {@link ShardQueryConfiguration} for use by the {@link ShardIndexQueryTable}
 *
 * 
 *
 */
@Deprecated
public class ShardIndexQueryConfiguration extends GenericShardQueryConfiguration {
    private static final long serialVersionUID = 7616552164239289739L;
    
    private List<String> normalizedTerms = Collections.emptyList();
    private List<String> normalizedPatterns = Collections.emptyList();
    
    public ShardIndexQueryConfiguration(ShardIndexQueryTable logic) {
        this.setIndexTableName(logic.getIndexTableName());
        this.setFullTableScanEnabled(logic.isFullTableScanEnabled());
    }
    
    public void setNormalizedTerms(List<String> normalizedTerms) {
        this.normalizedTerms = normalizedTerms;
    }
    
    public List<String> getNormalizedTerms() {
        return normalizedTerms;
    }
    
    public List<String> getNormalizedPatterns() {
        return normalizedPatterns;
    }
    
    public void setNormalizedPatterns(List<String> normalizedPatterns) {
        this.normalizedPatterns = normalizedPatterns;
    }
}
