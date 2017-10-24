package datawave.query.planner;

import datawave.query.config.ShardQueryConfiguration;
import org.apache.accumulo.core.client.IteratorSetting;

/**
 * SeekingQueryPlanner sets additional QueryIterator settings to support the SeekingFilter
 */
public class SeekingQueryPlanner extends DefaultQueryPlanner {
    public static final String MAX_FIELD_HITS_BEFORE_SEEK = "SeekingFilter.maxFieldHitsBeforeSeek";
    public static final String MAX_KEYS_BEFORE_SEEK = "SeekingFilter.maxKeysBeforeSeek";
    
    private int maxFieldsBeforeSeek;
    private int maxKeysBeforeSeek;
    
    @Override
    protected void configureAdditionalOptions(ShardQueryConfiguration config, IteratorSetting cfg) {
        addOption(cfg, MAX_FIELD_HITS_BEFORE_SEEK, Integer.toString(maxFieldsBeforeSeek), false);
        addOption(cfg, MAX_KEYS_BEFORE_SEEK, Integer.toString(maxKeysBeforeSeek), false);
    }
    
    public void setMaxFieldHitsBeforeSeek(int maxFieldHitsBeforeSeek) {
        this.maxFieldsBeforeSeek = maxFieldHitsBeforeSeek;
    }
    
    public void setMaxKeysBeforeSeek(int maxKeysBeforeSeek) {
        this.maxKeysBeforeSeek = maxKeysBeforeSeek;
    }
}
