package datawave.query.planner;

import org.apache.accumulo.core.client.IteratorSetting;

import datawave.query.config.ShardQueryConfiguration;

/**
 * SeekingQueryPlanner sets additional QueryIterator settings to support the SeekingFilter
 */
public class SeekingQueryPlanner extends DefaultQueryPlanner {
    public static final String MAX_FIELD_HITS_BEFORE_SEEK = "SeekingFilter.maxFieldHitsBeforeSeek";
    public static final String MAX_KEYS_BEFORE_SEEK = "SeekingFilter.maxKeysBeforeSeek";
    public static final String MAX_KEYS_BEFORE_DATATYPE_SEEK = "SeekingFilter.maxKeysBeforeDataTypeSeek";

    // default to disabled
    private int maxFieldsBeforeSeek = -1;
    private int maxKeysBeforeSeek = -1;
    private int maxKeysBeforeDataTypeSeek = -1;

    @Override
    protected void configureAdditionalOptions(ShardQueryConfiguration config, IteratorSetting cfg) {
        addOption(cfg, MAX_FIELD_HITS_BEFORE_SEEK, Integer.toString(maxFieldsBeforeSeek), false);
        addOption(cfg, MAX_KEYS_BEFORE_SEEK, Integer.toString(maxKeysBeforeSeek), false);
        addOption(cfg, MAX_KEYS_BEFORE_DATATYPE_SEEK, Integer.toString(maxKeysBeforeDataTypeSeek), false);
    }

    @Override
    public DefaultQueryPlanner clone() {
        SeekingQueryPlanner clone = new SeekingQueryPlanner();
        clone.setMaxFieldHitsBeforeSeek(maxFieldsBeforeSeek);
        clone.setMaxKeysBeforeSeek(maxKeysBeforeSeek);
        clone.setMaxKeysBeforeDataTypeSeek(maxKeysBeforeDataTypeSeek);

        return clone;
    }

    public void setMaxFieldHitsBeforeSeek(int maxFieldHitsBeforeSeek) {
        this.maxFieldsBeforeSeek = maxFieldHitsBeforeSeek;
    }

    public void setMaxKeysBeforeSeek(int maxKeysBeforeSeek) {
        this.maxKeysBeforeSeek = maxKeysBeforeSeek;
    }

    public void setMaxKeysBeforeDataTypeSeek(int maxKeysBeforeDataTypeSeek) {
        this.maxKeysBeforeDataTypeSeek = maxKeysBeforeDataTypeSeek;
    }
}
