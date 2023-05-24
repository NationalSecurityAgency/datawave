package datawave.webservice.query.service.config;

import java.io.Serializable;

/**
 * Provides an indexing specific configuration that can be used across executions.
 *
 * Likely simpler with lombok.
 */
public class IndexingConfiguration implements Serializable {
    
    // options
    boolean enableIndexInfoUidToDayIntersectionBypass = true;
    boolean enableRangeScannerLimitDays = false;
    
    private static IndexingConfiguration instance = new IndexingConfiguration();
    
    public boolean isEnableIndexInfoUidToDayIntersectionBypass() {
        return enableIndexInfoUidToDayIntersectionBypass;
    }
    
    public void setEnableIndexInfoUidToDayIntersectionBypass(boolean enableIndexInfoUidToDayIntersectionBypass) {
        this.enableIndexInfoUidToDayIntersectionBypass = enableIndexInfoUidToDayIntersectionBypass;
    }
    
    public boolean isEnableRangeScannerLimitDays() {
        return enableRangeScannerLimitDays;
    }
    
    public void setEnableRangeScannerLimitDays(boolean enableRangeScannerLimitDays) {
        this.enableRangeScannerLimitDays = enableRangeScannerLimitDays;
    }
    
    public static IndexingConfiguration getDefaultInstance() {
        return instance;
    }
    
}
