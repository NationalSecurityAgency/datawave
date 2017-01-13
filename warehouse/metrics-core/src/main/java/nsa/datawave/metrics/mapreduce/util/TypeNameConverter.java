package nsa.datawave.metrics.mapreduce.util;

import java.util.Map;

/**
 * Sometimes, the type names used in the poller and the ingest process do not match up. This class is intended to provide a simple, centralized view of the
 * mappings between poller and ingest type names.
 * 
 */
public class TypeNameConverter {
    
    private Map<String,String> pollerToIngest = null;
    private Map<String,String> ingestToPoller = null;
    
    /**
     * If no match found, normalizes type to upper case
     * 
     * @param type
     * @return
     */
    public String convertPollerToIngest(String type) {
        if (null != pollerToIngest) {
            if (pollerToIngest.containsKey(type)) {
                return pollerToIngest.get(type);
            }
        }
        return type.toUpperCase();
    }
    
    public String convertIngestToPoller(String type) {
        if (null != ingestToPoller) {
            if (ingestToPoller.containsKey(type)) {
                return ingestToPoller.get(type);
            }
        }
        return type;
    }
    
    public void setPollerToIngestMap(Map<String,String> pollerToIngestMap) {
        this.pollerToIngest = pollerToIngestMap;
    }
    
    public void setIngestToPollerMap(Map<String,String> ingestToPollerMap) {
        this.ingestToPoller = ingestToPollerMap;
    }
}
