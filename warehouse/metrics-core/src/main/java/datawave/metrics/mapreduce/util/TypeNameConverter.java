package datawave.metrics.mapreduce.util;

import java.util.Map;

/**
 * Sometimes, the type names used in the transform processing and the ingest processing do not match up. This class is intended to provide a simple, centralized
 * view of the mappings between transformer and ingest type names.
 * 
 */
public class TypeNameConverter {
    
    private Map<String,String> rawFileTransformerToIngest = null;
    private Map<String,String> ingestToRawFileTransformer = null;
    
    /**
     * If no match found, normalizes type to upper case
     * 
     * @param type
     *            a type
     * @return the type string
     */
    public String convertRawFileTransformerToIngest(String type) {
        if (null != rawFileTransformerToIngest) {
            if (rawFileTransformerToIngest.containsKey(type)) {
                return rawFileTransformerToIngest.get(type);
            }
        }
        return type.toUpperCase();
    }
    
    public String convertIngestToRawFileTransformer(String type) {
        if (null != ingestToRawFileTransformer) {
            if (ingestToRawFileTransformer.containsKey(type)) {
                return ingestToRawFileTransformer.get(type);
            }
        }
        return type;
    }
    
    public void setRawFileTransformerToIngestMap(Map<String,String> transformerToIngestMap) {
        this.rawFileTransformerToIngest = transformerToIngestMap;
    }
    
    public void setIngestToRawFileTransformerMap(Map<String,String> ingestToTransformerMap) {
        this.ingestToRawFileTransformer = ingestToTransformerMap;
    }
}
