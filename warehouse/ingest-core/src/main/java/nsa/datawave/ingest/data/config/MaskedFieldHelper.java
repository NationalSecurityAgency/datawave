package nsa.datawave.ingest.data.config;

import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

import nsa.datawave.ingest.data.RawRecordContainer;

public interface MaskedFieldHelper {
    
    /**
     * Configure this helper before it's first use
     * 
     * @param config
     *            Hadoop configuration object
     */
    void setup(Configuration config);
    
    /**
     * @return Map of field names to masked values.
     */
    Map<String,String> getMaskedValues();
    
    /**
     * Returns a list of unmasked values
     *
     * @return unmodifiable set of unmasked values
     */
    Set<String> getUnmaskedValues();
}
