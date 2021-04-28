package datawave.microservice.query.configuration;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.util.Map;

public interface ResultContext {
    /**
     * Set the last result returned. Setting a result of null denotes this scan is finished.
     * 
     * @param result
     */
    void setLastResult(Map.Entry<Key,Value> result);
    
    boolean isFinished();
}
