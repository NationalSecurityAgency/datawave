package datawave.microservice.query.configuration;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.util.Map;

public interface ResultContext {
    void setLastResult(Map.Entry<Key,Value> result);
}
