package datawave.microservice.query.edge;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.Map;

@Validated
@ConfigurationProperties(prefix = "datawave.query.edge.model")
public class EdgeModelProperties {
    @NotNull
    private Map<String,String> baseFieldMap;
    @NotNull
    private Map<String,String> keyUtilFieldMap;
    @NotNull
    private Map<String,String> transformFieldMap;
    
    public Map<String,String> getBaseFieldMap() {
        return baseFieldMap;
    }
    
    public void setBaseFieldMap(Map<String,String> baseFieldMap) {
        this.baseFieldMap = baseFieldMap;
    }
    
    public Map<String,String> getKeyUtilFieldMap() {
        return keyUtilFieldMap;
    }
    
    public void setKeyUtilFieldMap(Map<String,String> keyUtilFieldMap) {
        this.keyUtilFieldMap = keyUtilFieldMap;
    }
    
    public Map<String,String> getTransformFieldMap() {
        return transformFieldMap;
    }
    
    public void setTransformFieldMap(Map<String,String> transformFieldMap) {
        this.transformFieldMap = transformFieldMap;
    }
}
