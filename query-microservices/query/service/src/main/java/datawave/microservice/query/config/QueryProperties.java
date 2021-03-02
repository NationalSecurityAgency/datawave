package datawave.microservice.query.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;
import java.util.Map;

// TODO: This may be a common property file used by multiple services, so we will eventually need to move it out
@Validated
@EnableConfigurationProperties(QueryProperties.class)
@ConfigurationProperties(prefix = "query")
public class QueryProperties {
    
    @Valid
    private Map<String,QueryLogicConfig> logic;
    
    public Map<String,QueryLogicConfig> getLogic() {
        return logic;
    }
    
    public void setLogic(Map<String,QueryLogicConfig> logic) {
        this.logic = logic;
    }
}
