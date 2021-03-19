package datawave.microservice.query.config;

import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;
import java.util.Map;

@Validated
public class QueryProperties {
    
    @Valid
    private Map<String,QueryLogicProperties> logic;
    
    @Valid
    private QueryExpirationProperties expiration;
    
    public Map<String,QueryLogicProperties> getLogic() {
        return logic;
    }
    
    public void setLogic(Map<String,QueryLogicProperties> logic) {
        this.logic = logic;
    }
    
    public QueryExpirationProperties getExpiration() {
        return expiration;
    }
    
    public void setExpiration(QueryExpirationProperties expiration) {
        this.expiration = expiration;
    }
}
