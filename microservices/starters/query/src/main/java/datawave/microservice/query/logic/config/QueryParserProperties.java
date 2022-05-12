package datawave.microservice.query.logic.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties(prefix = "datawave.query.parser")
public class QueryParserProperties {
    private List<String> skipTokenizeUnfieldedFields;
    private List<String> tokenizedFields;
    
    public List<String> getSkipTokenizeUnfieldedFields() {
        return skipTokenizeUnfieldedFields;
    }
    
    public void setSkipTokenizeUnfieldedFields(List<String> skipTokenizeUnfieldedFields) {
        this.skipTokenizeUnfieldedFields = skipTokenizeUnfieldedFields;
    }
    
    public List<String> getTokenizedFields() {
        return tokenizedFields;
    }
    
    public void setTokenizedFields(List<String> tokenizedFields) {
        this.tokenizedFields = tokenizedFields;
    }
}
