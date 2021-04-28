package datawave.microservice.query.logic.config;

import datawave.query.data.UUIDType;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

@ConfigurationProperties(prefix = "datawave.query.parser")
public class QueryParserProperties {
    private List<String> skipTokenizeUnfieldedFields;
    private List<String> tokenizedFields;
    private List<UUIDType> uuidTypes = new ArrayList<>();
    
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
    
    public List<UUIDType> getUuidTypes() {
        return uuidTypes;
    }
    
    public void setUuidTypes(List<UUIDType> uuidTypes) {
        this.uuidTypes = uuidTypes;
    }
}
