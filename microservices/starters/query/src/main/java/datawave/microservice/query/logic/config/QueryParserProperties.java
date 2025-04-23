package datawave.microservice.query.logic.config;

import java.util.List;
import java.util.Set;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "datawave.query.parser")
public class QueryParserProperties {
    private List<String> skipTokenizeUnfieldedFields;
    private List<String> tokenizedFields;
    private Set<String> tokenizerStopwords;
    
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
    
    public Set<String> getTokenizerStopwords() {
        return tokenizerStopwords;
    }
    
    public void setTokenizerStopwords(Set<String> tokenizerStopwords) {
        this.tokenizerStopwords = tokenizerStopwords;
    }
}
