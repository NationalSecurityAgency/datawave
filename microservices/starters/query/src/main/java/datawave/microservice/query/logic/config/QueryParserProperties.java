package datawave.microservice.query.logic.config;

import java.util.List;
import java.util.Set;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "datawave.query.parser")
public class QueryParserProperties {
    private List<String> skipTokenizeUnfieldedFields;
    private List<String> tokenizedFields;
    private Set<String> tokenizerStopwords;
    private List<String> allowedFunctions;
    private List<String> disallowedFunctions;

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

    public List<String> getAllowedFunctions() {
        return allowedFunctions;
    }

    public void setAllowedFunctions(List<String> allowedFunctions) {
        this.allowedFunctions = allowedFunctions;
    }

    public List<String> getDisallowedFunctions() {
        return disallowedFunctions;
    }

    public void setDisallowedFunctions(List<String> disallowedFunctions) {
        this.disallowedFunctions = disallowedFunctions;
    }
}
