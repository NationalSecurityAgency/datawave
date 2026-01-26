package datawave.query.transformer.annotation.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * AllHits that includes an additional errorMessage field to communicate problems
 */
public class AllHitsError extends AllHits {
    @JsonProperty
    private String errorMessage;

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
}
