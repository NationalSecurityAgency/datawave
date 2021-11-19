package datawave.microservice.query.messaging;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

public class Result {
    private final Object payload;
    private final String id;
    private AcknowledgementCallback acknowledgementCallback = null;
    
    public Result(@JsonProperty("resultId") String id, @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
                    property = "@class") @JsonProperty("payload") Object payload) {
        this.id = id;
        this.payload = payload;
    }
    
    public void setAcknowledgementCallback(AcknowledgementCallback acknowledgementCallback) {
        this.acknowledgementCallback = acknowledgementCallback;
    }
    
    public String getResultId() {
        return id;
    }
    
    public Object getPayload() {
        return payload;
    }
    
    @JsonIgnore
    public void acknowledge(AcknowledgementCallback.Status status) {
        acknowledgementCallback.acknowledge(status);
    }
}
