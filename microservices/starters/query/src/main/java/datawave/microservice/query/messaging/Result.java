package datawave.microservice.query.messaging;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;

public class Result {
    
    private final String id;
    private Object payload;
    
    private ClaimCheckCallback claimCheckCallback = null;
    private AcknowledgementCallback acknowledgementCallback = null;
    
    public Result(@JsonProperty("id") String id, @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS) @JsonProperty("payload") Object payload) {
        this.id = id;
        this.payload = payload;
    }
    
    public void setClaimCheckCallback(ClaimCheckCallback claimCheckCallback) {
        this.claimCheckCallback = claimCheckCallback;
    }
    
    public void setAcknowledgementCallback(AcknowledgementCallback acknowledgementCallback) {
        this.acknowledgementCallback = acknowledgementCallback;
    }
    
    public String getId() {
        return id;
    }
    
    public Object getPayload() throws InterruptedException, JsonProcessingException {
        // if the payload is checked, claim it
        if (payload == null && claimCheckCallback != null) {
            payload = claimCheckCallback.getPayload();
        }
        return payload;
    }
    
    public void acknowledge(AcknowledgementCallback.Status status) {
        if (acknowledgementCallback != null) {
            acknowledgementCallback.acknowledge(status);
        }
    }
}
