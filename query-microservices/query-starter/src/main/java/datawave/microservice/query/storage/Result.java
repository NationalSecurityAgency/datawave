package datawave.microservice.query.storage;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.springframework.integration.acks.SimpleAcknowledgment;

public class Result {
    private final String payloadType;
    private final Object[] payload;
    private final String id;
    private SimpleAcknowledgment acknowledgement = null;
    
    public Result(@JsonProperty("resultId") String id, @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
                    property = "@class") @JsonProperty("payload") Object[] payload, @JsonProperty("payloadType") String payloadType) {
        this.id = id;
        this.payloadType = payloadType;
        this.payload = payload;
    }
    
    public Result(String id, Object[] payload) throws Exception {
        this(id, payload, payload[0].getClass().getName());
    }
    
    public void setAcknowledgement(SimpleAcknowledgment acknowledgement) {
        this.acknowledgement = acknowledgement;
    }
    
    public String getResultId() {
        return id;
    }
    
    public String getPayloadType() {
        return payloadType;
    }
    
    public Object[] getPayload() {
        if (acknowledgement != null) {
            acknowledgement.acknowledge();
        }
        return payload;
    }
}
