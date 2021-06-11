package datawave.microservice.query.storage;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

public class Result {
    private String payloadType;
    private Object[] payload;
    private String id;
    
    public Result(@JsonProperty("resultId") String id, @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
                    property = "@class") @JsonProperty("payload") Object[] payload, @JsonProperty("payloadType") String payloadType) {
        this.id = id;
        this.payloadType = payloadType;
        this.payload = payload;
    }
    
    public Result(String id, Object[] payload) throws Exception {
        this(id, payload, payload[0].getClass().getName());
    }
    
    public String getResultId() {
        return id;
    }
    
    public String getPayloadType() {
        return payloadType;
    }
    
    public Object[] getPayload() {
        return payload;
    }
}
