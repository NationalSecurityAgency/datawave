package datawave.microservice.common.storage;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import java.util.Arrays;

public class Result {
    private String payloadType;
    private String[] payload;
    private String id;
    
    public Result(@JsonProperty("resultId") String id, @JsonProperty("payload") String[] payload, @JsonProperty("payloadType") String payloadType) {
        this.id = id;
        this.payloadType = payloadType;
        this.payload = payload;
    }
    
    public Result(String id, Object[] payload) throws Exception {
        this(id, Arrays.stream(payload).map(p -> {
            try {
                return new ObjectMapper().writeValueAsString(p);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Unable to serialize result");
            }
        }).toArray(String[]::new), payload[0].getClass().getName());
    }
    
    public String getResultId() {
        return id;
    }
    
    public String getPayloadType() {
        return payloadType;
    }
    
    public String[] getPayload() {
        return payload;
    }
    
    @JsonIgnore
    public Object[] getPayloadObject() throws ClassNotFoundException, JsonProcessingException {
        final ObjectReader reader = new ObjectMapper().readerFor(Class.forName(payloadType));
        return Arrays.stream(payload).map(p -> {
            try {
                return reader.readValue(p);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Unable to serialize result");
            }
        }).toArray();
    }
}
