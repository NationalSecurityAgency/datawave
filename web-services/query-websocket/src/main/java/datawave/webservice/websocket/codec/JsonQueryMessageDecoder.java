package datawave.webservice.websocket.codec;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import datawave.webservice.websocket.messages.CancelMessage;
import datawave.webservice.websocket.messages.QueryMessage;

import javax.websocket.DecodeException;
import javax.websocket.Decoder;
import javax.websocket.EndpointConfig;

/**
 * Decodes incoming JSON text into a {@link datawave.webservice.websocket.messages.QueryMessage}. Based on the message content, the returned object will be one
 * of the known types of query messages. //
 */
public class JsonQueryMessageDecoder implements Decoder.Text<QueryMessage> {
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void init(EndpointConfig config) {}

    @Override
    public void destroy() {}

    @Override
    public QueryMessage decode(String jsonMessage) throws DecodeException {
        QueryMessage message = null;
        JsonNode jsonNode;
        try {
            jsonNode = objectMapper.readTree(jsonMessage);
        } catch (JsonProcessingException e) {
            throw new DecodeException("", e.getMessage(), e.getCause());
        }

        QueryMessage.Type type = null;
        if (jsonNode != null) {
            try {
                type = QueryMessage.Type.valueOf(jsonNode.get("action").asText().toUpperCase());
            } catch (IllegalArgumentException e) {
                // The only way to get here is if a client has sent valid json with an "action" specified that we don't have a
                // message object for. Return null here so that the websocket can send a sensible message can be returned to the client.
                return null;
            }
        }

        if (type != null) {
            switch (type) {
                case CANCEL: {
                    try {
                        message = objectMapper.readValue(jsonMessage, CancelMessage.class);
                    } catch (JsonProcessingException e) {
                        throw new DecodeException("", e.getMessage(), e.getCause());
                    }
                }
                break;
                case CREATE: {
                    try {
                        message = objectMapper.readValue(jsonMessage, QueryMessage.class);
                    } catch (JsonProcessingException e) {
                        throw new DecodeException("", e.getMessage(), e.getCause());
                    }
                }
            }
        }

        return message;
    }

    @Override
    public boolean willDecode(String jsonMessage) {
        // See if it's valid JSON. If so, then we indicate we can read it.
        try {
            objectMapper.readTree(jsonMessage);
            return true;
        } catch (JsonProcessingException e) {
            return false;
        }
    }

}