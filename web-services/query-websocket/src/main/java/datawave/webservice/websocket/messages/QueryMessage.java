package datawave.webservice.websocket.messages;

/**
 *
 */
public interface QueryMessage {
    enum Type {
        CREATE, CANCEL
    }

    Type getType();
}
