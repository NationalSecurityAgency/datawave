package datawave.webservice.websocket.messages;

/**
 * A query message for clients to send to indicate the query on this websocket should be cancelled. The client should send a JSON message with a single property
 * "cancel" set to any simple value. For example,
 *
 * <pre>
 * <code>
 * { "cancel": true }
 * </code>
 * </pre>
 */
public class CancelMessage implements QueryMessage {
    @Override
    public Type getType() {
        return Type.CANCEL;
    }
}
