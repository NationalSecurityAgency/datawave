package org.springframework.cloud.bus.event;

/**
 * A {@link RemoteApplicationEvent} to indicate that users should be evicted from the authorization cache.
 */
public class AuthorizationEvictionEvent extends RemoteApplicationEvent {
    public enum Type {
        FULL, PARTIAL, USER
    }
    
    private final Type evictionType;
    private final String substring;
    
    @SuppressWarnings("unused")
    public AuthorizationEvictionEvent() {
        // this constructor is only for serialization/deserialization
        evictionType = Type.FULL;
        substring = null;
    }
    
    public AuthorizationEvictionEvent(Object source, String originService, Type evictionType, String substring) {
        this(source, originService, null, evictionType, substring);
    }
    
    public AuthorizationEvictionEvent(Object source, String originService, String destinationService, Type evictionType, String substring) {
        super(source, originService, destinationService);
        this.evictionType = evictionType;
        this.substring = substring;
    }
    
    public Type getEvictionType() {
        return evictionType;
    }
    
    public String getSubstring() {
        return substring;
    }
}
