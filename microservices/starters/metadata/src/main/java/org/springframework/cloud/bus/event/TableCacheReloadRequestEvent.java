package org.springframework.cloud.bus.event;

public class TableCacheReloadRequestEvent extends RemoteApplicationEvent {
    
    private final String tableName;
    
    @SuppressWarnings("unused")
    public TableCacheReloadRequestEvent() {
        // this constructor is only for serialization/deserialization
        tableName = null;
    }
    
    public TableCacheReloadRequestEvent(Object source, String originService, String tableName) {
        this(source, originService, null, tableName);
    }
    
    public TableCacheReloadRequestEvent(Object source, String originService, String destinationService, String tableName) {
        super(source, originService, DEFAULT_DESTINATION_FACTORY.getDestination(destinationService));
        this.tableName = tableName;
    }
    
    public String getTableName() {
        return tableName;
    }
}
