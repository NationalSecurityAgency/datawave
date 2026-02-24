package datawave.webservice.annotation;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.query.config.annotation.AnnotationConfig;
import datawave.query.tables.ShardQueryLogic;

public class AnnotationManagerConfig {
    private AnnotationConfig annotationConfig;
    private String connPoolName;
    private boolean enableInternalIdLookup = false;
    private ShardQueryLogic lookupUUIDQueryLogic;
    private LookupUUIDServiceConfig lookupUUIDServiceConfig;
    private AccumuloConnectionFactory.Priority priority = AccumuloConnectionFactory.Priority.LOW;

    public String getConnPoolName() {
        return connPoolName;
    }

    public void setConnPoolName(String connPoolName) {
        this.connPoolName = connPoolName;
    }

    public ShardQueryLogic getLookupUUIDQueryLogic() {
        return lookupUUIDQueryLogic;
    }

    public void setLookupUUIDQueryLogic(ShardQueryLogic lookupUUIDQueryLogic) {
        this.lookupUUIDQueryLogic = lookupUUIDQueryLogic;
    }

    public LookupUUIDServiceConfig getLookupUUIDServiceConfig() {
        return lookupUUIDServiceConfig;
    }

    public void setLookupUUIDServiceConfig(LookupUUIDServiceConfig lookupUUIDServiceConfig) {
        this.lookupUUIDServiceConfig = lookupUUIDServiceConfig;
    }

    public AccumuloConnectionFactory.Priority getPriority() {
        return priority;
    }

    public void setPriority(AccumuloConnectionFactory.Priority priority) {
        this.priority = priority;
    }

    public boolean isEnableInternalIdLookup() {
        return enableInternalIdLookup;
    }

    public void setEnableInternalIdLookup(boolean enableInternalIdLookup) {
        this.enableInternalIdLookup = enableInternalIdLookup;
    }

    public AnnotationConfig getAnnotationConfig() {
        return annotationConfig;
    }

    public void setAnnotationConfig(AnnotationConfig annotationConfig) {
        this.annotationConfig = annotationConfig;
    }
}
