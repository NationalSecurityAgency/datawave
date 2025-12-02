package datawave.webservice.annotation;

import datawave.annotation.data.transform.TimestampTransformer;
import datawave.annotation.data.transform.VisibilityTransformer;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.query.tables.ShardQueryLogic;

public class AnnotationManagerConfig {
    private String annotationTableName;
    private String annotationSourceTableName;
    private String connPoolName;
    private boolean enableInternalIdLookup = false;
    private ShardQueryLogic lookupUUIDQueryLogic;
    private LookupUUIDServiceConfig lookupUUIDServiceConfig;
    private AccumuloConnectionFactory.Priority priority = AccumuloConnectionFactory.Priority.LOW;
    private VisibilityTransformer visibilityTransformer;
    private TimestampTransformer timestampTransformer;

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

    public String getAnnotationTableName() {
        return annotationTableName;
    }

    public void setAnnotationTableName(String annotationTableName) {
        this.annotationTableName = annotationTableName;
    }

    public String getAnnotationSourceTableName() {
        return annotationSourceTableName;
    }

    public void setAnnotationSourceTableName(String annotationSourceTableName) {
        this.annotationSourceTableName = annotationSourceTableName;
    }

    public TimestampTransformer getTimestampTransformer() {
        return timestampTransformer;
    }

    public void setTimestampTransformer(TimestampTransformer timestampTransformer) {
        this.timestampTransformer = timestampTransformer;
    }

    public VisibilityTransformer getVisibilityTransformer() {
        return visibilityTransformer;
    }

    public void setVisibilityTransformer(VisibilityTransformer visibilityTransformer) {
        this.visibilityTransformer = visibilityTransformer;
    }

    public boolean isEnableInternalIdLookup() {
        return enableInternalIdLookup;
    }

    public void setEnableInternalIdLookup(boolean enableInternalIdLookup) {
        this.enableInternalIdLookup = enableInternalIdLookup;
    }
}
