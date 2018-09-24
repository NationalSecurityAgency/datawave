package datawave.query.testframework;

import org.apache.accumulo.core.security.ColumnVisibility;

import java.util.List;
import java.util.Map;

/**
 * POJO for data related to test classes execution.
 */
public class ConfigData {
    
    private final String dateField;
    private final String eventField;
    private final List<String> headers;
    private final ColumnVisibility defaultVisibility;
    private final Map<String,RawMetaData> metadata;
    
    /**
     * Creates a POJO datatype configuration, without a default column visibility.
     *
     * @param dateFieldName
     *            name of the date field
     * @param eventName
     *            name of the event field
     * @param headerValues
     *            list of header field names
     * @param metadata
     *            mapping of header field to metadata
     */
    public ConfigData(final String dateFieldName, final String eventName, final List<String> headerValues, final Map<String,RawMetaData> metadata) {
        this(dateFieldName, eventName, headerValues, null, metadata);
    }
    
    /**
     * Creates a POJO datatype configuration.
     *
     * @param dateFieldName
     *            name of the date field
     * @param eventName
     *            name of the event field
     * @param headerValues
     *            list of header field names
     * @param visibility
     *            default column visibility
     * @param metadata
     *            mapping of header field to metadata
     */
    public ConfigData(final String dateFieldName, final String eventName, final List<String> headerValues, final ColumnVisibility visibility,
                    final Map<String,RawMetaData> metadata) {
        this.eventField = eventName;
        this.dateField = dateFieldName;
        this.headers = headerValues;
        this.metadata = metadata;
        this.defaultVisibility = visibility;
    }
    
    public String getDateField() {
        return this.dateField;
    }
    
    public String getEventId() {
        return this.eventField;
    }
    
    public List<String> headers() {
        return this.headers;
    }
    
    public ColumnVisibility getDefaultVisibility() {
        return defaultVisibility;
    }
    
    Map<String,RawMetaData> getMetadata() {
        return this.metadata;
    }
}
