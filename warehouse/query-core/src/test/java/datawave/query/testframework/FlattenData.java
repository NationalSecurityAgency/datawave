package datawave.query.testframework;

import datawave.ingest.json.util.JsonObjectFlattener;

import java.util.List;
import java.util.Map;

/**
 * POJO for data related to test classes that use a JSON cityFlatten.
 */
public class FlattenData extends ConfigData {
    
    private final JsonObjectFlattener.FlattenMode mode;
    
    /**
     * Creates a POJO of flatten data.
     * 
     * @param dateFieldName
     *            name of the date field
     * @param eventName
     *            name of the event field
     * @param flattenMode
     *            flatten mode action
     * @param headerValues
     *            list of header field names
     * @param metadata
     *            mapping of header field to metadata
     */
    public FlattenData(final String dateFieldName, final String eventName, final JsonObjectFlattener.FlattenMode flattenMode, final List<String> headerValues,
                    final Map<String,RawMetaData> metadata) {
        super(dateFieldName, eventName, headerValues, metadata);
        this.mode = flattenMode;
    }
    
    public JsonObjectFlattener.FlattenMode getMode() {
        return mode;
    }
}
