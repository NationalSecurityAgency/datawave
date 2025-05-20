package datawave.query.transformer;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.google.common.collect.Multimap;

import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.result.event.ResponseObjectFactory;

public class EventQueryDataDecorator {
    private String fieldName = null;
    private Map<String,String> patternMap = new LinkedHashMap<>();
    private Logger log = Logger.getLogger(EventQueryDataDecorator.class);
    private ResponseObjectFactory responseObjectFactory;

    public EventQueryDataDecorator() {}

    public void decorateData(Multimap<String,FieldBase> data) {
        // Get the values for the FieldName to put the decorated data into
        Collection<FieldBase> destinationOfData = data.get(fieldName);

        // Loop over all decorator patterns
        for (Map.Entry<String,String> entry : this.patternMap.entrySet()) {
            // Find fieldNames which match the current pattern
            Collection<FieldBase> collectionSourceOfData = data.get(entry.getKey());
            if (collectionSourceOfData != null && !collectionSourceOfData.isEmpty()) {

                // multiple value source fields for the substitution value not supported -- use the first one
                Iterator<FieldBase> collectionSourceItr = collectionSourceOfData.iterator();
                FieldBase sourceOfData = collectionSourceItr.next();
                Map<String,String> markings = sourceOfData.getMarkings();

                String id = sourceOfData.getValueString();
                String newValue = entry.getValue().replace("@field_value@", id);

                // If we have no values for the decorated data's field name
                if (destinationOfData == null || destinationOfData.size() < 1) {
                    // Add the result
                    data.put(fieldName, this.makeField(fieldName, sourceOfData.getMarkings(), sourceOfData.getColumnVisibility(), sourceOfData.getTimestamp(),
                                    newValue));
                } else {
                    // Otherwise, find the original data
                    for (FieldBase dest : destinationOfData) {
                        // Update that Value's value (destinationOfData changes the underlying Multimap)
                        dest.setValue(newValue);
                    }
                }

                // attempt to use a multiple value source field
                if (collectionSourceItr.hasNext()) {
                    log.info("EventQueryDataDecorator configured to use a source field: " + entry.getKey() + " with multiple values -- using the first");
                }

                break;
            }
        }
    }

    private FieldBase makeField(String name, Map<String,String> markings, String columnVisibility, Long timestamp, Object value) {
        FieldBase field = this.responseObjectFactory.getField();
        field.setName(name);
        field.setMarkings(markings);
        field.setColumnVisibility(columnVisibility);
        field.setTimestamp(timestamp);
        field.setValue(value);
        return field;
    }

    public Map<String,String> getPatternMap() {
        return patternMap;
    }

    public void setPatternMap(Map<String,String> patternMap) {
        this.patternMap.clear();
        this.patternMap.putAll(patternMap);
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public ResponseObjectFactory getResponseObjectFactory() {
        return responseObjectFactory;
    }

    public void setResponseObjectFactory(ResponseObjectFactory responseObjectFactory) {
        this.responseObjectFactory = responseObjectFactory;
    }

}
