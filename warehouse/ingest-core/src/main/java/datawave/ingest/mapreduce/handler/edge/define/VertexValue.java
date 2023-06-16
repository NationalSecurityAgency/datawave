package datawave.ingest.mapreduce.handler.edge.define;

import datawave.ingest.data.config.NormalizedContentInterface;

import java.util.Map;

/**
 * Combines a VertexDefinition with value obtained from a {@link datawave.ingest.data.config.NormalizedContentInterface}.
 *
 */
public class VertexValue {

    public enum ValueType {
        INDEXED, EVENT
    };

    private String indexedFieldName = null;
    private String indexedFieldValue = null;
    private String eventFieldName = null;
    private String eventFieldValue = null;
    private String relationshipIndex = null;
    private boolean useRealm = false;
    private String indexedRealmLabel = null;
    private String eventRealmLabel = null;
    private String sourceIndex = null;
    private Map<String,String> markings = null;
    private String maskedValue = null;
    private boolean hasMaskedValue = false;
    private String relationshipType = null;
    private String collectionType = null;

    public VertexValue() {}

    public VertexValue(boolean useRealm, String indexedRealmLabel, String eventRealmLabel, String relationshipType, String collectionType,
                    NormalizedContentInterface nci) {

        this.relationshipType = relationshipType;
        this.collectionType = collectionType;
        this.indexedFieldName = nci.getIndexedFieldName();
        this.indexedFieldValue = nci.getIndexedFieldValue();
        this.eventFieldName = nci.getEventFieldName();
        this.eventFieldValue = nci.getEventFieldValue();
        this.markings = nci.getMarkings();
        this.useRealm = useRealm;
        if (this.useRealm && indexedRealmLabel != null && eventRealmLabel != null) {
            this.indexedRealmLabel = indexedRealmLabel;
            this.eventRealmLabel = eventRealmLabel;
        } else {
            this.useRealm = false;
        }
    }

    public Map<String,String> getMarkings() {
        return this.markings;
    }

    public String getValue(ValueType valueType) {

        String value = null;
        if (valueType.equals(ValueType.INDEXED)) {
            value = this.getIndexedFieldValue();
        } else {
            value = this.getEventFieldValue();
        }

        if (useRealm) {
            return getRealmedString(valueType, value);
        } else {
            return value;
        }
    }

    public String getMaskedValue(ValueType valueType) {
        if (!this.hasMaskedValue)
            return getValue(valueType);
        if (useRealm) {
            return getRealmedString(valueType, maskedValue);
        } else {
            return maskedValue;
        }
    }

    private String getRealmedString(ValueType valueType, String value) {
        if (valueType.equals(ValueType.INDEXED)) {
            return value + this.indexedRealmLabel;
        } else {
            return value + this.eventRealmLabel;
        }
    }

    public boolean hasMaskedValue() {
        return this.hasMaskedValue;
    }

    public void setHasMaskedValue(boolean masked) {
        this.hasMaskedValue = masked;
    }

    public void setFieldName(String indexedFieldName) {
        this.indexedFieldName = indexedFieldName;
    }

    public String getFieldName() {
        return indexedFieldName;
    }

    public String getRelationshipIndex() {
        return relationshipIndex;
    }

    public String getSourceIndex() {
        return sourceIndex;
    }

    public void setMaskedValue(String value) {
        this.maskedValue = value;
    }

    public String getIndexedFieldValue() {
        return indexedFieldValue;
    }

    public void setIndexedFieldValue(String fieldValue) {
        this.indexedFieldValue = fieldValue;
    }

    public String getEventFieldName() {
        return eventFieldName;
    }

    public void setEventFieldName(String eventFieldName) {
        this.eventFieldName = eventFieldName;
    }

    public String getEventFieldValue() {
        return eventFieldValue;
    }

    public void setEventFieldValue(String eventFieldValue) {
        this.eventFieldValue = eventFieldValue;
    }

    public String getMaskedValue() {
        return maskedValue;
    }

    public String getCollectionType() {
        return collectionType;
    }

    public String getRelationshipType() {
        return relationshipType;
    }

    public void setCollectionType(String collectionType) {
        this.collectionType = collectionType;
    }

    public void setRelationshipType(String relationshipType) {
        this.relationshipType = relationshipType;
    }

    @Override
    public String toString() {
        return "VertexValue [fieldName=" + eventFieldName + ", fieldValue=" + eventFieldValue + ", relationship=" + relationshipIndex + ", useRealm=" + useRealm
                        + ", realmLabel=" + eventRealmLabel + ", vertexType=" + sourceIndex + ", markings=" + this.markings + ", maskedValue=" + maskedValue
                        + ", hasMaskedValue=" + hasMaskedValue + "]";
    }

} /* end VertexValue */
