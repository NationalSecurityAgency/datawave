package datawave;

import com.google.common.collect.Multimap;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.AbstractContentIngestHelper;

public class TestAbstractContentIngestHelper extends AbstractContentIngestHelper {
    private final Multimap<String,NormalizedContentInterface> eventFields;
    
    /**
     * Deliberately return null from getEventFields when created
     */
    public TestAbstractContentIngestHelper() {
        this(null);
    }
    
    public TestAbstractContentIngestHelper(Multimap<String,NormalizedContentInterface> eventFields) {
        super();
        this.eventFields = eventFields;
    }
    
    @Override
    public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer event) {
        return eventFields;
    }
    
    @Override
    public boolean isDataTypeField(String fieldName) {
        return false;
    }
    
    @Override
    public boolean isCompositeField(String fieldName) {
        return false;
    }
    
    @Override
    public String getTokenFieldNameDesignator() {
        return "_TOKEN";
    }
    
    @Override
    public boolean isContentIndexField(String field) {
        return false;
    }
    
    @Override
    public boolean isReverseContentIndexField(String field) {
        return false;
    }
    
    @Override
    public boolean getSaveRawDataOption() {
        return false;
    }
    
    @Override
    public String getRawDocumentViewName() {
        return null;
    }
    
    @Override
    public boolean isIndexListField(String field) {
        return false;
    };
    
    @Override
    public boolean isReverseIndexListField(String field) {
        return false;
    };
    
    @Override
    public String getListDelimiter() {
        return ",";
    }
    
}
