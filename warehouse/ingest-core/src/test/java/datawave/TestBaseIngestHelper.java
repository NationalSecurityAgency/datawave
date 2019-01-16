package datawave;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.BaseIngestHelper;

import com.google.common.collect.Multimap;

public class TestBaseIngestHelper extends BaseIngestHelper {
    private final Multimap<String,NormalizedContentInterface> eventFields;
    
    /**
     * Deliberately return null from getEventFields when created
     */
    public TestBaseIngestHelper() {
        this(null);
    }
    
    public TestBaseIngestHelper(Multimap<String,NormalizedContentInterface> eventFields) {
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
}
