package nsa.datawave;

import nsa.datawave.ingest.data.RawRecordContainer;
import nsa.datawave.ingest.data.config.NormalizedContentInterface;
import nsa.datawave.ingest.data.config.ingest.BaseIngestHelper;

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
}
