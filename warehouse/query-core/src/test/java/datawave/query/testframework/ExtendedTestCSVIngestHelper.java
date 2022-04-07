package datawave.query.testframework;

import datawave.ingest.csv.config.helper.ExtendedCSVIngestHelper;

import com.google.common.collect.Multimap;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;

/** Patches the ExtendedCSVIngestHelper to add visibility markings to the fields based on the contents of the event */
public class ExtendedTestCSVIngestHelper extends ExtendedCSVIngestHelper {
    @Override
    public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer event) {
        Multimap<String,NormalizedContentInterface> normalizedFields = super.getEventFields(event);
        
        if (null != event.getSecurityMarkings() && !event.getSecurityMarkings().isEmpty()) {
            for (NormalizedContentInterface nci : normalizedFields.values()) {
                nci.setMarkings(event.getSecurityMarkings());
            }
        }
        
        return normalizedFields;
    }
}
