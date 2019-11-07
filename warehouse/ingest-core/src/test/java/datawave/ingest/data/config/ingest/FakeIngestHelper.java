package datawave.ingest.data.config.ingest;

import java.util.HashMap;
import java.util.Map;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 *
 * Stub for testing purposes
 *
 */
public class FakeIngestHelper extends BaseIngestHelper {
    @Override
    public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer event) {
        Multimap<String,NormalizedContentInterface> eventFields = HashMultimap.create();
        Map<String,String> markings = new HashMap<>();
        markings.put("P", "PERSONAL");
        
        eventFields.put("FAKE_FIELD", new NormalizedFieldAndValue("FAKE_FIELD", "fake value", markings));
        return eventFields;
    }
}
