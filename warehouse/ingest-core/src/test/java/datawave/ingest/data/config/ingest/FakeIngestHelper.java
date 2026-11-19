package datawave.ingest.data.config.ingest;

import org.apache.accumulo.core.security.ColumnVisibility;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;

/**
 *
 * Stub for testing purposes
 *
 */
public class FakeIngestHelper extends BaseIngestHelper {
    @Override
    public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer event) {
        Multimap<String,NormalizedContentInterface> eventFields = HashMultimap.create();
        eventFields.put("FAKE_FIELD", new NormalizedFieldAndValue("FAKE_FIELD", "fake value", new ColumnVisibility("PERSONAL")));
        return eventFields;
    }
}
