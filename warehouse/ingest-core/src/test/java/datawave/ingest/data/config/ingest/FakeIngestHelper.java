package datawave.ingest.data.config.ingest;

import org.apache.accumulo.access.Access;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.marking.AccessExpressionMarkings;

/**
 *
 * Stub for testing purposes
 *
 */
public class FakeIngestHelper extends BaseIngestHelper {
    @Override
    public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer event) {
        Multimap<String,NormalizedContentInterface> eventFields = HashMultimap.create();
        AccessExpressionMarkings markings = AccessExpressionMarkings.builder().accessExpression(Access.builder().build().newExpression("PERSONAL")).build();

        eventFields.put("FAKE_FIELD", new NormalizedFieldAndValue("FAKE_FIELD", "fake value", markings));
        return eventFields;
    }
}
