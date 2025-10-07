package datawave.ingest.data.config.ingest;

import com.google.common.collect.Multimap;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;

/**
 * * Class is here because the tests need it.
 */
public class AtomIngestHelper extends BaseIngestHelper {

    @Override
    public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer event) {
        return null;
    }

}
