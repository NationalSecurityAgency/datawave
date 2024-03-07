package datawave.ingest.data.config.ingest;

import com.google.common.collect.Multimap;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.WithAgeOff;
import datawave.ingest.data.config.NormalizedContentInterface;

/**
 * An interface used to derive the age off date from the record and the extracted fields. Default implementation simply returns the event date.
 */
public interface AgeOffIngest {

    /**
     * Get the ageoff date from the record and fields
     *
     * @param value
     * @param fields
     * @return the age off date (usually the record date)
     */
    default long getAgeOffDate(RawRecordContainer value, Multimap<String,NormalizedContentInterface> fields) {
        if (value instanceof WithAgeOff) {
            return ((WithAgeOff) value).getAgeOffDate();
        }
        return value.getDate();
    }

}
