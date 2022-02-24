package datawave.ingest.mapreduce;

import com.google.common.collect.Multimap;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;

/**
 * This optional interface is intended to complement the IngestHelperInterface interface's handling of errors that occur within ingest jobs.
 *
 * One use case is when IngestHelperInterface's getEventFields throws an exception. The getEventFields method will not return a Multimap of field values
 * (because it instead threw an exception). Prior to FieldSalvager, this meant that the error tables would not have information on any of the
 * RawRecordContainer's field values.
 *
 * FieldSalvager implementations can attempt to provide a subset of the field values, so that the error tables can have more helpful information about the
 * failed record, perhaps aiding troubleshooting efforts. An implementation could return only those field names that are relatively well-structured and
 * predictably formatted, very unlikely to cause exceptions while processing.
 */
public interface FieldSalvager {
    /**
     * @param rawRecordContainer
     * @return Multimap containing subset of field values, possibly empty but not null
     */
    Multimap<String,NormalizedContentInterface> getSalvageableEventFields(RawRecordContainer rawRecordContainer);
}
