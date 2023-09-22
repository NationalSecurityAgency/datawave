package datawave.ingest.metadata;

import org.apache.accumulo.core.data.Value;

import com.google.common.collect.Multimap;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.mapreduce.job.BulkIngestKey;

public interface RawRecordMetadata {

    String NO_TOKEN_DESIGNATOR = "";
    String DELIMITER = "\0";
    boolean INCLUDE_LOAD_DATES = true;
    boolean EXCLUDE_LOAD_DATES = false;

    void addEvent(IngestHelperInterface helper, RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields, long loadTimeInMillis);

    void addEvent(IngestHelperInterface helper, RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields);

    void addEventWithoutLoadDates(IngestHelperInterface helper, RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields);

    void addEvent(IngestHelperInterface helper, RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields, boolean frequency);

    Multimap<BulkIngestKey,Value> getBulkMetadata();

    void clear();

}
