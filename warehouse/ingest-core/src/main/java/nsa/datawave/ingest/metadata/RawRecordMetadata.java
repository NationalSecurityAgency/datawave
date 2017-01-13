package nsa.datawave.ingest.metadata;

import nsa.datawave.ingest.data.RawRecordContainer;
import nsa.datawave.ingest.data.config.NormalizedContentInterface;
import nsa.datawave.ingest.data.config.ingest.IngestHelperInterface;
import nsa.datawave.ingest.mapreduce.job.BulkIngestKey;

import org.apache.accumulo.core.data.Value;

import com.google.common.collect.Multimap;

public interface RawRecordMetadata {
    
    public static final String NO_TOKEN_DESIGNATOR = "";
    public static final String DELIMITER = "\0";
    public static final boolean INCLUDE_LOAD_DATES = true;
    public static final boolean EXCLUDE_LOAD_DATES = false;
    
    public void addEvent(IngestHelperInterface helper, RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields, long loadTimeInMillis);
    
    public void addEvent(IngestHelperInterface helper, RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields);
    
    public void addEventWithoutLoadDates(IngestHelperInterface helper, RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields);
    
    public void addEvent(IngestHelperInterface helper, RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields, boolean frequency);
    
    public Multimap<BulkIngestKey,Value> getBulkMetadata();
    
    public void clear();
    
}
