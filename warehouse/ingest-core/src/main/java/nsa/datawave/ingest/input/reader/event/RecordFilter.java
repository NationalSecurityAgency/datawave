package nsa.datawave.ingest.input.reader.event;

import org.apache.hadoop.conf.Configuration;

import nsa.datawave.ingest.data.RawRecordContainer;

/**
 * event filters should implement this
 */
public interface RecordFilter {
    public void initialize(Configuration conf);
    
    public boolean accept(RawRecordContainer e);
    
    public void close();
}
