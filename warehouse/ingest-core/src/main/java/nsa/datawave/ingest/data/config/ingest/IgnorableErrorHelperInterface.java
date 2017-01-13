package nsa.datawave.ingest.data.config.ingest;

import nsa.datawave.ingest.data.RawRecordContainer;

import org.apache.hadoop.conf.Configuration;

/**
 * This is a class that can be used to determine when an event fatal error can be ignored resulting in the event being dropped altogether.
 * 
 * 
 */
public interface IgnorableErrorHelperInterface {
    public void setup(Configuration conf);
    
    public boolean isIgnorableFatalError(RawRecordContainer e, String err);
}
