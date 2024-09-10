package datawave.ingest.data.config.ingest;

import org.apache.hadoop.conf.Configuration;

import datawave.ingest.data.RawRecordContainer;

/**
 * This is a class that can be used to determine when an event fatal error can be ignored resulting in the event being dropped altogether.
 *
 *
 */
public interface IgnorableErrorHelperInterface {
    void setup(Configuration conf);

    boolean isIgnorableFatalError(RawRecordContainer e, String err);
}
