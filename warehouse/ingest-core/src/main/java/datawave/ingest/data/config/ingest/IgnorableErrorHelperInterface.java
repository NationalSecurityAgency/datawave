package datawave.ingest.data.config.ingest;

import datawave.ingest.data.RawRecordContainer;

import org.apache.hadoop.conf.Configuration;

/**
 * This is a class that can be used to determine when an event fatal error can be ignored resulting in the event being dropped altogether.
 *
 *
 */
public interface IgnorableErrorHelperInterface {
    void setup(Configuration conf);

    boolean isIgnorableFatalError(RawRecordContainer e, String err);
}
