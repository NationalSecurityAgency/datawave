package datawave.ingest.input.reader.event;

import org.apache.hadoop.conf.Configuration;

import datawave.ingest.data.RawRecordContainer;

/**
 * event filters should implement this
 */
public interface RecordFilter {
    void initialize(Configuration conf);

    boolean accept(RawRecordContainer e);

    void close();
}
