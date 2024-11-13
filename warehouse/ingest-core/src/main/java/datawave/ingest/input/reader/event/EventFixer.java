package datawave.ingest.input.reader.event;

import org.apache.hadoop.conf.Configuration;

import datawave.ingest.data.RawRecordContainer;

public interface EventFixer {
    void setup(Configuration conf);

    RawRecordContainer fixEvent(RawRecordContainer e);
}
