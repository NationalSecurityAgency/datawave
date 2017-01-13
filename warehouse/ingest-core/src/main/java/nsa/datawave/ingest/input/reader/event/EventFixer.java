package nsa.datawave.ingest.input.reader.event;

import nsa.datawave.ingest.data.RawRecordContainer;
import org.apache.hadoop.conf.Configuration;

public interface EventFixer {
    void setup(Configuration conf);
    
    RawRecordContainer fixEvent(RawRecordContainer e);
}
