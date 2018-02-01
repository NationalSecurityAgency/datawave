package datawave.ingest.data;

import org.apache.hadoop.conf.Configuration;
import datawave.ingest.data.config.ingest.IgnorableErrorHelperInterface;

public class TestIgnorableHelper implements IgnorableErrorHelperInterface {
    
    public void setup(Configuration conf) {}
    
    /**
     * @return true if the event is missing the UUID.
     */
    @Override
    public boolean isIgnorableFatalError(RawRecordContainer e, String err) {
        if (err.equals(RawDataErrorNames.UUID_MISSING)) {
            return true;
        }
        return false;
    }
}
