package datawave.ingest.data.config.ingest;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;

import org.apache.hadoop.conf.Configuration;

/**
 *
 */
public class ErrorShardedIngestHelper extends BaseIngestHelper {

    @Override
    public void setup(Configuration config) {
        // we are error
        config.set(Properties.DATA_NAME, "error");
        super.setup(config);
    }

    private IngestHelperInterface delegate = null;

    public void setDelegateHelper(IngestHelperInterface delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean isIndexedField(String fieldName) {
        return true;
    }

    @Override
    public boolean isReverseIndexedField(String fieldName) {
        return true;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.config.ingest.AbstractIngestHelper#getEventFields(datawave.ingest.data.Event)
     */
    @Override
    public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer event) {
        // we need to do this safely, make our best attempt to get some fields
        try {
            return delegate.getEventFields(event);
        } catch (Exception e) {
            return HashMultimap.create();
        }
    }

    /**
     * Override to provide access to the data type handler
     */
    @Override
    public Multimap<String,NormalizedContentInterface> normalizeMap(Multimap<String,NormalizedContentInterface> fields) {
        return super.normalizeMap(fields);
    }
}
