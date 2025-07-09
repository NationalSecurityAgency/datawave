package datawave.ingest.data.config.ingest;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;

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

    /**
     * Checks if error-index-fields have been initialized yet.
     * @return FALSE if errorIndexedFields is empty, TRUE if it's not.
     */
    private boolean hasErrorIndexConfig(){
        return !super.errorIndexedFields.isEmpty();
    }

    /**
     * Checks if error-reverse-index-fields have been initialized yet.
     * @return FALSE if errorReverseIndexedFields is empty, TRUE if it's not.
     */
    private boolean hasErrorReverseIndexConfig(){
        return !super.errorReverseIndexedFields.isEmpty();

    }

    /**
     * Checks if the {@code fieldName} has been indexed in either the
     * index-field map or the error-index-field map.
     * @return TRUE if {@code fieldName} has been indexed, FALSE if not.
     */
    @Override
    public boolean isIndexedField(String fieldName) {
        if(hasErrorIndexConfig()) {
            return super.isErrorIndexedField(fieldName);
        } else {
            return super.isIndexedField(fieldName);
        }
    }

    /**
     * Checks if the {@code fieldName} has been indexed in either the
     * reverse-index-field map or the error-reverse-index-field map.
     * @return TRUE if {@code fieldName} has been indexed, FALSE if not.
     */
    @Override
    public boolean isReverseIndexedField(String fieldName) {
        if(hasErrorReverseIndexConfig()) {
            return super.isErrorReverseIndexedField(fieldName);
        } else {
            return super.isIndexedField(fieldName);
        }
    }
}
