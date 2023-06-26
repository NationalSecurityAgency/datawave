package datawave.ingest.data.config.ingest;

import datawave.data.type.LcNoDiacriticsType;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Multimap;

/**
 * Simple configuration object for use with the Metadata ingest software.
 *
 *
 *
 */
public class MetaDataIngestHelper extends BaseIngestHelper {

    @Override
    public void setup(Configuration config) {
        config.set(Properties.DATA_NAME, "metadata");
        config.set("metadata" + BaseIngestHelper.DEFAULT_TYPE, LcNoDiacriticsType.class.getName());
        super.setup(config);
    }

    @Override
    public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer event) {
        return null;
    }
}
