package datawave.ingest.data.config.ingest;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Multimap;

import datawave.ingest.data.config.NormalizedContentInterface;

public interface WhindexIngest {

    /**
     * Used to allow external scopes to interface with the WhindexIngest's WhindexFieldNormalizer's .setup() method. Initializes the WhindexIngest from a
     * {@link Configuration}.
     *
     * @param config
     *            the {@link Configuration}.
     */
    void setup(Configuration config);

    /**
     * @param field
     *            the field to check.
     * @return {@code true} if {@code field} is a whindex field.
     */
    boolean isWhindexField(String field);

    /**
     * // todo Given a "{@code RULE}", return a {@code Multimap<String, NormalizedContentInterface>} of whindex fields ("{@code DST_FIELD}") mapped to the
     * values specified by the {@code RULE}.
     *
     * @return the mapping of whindex fields to values.
     */
    Multimap<String,NormalizedContentInterface> processWhindexFields(Multimap<String,NormalizedContentInterface> eventFields);

    Multimap<String,WhindexConfig> getValueFieldsToWhindexConfigs();
}
