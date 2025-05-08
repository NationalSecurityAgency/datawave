package datawave.ingest.data.config.ingest;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Multimap;

import datawave.ingest.data.config.NormalizedContentInterface;

/**
 * Interface for applying Whindex transformations during the ingest process.
 */
public interface WhindexIngest {

    /**
     * Parses the whindex rules from the provided Hadoop Configuration.
     * <p>
     * The configuration is expected to have properties in the following form: <code>typeName.whindex.rules.[groupID].[property]=value</code>, where the
     * property is one of VALUE_FIELD, SRC_FIELD, DELETE_SRC_FIELD, DST_FIELD, or VALUES. Each groupID represents a separate whindex rule.
     * </p>
     *
     * @param config
     *            the Hadoop Configuration containing whindex rules.
     * @throws IllegalArgumentException
     *             if there are configuration issues.
     */
    void setup(Configuration config);

    /**
     * @param field
     *            the field to check.
     * @return {@code true} if {@code field} is a whindex field.
     */
    boolean isWhindexField(String field);

    /**
     * Applies the loaded whindex configuration to the Multimap of values passed in. Only applies whindex configurations for which all the necessary fields and
     * values are present in the Multimap.
     *
     * @param eventFields
     *            the multimap of values to check and apply the whindex configurations to.
     * @return a multimap of updated values based on the loaded whindex configuration.
     */
    Multimap<String,NormalizedContentInterface> processWhindexFields(Multimap<String,NormalizedContentInterface> eventFields);

    /**
     * Returns a multimap containing all WhindexConfigs mapped to their related ValueFields.
     *
     * @return a multimap of K:ValueFields -> V:WhindexConfig(s)
     */
    Multimap<String,WhindexConfig> getValueFieldsToWhindexConfigs();
}
