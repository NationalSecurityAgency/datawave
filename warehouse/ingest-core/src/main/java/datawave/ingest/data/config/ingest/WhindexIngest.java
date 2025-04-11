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
    void setup(Configuration config) throws IllegalArgumentException;

    /**
     * Given a "{@code RULE}", return a {@code Multimap<String, String>} of whindex fields ("{@code DST_FIELD}") mapped to the values specified by the
     * {@code RULE}.
     *
     * @return the mapping of whindex fields to values.
     */
    Multimap<String,String> getWhindexFieldDefinitions();

    /**
     * @param field
     *            the field to check.
     * @return {@code true} if {@code field} is a whindex field.
     */
    boolean isWhindexField(String field);

    /**
     * {@code OverloadedWhindexField}s are source fields ("{@code SRC_FIELD}") that become redundant once whindex entries are generated. They are marked to be
     * removed.
     *
     * @param field
     *            the field to check.
     * @return {@code true} if {@code field} is an {@code OverloadedWhindexField} (marked for removal)
     */
    boolean isOverloadedWhindexField(String field);

    /**
     * // todo Given a "{@code RULE}", return a {@code Multimap<String, NormalizedContentInterface>} of whindex fields ("{@code DST_FIELD}") mapped to the
     * values specified by the {@code RULE}.
     *
     * @return the mapping of whindex fields to values.
     */
    Multimap<String,NormalizedContentInterface> getWhindexFields(Multimap<String,NormalizedContentInterface> eventFields);

}
