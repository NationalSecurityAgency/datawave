package datawave.ingest.data.config.ingest;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Multimap;

import datawave.ingest.data.Type;
import datawave.ingest.data.config.NormalizedContentInterface;

/**
 * Helper class for {@link WhindexIngest} and {@link WhindexFieldNormalizer}. Makes accessing a WhindexIngest easier.
 */
public class WhindexFieldIngestHelper implements WhindexIngest {

    private final Type type;
    private final WhindexIngest.WhindexFieldNormalizer normalizer = new WhindexIngest.WhindexFieldNormalizer();

    /**
     * Constructor
     *
     * @param type
     *            the datatype we're interested in reading rules for.
     */
    public WhindexFieldIngestHelper(Type type) {
        this.type = type;
    }

    /**
     * Initializes the normalizer, passing it a {@link Configuration} that has {@code <datatype>.rules}.
     *
     * @param config
     *            the {@link Configuration}.
     */
    @Override
    public void setup(Configuration config) throws IllegalArgumentException {
        normalizer.setup(type, config);
    }

    /**
     * Given a "{@code RULE}", return a {@code Multimap<String, String>} of whindex fields ("{@code DST_FIELD}") mapped to the values specified by the
     * {@code RULE}.
     *
     * @return the mapping of whindex fields to values.
     */
    @Override
    public Multimap<String,String> getWhindexFieldDefinitions() {
        return normalizer.getWhindexFieldDefinitions();
    }

    /**
     * // todo Given a "{@code RULE}", return a {@code Multimap<String, NormalizedContentInterface>} of whindex fields ("{@code DST_FIELD}") mapped to the
     * values specified by the {@code RULE}.
     *
     * @return the mapping of whindex fields to values.
     */
    @Override
    public Multimap<String,NormalizedContentInterface> getWhindexFields(Multimap<String,NormalizedContentInterface> eventFields) {
        return normalizer.normalizeMap(eventFields);
    }

    /**
     * @param field
     *            the field to check.
     * @return {@code true} if {@code field} is a whindex field.
     */
    @Override
    public boolean isWhindexField(String field) {
        return normalizer.isWhindexField(field);
    }

    /**
     * {@code OverloadedWhindexField}s are source fields ("{@code SRC_FIELD}") that become redundant once whindex entries are generated. They are marked to be
     * removed.
     *
     * @param field
     *            the field to check.
     * @return {@code true} if {@code field} is an {@code OverloadedWhindexField} (marked for removal)
     */
    @Override
    public boolean isOverloadedWhindexField(String field) {
        return normalizer.getOverloadedFields().contains(field);
    }

}
