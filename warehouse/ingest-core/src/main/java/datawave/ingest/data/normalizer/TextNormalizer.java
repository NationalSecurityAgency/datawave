package datawave.ingest.data.normalizer;

import datawave.ingest.data.Type;
import datawave.ingest.data.config.NormalizedContentInterface;

import datawave.data.normalizer.NormalizationException;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Multimap;

public interface TextNormalizer {

    /**
     * A method used to setup this normalizer. Normally properties that are used to configure this normalizer start with type.name() + '.' +
     * this.getClass().getSimpleName() + '.' + instance.
     *
     * @param type
     *            The data type for which this normalizer is being configured
     * @param instance
     *            The instance of this normalizer being configured
     * @param config
     *            The configuration
     */
    void setup(Type type, String instance, Configuration config);

    /**
     * Creates normalized content for ingest based upon implemented logic.
     *
     * @param field
     *            The field being normalized
     * @param value
     *            The value to normalize
     * @return a normalized value
     * @throws NormalizationException
     *             if there are issues with the normalization
     */
    String normalizeFieldValue(String field, String value) throws NormalizationException;

    /**
     * Creates normalized content for ingest based upon implemented logic.
     *
     * @param field
     *            The field being normalized
     * @param regex
     *            The regex to normalize
     * @return a normalized value
     * @throws NormalizationException
     *             if there are issues with the normalization
     */
    String normalizeFieldRegex(String field, String regex) throws NormalizationException;

    /**
     * Creates normalized content for ingest based upon implemented logic.
     *
     * @param field
     *            The field to normalize
     * @return a normalized content object.
     */
    NormalizedContentInterface normalize(NormalizedContentInterface field);

    /**
     * Creates normalized content for ingest based upon implemented logic.
     *
     * @param fields
     *            the fields to normalize
     * @return a multimap of normalized content objects.
     */
    Multimap<String,NormalizedContentInterface> normalize(Multimap<String,String> fields);

    /**
     * Creates normalized content for ingest based upon implemented logic.
     *
     * @param fields
     *            the fields to normalize
     * @return a multimap of normalized content objects.
     */
    Multimap<String,NormalizedContentInterface> normalizeMap(Multimap<String,NormalizedContentInterface> fields);

}
