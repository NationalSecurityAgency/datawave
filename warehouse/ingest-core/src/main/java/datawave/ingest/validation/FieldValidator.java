package datawave.ingest.validation;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Multimap;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.config.NormalizedContentInterface;

/**
 * Event validator that will be used to validate events against a known configuration.
 */
public interface FieldValidator {
    String FIELD_VALIDATOR_NAMES = ".fields.validator.classes";

    /**
     * Setup this event validator for the type specified
     *
     * @param type
     *            the field type
     * @param conf
     *            a configuration
     */
    void init(Type type, Configuration conf);

    /**
     * Validate the event against a set of metadata
     *
     * @param event
     *            the event
     * @param fields
     *            a map of fields
     * @throws ValidationException
     *             for issues with field validation
     */
    void validate(RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields) throws ValidationException;

    /**
     * Validate the event against a set of metadata
     *
     * @param fields
     *            a map of fields
     * @throws ValidationException
     *             for issues with field validation
     */
    void validate(Multimap<String,String> fields) throws ValidationException;

}
