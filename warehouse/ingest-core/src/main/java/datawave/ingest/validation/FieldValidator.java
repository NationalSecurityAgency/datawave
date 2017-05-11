package datawave.ingest.validation;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.config.NormalizedContentInterface;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Multimap;

/**
 * Event validator that will be used to validate events against a known configuration.
 */
public interface FieldValidator {
    String FIELD_VALIDATOR_NAMES = ".fields.validator.classes";
    
    /**
     * Setup this event validator for the type specified
     * 
     * @param type
     * @param conf
     */
    void init(Type type, Configuration conf);
    
    /**
     * Validate the event against a set of metadata
     * 
     * @param event
     * @param fields
     */
    void validate(RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields) throws ValidationException;
    
    /**
     * Validate the event against a set of metadata
     * 
     * @param fields
     */
    void validate(Multimap<String,String> fields) throws ValidationException;
    
}
