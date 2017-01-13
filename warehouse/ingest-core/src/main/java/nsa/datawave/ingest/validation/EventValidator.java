package nsa.datawave.ingest.validation;

import java.util.Collection;
import java.util.Map;

import nsa.datawave.ingest.data.RawRecordContainer;
import nsa.datawave.ingest.data.Type;

import org.apache.hadoop.conf.Configuration;

public interface EventValidator {
    /**
     * Setup this event validator for the type specified
     * 
     * @param type
     * @param conf
     */
    public void setup(Type type, Configuration conf);
    
    /**
     * Is this a fieldname that is to be validated. This method can be used to gather the fields for the final validation
     * 
     * @param fieldname
     * @return true if required for validation, false otherwise
     */
    public boolean validated(String fieldname);
    
    /**
     * Validate the event against a set of metadata
     * 
     * @param event
     * @param parameters
     */
    public void validate(RawRecordContainer event, Map<String,Collection<Object>> parameters);
    
}
