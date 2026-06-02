package datawave.ingest.jobcache;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;

/**
 * Validates a parameter is a non-empty String.
 */
public class NonEmptyStringValidator implements IParameterValidator {
    @Override
    public void validate(String parameter, String value) throws ParameterException {
        if (value == null || value.isEmpty()) {
            throw new ParameterException(parameter + " must be a non-empty String.");
        }
    }
}
