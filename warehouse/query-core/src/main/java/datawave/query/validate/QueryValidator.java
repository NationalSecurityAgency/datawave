package datawave.query.validate;

import org.apache.commons.jexl3.parser.ParseException;

public interface QueryValidator {
    
    QueryValidationResult validate(String query, QueryValidatorConfiguration config) throws ParseException;
}
