package datawave.core.query.language.parser.jexl;

import java.util.Map;
import java.util.Set;

/**
 *
 */
public interface ControlledQueryParser {

    void setAllowedFields(Set<String> allowedSpcmaFields);

    Set<String> getAllowedFields();

    void setExcludedValues(Map<String,Set<String>> defeatedValues);

    Map<String,Set<String>> getExcludedValues();

    void setIncludedValues(Map<String,Set<String>> requiredValues);

    Map<String,Set<String>> getIncludedValues();

}
