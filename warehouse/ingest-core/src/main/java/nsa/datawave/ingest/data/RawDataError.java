package nsa.datawave.ingest.data;

/**
 * List of error conditions that occurred during parsing of the raw data.
 * 
 * 
 * 
 */
@Deprecated
public enum RawDataError {
    TOO_MANY_FIELDS,
    NOT_ENOUGH_FIELDS,
    UUID_MISSING,
    INVALID_XML,
    IO_ERROR,
    INVALID_RECORD_NUMBER,
    EVENT_DATE_MISSING,
    EVENT_DATE_ERROR,
    DEPENDENT_LOOKUP_FAILED,
    FIELD_EXTRACTION_ERROR,
    RUNTIME_EXCEPTION,
    MISSING_DATA_ERROR,
    INVALID_DATA_ERROR,
    UID_ERROR,
    GROUP_MARKING_ERROR,
    UNKNOWN_DATATYPE_OVERRIDE,
    ERROR_METRIC,
    UPSTREAM_ERROR,
    MISSING_POLICY_ENFORCER
}
