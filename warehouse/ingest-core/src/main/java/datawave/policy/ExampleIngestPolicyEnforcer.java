package datawave.policy;

import org.apache.log4j.Logger;

import datawave.core.common.logging.ThreadConfigurableLogger;
import datawave.ingest.data.RawDataErrorNames;
import datawave.ingest.data.RawRecordContainer;

/**
 * The purpose of this class is to demonstrate a likely usage pattern for an IngestPolicyEnforcer implementation
 */
public class ExampleIngestPolicyEnforcer extends IngestPolicyEnforcer {

    private static final Logger log = ThreadConfigurableLogger.getLogger(ExampleIngestPolicyEnforcer.class);

    /**
     * Here we'll check for a few basic issues, and rather than throw an exception to convey failure we'll rely on DataWave ingest's internal error-tracking
     * capabilities, and just "add" errors to the event as needed. This affords the opportunity to alter the behavior of ingest at runtime by classifying
     * certain error types as "fatal", "ignorable", etc, via external configuration.
     */
    @Override
    public void validate(RawRecordContainer event) throws Exception {
        if (null == event) {
            log.warn("Attempting to validate a NULL " + RawRecordContainer.class.getSimpleName() + " instance");
            return;
        }
        validateUUIDs(event);
        validateEventDate(event);
        validateSecurityMarkings(event);
    }

    /**
     * Verify that security markings have already been applied to the raw record. See ingest config properties, '*.data.category.security.field.domains' and
     * '*.data.category.security.field.names'
     *
     * @param event
     *            the event container
     */
    private void validateSecurityMarkings(RawRecordContainer event) {
        if (null == event.getSecurityMarkings() || event.getSecurityMarkings().isEmpty()) {
            event.addError(RawDataErrorNames.MISSING_DATA_ERROR);
            log.error("No security markings have been applied to the event: " + event.getRawFileName() + ", record: " + event.getRawRecordNumber());
        }
    }

    /**
     * Verify that the event date should have already been set to a value greater than unix epoch (long integer). See ingest config property,
     * '*.data.category.date'
     *
     * @param event
     *            an event container
     */
    private void validateEventDate(RawRecordContainer event) {
        if (!event.isTimestampSet()) {
            event.addError(RawDataErrorNames.EVENT_DATE_MISSING);
            log.error("Event date missing for Event in raw file: " + event.getRawFileName() + ", record: " + event.getRawRecordNumber());
        }
    }

    /**
     * Verify that one or more *alternate* ID's are associated with the raw record... Typically, these represent unique identifiers that are known to external
     * clients or data providers. Populating the "altIds" collection may be accomplished by setting the ingest config property, '*.data.category.uuid.fields'
     * for the data type in question
     *
     * @param event
     *            the event container
     */
    private void validateUUIDs(RawRecordContainer event) {
        if (null == event.getAltIds() || event.getAltIds().isEmpty()) {
            event.addError(RawDataErrorNames.UUID_MISSING);
            log.error("UUID missing for event in raw file: " + event.getRawFileName() + ", record: " + event.getRawRecordNumber());
        }
    }
}
