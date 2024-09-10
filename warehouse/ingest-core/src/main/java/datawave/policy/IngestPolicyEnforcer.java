package datawave.policy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.ingest.data.RawRecordContainer;

/**
 * Abstract class to be used by RecordReaders for event validation.
 *
 */
public abstract class IngestPolicyEnforcer implements Policy<RawRecordContainer> {

    protected static final Logger log = LoggerFactory.getLogger(IngestPolicyEnforcer.class);

    public static class NoOpIngestPolicyEnforcer extends IngestPolicyEnforcer {
        @Override
        public void validate(RawRecordContainer arg) throws Exception {
            // no op, everything is valid
        }
    }
}
