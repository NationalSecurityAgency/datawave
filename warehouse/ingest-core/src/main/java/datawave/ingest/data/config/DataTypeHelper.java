package datawave.ingest.data.config;

import org.apache.hadoop.conf.Configuration;

import datawave.ingest.data.Type;
import datawave.policy.IngestPolicyEnforcer;

public interface DataTypeHelper {

    /**
     * Performs property validation and setup
     *
     * @param config
     *            a hadoop {@link Configuration} object
     * @throws IllegalArgumentException
     *             if an illegal argument is detected
     */
    void setup(Configuration config) throws IllegalArgumentException;

    /**
     *
     * @return the datatype value from the Configuration file.
     */
    Type getType();

    IngestPolicyEnforcer getPolicyEnforcer();

    interface Properties {
        String DATA_NAME = "data.name";
        String DATA_NAME_OVERRIDE = "data.name.override";
        String DOWNCASE_FIELDS = ".data.fields.to.downcase";
        String INGEST_POLICY_ENFORCER_CLASS = ".ingest.policy.enforcer.class";
        String[] DEFAULT_DOWNCASE_FIELDS = {"md5", "sha1", "sha256"};
    }
}
