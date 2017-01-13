package nsa.datawave.ingest.data.config;

import nsa.datawave.ingest.data.Type;

import nsa.datawave.policy.IngestPolicyEnforcer;
import org.apache.hadoop.conf.Configuration;

public interface DataTypeHelper {
    
    /**
     * Performs property validation and setup
     * 
     * @param config
     * @throws IllegalArgumentException
     */
    public void setup(Configuration config) throws IllegalArgumentException;
    
    /**
     * 
     * @return the datatype value from the Configuration file.
     */
    public Type getType();
    
    public IngestPolicyEnforcer getPolicyEnforcer();
    
    public static interface Properties {
        public static final String DATA_NAME = "data.name";
        public static final String DATA_NAME_OVERRIDE = "data.name.override";
        public static final String DOWNCASE_FIELDS = ".data.fields.to.downcase";
        public static final String INGEST_POLICY_ENFORCER_CLASS = ".ingest.policy.enforcer.class";
        public static final String[] DEFAULT_DOWNCASE_FIELDS = {"md5", "sha1", "sha256"};
    }
}
