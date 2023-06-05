package datawave.ingest.data.config;

import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.Set;

import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;

import datawave.ingest.data.config.ingest.FieldNameAliaserNormalizer;
import datawave.policy.IngestPolicyEnforcer;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base helper object that supports validation of the datatype. Subclasses will further validate the configuration object.
 * 
 * 
 * 
 */
public class DataTypeHelperImpl implements DataTypeHelper {
    /** Logger for DataTypeHelperImpl */
    private static final Logger logger = LoggerFactory.getLogger(DataTypeHelperImpl.class);
    
    protected static final String[] EMPTY_VALUES = new String[0];
    
    private Type type = null;
    
    protected final Set<String> fieldsToDowncase = new HashSet<>();
    protected IngestPolicyEnforcer ingestPolicyEnforcer;
    
    protected FieldNameAliaserNormalizer aliaser = new FieldNameAliaserNormalizer();
    
    @Override
    public void setup(Configuration config) {
        initType(config);
        
        // Ingest Policy Enforcement
        // check if we have a default
        String policyEnforcerClass = "";
        if (null != config.get("all" + Properties.INGEST_POLICY_ENFORCER_CLASS)) {
            policyEnforcerClass = config.get("all" + Properties.INGEST_POLICY_ENFORCER_CLASS);
        }
        
        // check if we have a type specific one
        if (null != config.get(getType().typeName() + Properties.INGEST_POLICY_ENFORCER_CLASS)) {
            policyEnforcerClass = config.get(getType().typeName() + Properties.INGEST_POLICY_ENFORCER_CLASS);
        }
        
        try {
            ingestPolicyEnforcer = (IngestPolicyEnforcer) Class.forName(policyEnforcerClass).getDeclaredConstructor().newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException |
                 InvocationTargetException e) {
            logger.error(e.getLocalizedMessage());
            throw new RuntimeException(e.getLocalizedMessage());
        }
        
        aliaser.setup(getType(), config);
    }
    
    private void initType(final Configuration config) {
        if (type != null && TypeRegistry.hasInstance())
            return;
        
        final String t = ConfigurationHelper.isNull(config, Properties.DATA_NAME, String.class);
        TypeRegistry.getInstance(config);
        type = TypeRegistry.getType(t);
        
        String[] downcaseFields = config.getStrings(type.typeName() + Properties.DOWNCASE_FIELDS, Properties.DEFAULT_DOWNCASE_FIELDS);
        for (String s : downcaseFields) {
            fieldsToDowncase.add(s.toLowerCase());
        }
    }
    
    /**
     * 
     * @return the datatype value from the Configuration file.
     */
    public Type getType() {
        return type;
    }
    
    /**
     * Remove whitespace, lowercase the specified fields
     * 
     * @param fieldName
     *            the field name
     * @param fieldValue
     *            the field value
     * @return null if the fieldValue is null or empty after {@code trim()} is applied.
     */
    
    public String clean(String fieldName, String fieldValue) {
        String result = StringUtils.trim(fieldValue);
        if (StringUtils.isEmpty(result)) {
            return null;
        }
        
        if (fieldsToDowncase.contains(fieldName.toLowerCase())) {
            result = result.toLowerCase();
        }
        
        return result;
    }
    
    /** @return the type name */
    protected String typeName() {
        return getType().typeName();
    }
    
    /**
     * Creates the property name for the datatype
     *
     * @param config
     *            a hadoop {@link Configuration}
     * @param base
     *            the base of a property
     * @return a period concatenated type and base
     */
    protected String key2property(final Configuration config, String base) {
        if (type == null)
            initType(config);
        if (!base.startsWith("."))
            base = "." + base;
        
        return typeName() + base;
    }
    
    /**
     * Logs that a default value has been set
     * 
     * @param p
     *            the property
     * @param v
     *            the value
     */
    private void logSetDefault(final String p, final String v) {
        if (logger.isDebugEnabled())
            logger.debug("Set default value: '{}' to '{}' in '{}'", p, v, typeName());
    }
    
    /**
     * Sets a property in the config if not already set.
     *
     * @param config
     *            a hadoop {@link Configuration}
     * @param key
     *            the property key
     * @param value
     *            the property value
     */
    protected void setIfUnset(final Configuration config, final String key, final String value) {
        final String property = key2property(config, key);
        
        if (config.get(property) == null) {
            config.setStrings(property, value);
            logSetDefault(property, value);
        }
    }
    
    /**
     * Sets a property in the config if not already set.
     * 
     * @param config
     *            a hadoop {@link Configuration}
     * @param key
     *            the property key
     * @param values
     *            an array of property values
     */
    protected void setIfUnset(final Configuration config, final String key, final String[] values) {
        final String property = key2property(config, key);
        
        if (config.get(property) == null) {
            config.setStrings(property, values);
            logSetDefault(property, StringUtils.join(values));
        }
    }
    
    public IngestPolicyEnforcer getPolicyEnforcer() {
        return ingestPolicyEnforcer;
    }
    
    public void setPolicyEnforcer(IngestPolicyEnforcer ingestPolicyEnforcer) {
        this.ingestPolicyEnforcer = ingestPolicyEnforcer;
    }
}
