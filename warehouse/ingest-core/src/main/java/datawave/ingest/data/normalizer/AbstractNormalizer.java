package datawave.ingest.data.normalizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.data.normalizer.NormalizationException;
import datawave.ingest.data.Type;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

public abstract class AbstractNormalizer implements TextNormalizer {
    private static final Logger log = Logger.getLogger(AbstractNormalizer.class);
    
    @Override
    public void setup(Type type, String instance, Configuration config) {}
    
    /**
     * A factory method to create and configure a normalizer given a class name
     *
     * @param type
     *            the type
     * @param instance
     *            name of the instance
     * @param config
     *            configuration to use
     * @param normalizerClass
     *            the normalizerClass to set up
     * @return An configured instance of the normalizerClass
     */
    public static TextNormalizer createNormalizer(Type type, String instance, Configuration config, String normalizerClass) {
        Class<?> c;
        try {
            c = Class.forName(normalizerClass);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Error finding class " + normalizerClass, e);
        }
        Object o;
        try {
            o = c.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            log.warn("Error creating Normalizer: {}", e);
            throw new IllegalArgumentException("Error creating instance of class " + normalizerClass + ':' + e.getLocalizedMessage(), e);
        }
        if (o instanceof TextNormalizer) {
            // setup the normalizer
            ((TextNormalizer) o).setup(type, instance, config);
        } else {
            throw new IllegalArgumentException(normalizerClass + " is not an instance of " + TextNormalizer.class.getName());
        }
        return (TextNormalizer) o;
    }
    
    /**
     * Convert a field value to its normalized form.
     * 
     * @param fieldName
     *            the field name
     * @param fieldValue
     *            the field value
     * @return the normalized field value, or the original field value otherwise.
     * @throws NormalizationException
     *             if the value cannot be normalized
     */
    public abstract String convertFieldValue(String fieldName, String fieldValue) throws NormalizationException;
    
    /**
     * Convert a field value regex to work against its normalized form.
     * 
     * @param fieldName
     *            the field name
     * @param fieldRegex
     *            regex to check for a match
     * @return the normalized field value, or the original field value otherwise.
     * @throws NormalizationException
     *             if the value cannot be normalized
     */
    public abstract String convertFieldRegex(String fieldName, String fieldRegex) throws NormalizationException;
    
    /**
     * A convenience routine to get a configuration value
     *
     * @param type
     *            the type
     * @param instance
     *            name of the instance
     * @param config
     *            the configuration
     * @param key
     *            the key to pull from
     * @param defaultVal
     *            default value to return
     * @return The value, null if not available
     */
    protected String get(Type type, String instance, Configuration config, String key, String defaultVal) {
        for (String prefix : getConfPrefixes(type, instance)) {
            String value = config.get(prefix + key, null);
            if (value != null) {
                return value;
            }
        }
        return defaultVal;
    }
    
    /**
     * A convenience routine to get a configuration value
     *
     * @param type
     *            the type
     * @param instance
     *            name of the instance
     * @param config
     *            the configuration to use
     * @param key
     *            the key to pull from
     * @param defaultVal
     *            default boolean to return
     * @return The value, null if not available
     */
    protected boolean getBoolean(Type type, String instance, Configuration config, String key, boolean defaultVal) {
        for (String prefix : getConfPrefixes(type, instance)) {
            String value = config.get(prefix + key, null);
            if (value != null) {
                return Boolean.valueOf(value);
            }
        }
        return defaultVal;
    }
    
    /**
     * A convenience routine to get a configuration value
     *
     * @param type
     *            the name of the type
     * @param instance
     *            the name of the instance
     * @param config
     *            the configuration to use
     * @param key
     *            the key to draw from
     * @param defaultVal
     *            default value to return
     * @return The value, null if not available
     */
    protected String[] getStrings(Type type, String instance, Configuration config, String key, String[] defaultVal) {
        for (String prefix : getConfPrefixes(type, instance)) {
            String[] value = config.getStrings(prefix + key, (String[]) null);
            if (value != null) {
                return value;
            }
        }
        return defaultVal;
    }
    
    /**
     * Get the configuration key prefixes in precedence order: &lt;datatype&gt;.&lt;classname&gt;.&lt;instance&gt; &lt;datatype&gt;.&lt;classname&gt;
     * &lt;datatype&gt;.&lt;instance&gt; &lt;datatype&gt; all.&lt;classname&gt; all
     *
     * @param type
     *            the name of the type
     * @param instance
     *            the name of the instance
     * @return a list of the configuration prefixes
     */
    protected String[] getConfPrefixes(Type type, String instance) {
        List<String> prefixes = new ArrayList<>();
        // type specific ones first, then the "all" ones
        prefixes.addAll(Arrays.asList(getConfPrefixes(type.typeName(), instance)));
        prefixes.addAll(Arrays.asList(getConfPrefixes("all", null)));
        return prefixes.toArray(new String[prefixes.size()]);
    }
    
    private String[] getConfPrefixes(String type, String instance) {
        StringBuilder builder = new StringBuilder();
        builder.append(type);
        if (instance != null) {
            // <datatype>
            String str1 = builder.toString();
            builder.append('.').append(instance);
            // <datatype>.<instance>
            String str2 = builder.toString();
            builder.setLength(builder.length() - instance.length());
            builder.append(this.getClass().getSimpleName());
            // <datatype>.<classname>
            String str3 = builder.toString();
            builder.append('.').append(instance);
            // <datatype>.<classname>.<instance>
            String str4 = builder.toString();
            return new String[] {str4, str3, str2, str1};
        } else {
            // all
            String str1 = builder.toString();
            builder.append('.').append(this.getClass().getSimpleName());
            // all.<classname>
            String str2 = builder.toString();
            return new String[] {str2, str1};
        }
    }
    
    @Override
    public String normalizeFieldValue(String field, String value) throws NormalizationException {
        return convertFieldValue(field, value);
    }
    
    @Override
    public String normalizeFieldRegex(String field, String regex) throws NormalizationException {
        return convertFieldRegex(field, regex);
    }
    
    @Override
    public NormalizedContentInterface normalize(NormalizedContentInterface field) {
        NormalizedFieldAndValue n = new NormalizedFieldAndValue(field);
        try {
            n.setIndexedFieldValue(convertFieldValue(field.getIndexedFieldName(), field.getIndexedFieldValue()));
        } catch (NormalizationException e) {
            if (field.getEventFieldName().equals("IP_GEO_FM_COORDINATES") && field.getEventFieldValue().equals("-99.999/-999.999")) {
                log.warn("Found know bad default value: IP_GEO_FM_COORDINATES=-99.999/-999.999");
            } else {
                log.error("Failed to normalize " + field.getEventFieldName() + '=' + field.getEventFieldValue(), e);
            }
            n.setError(e);
        }
        return n;
    }
    
    @Override
    public Multimap<String,NormalizedContentInterface> normalize(Multimap<String,String> fields) {
        Multimap<String,NormalizedContentInterface> results = HashMultimap.create();
        // if handling all fields, then we need to navigate the entire list
        for (Entry<String,String> field : fields.entries()) {
            if (field.getValue() != null) {
                NormalizedContentInterface normalizedContent = null;
                try {
                    normalizedContent = normalize(new NormalizedFieldAndValue(field.getKey(), field.getValue()));
                } catch (Exception e) {
                    log.error("Failed to normalize " + field.getKey() + '=' + field.getValue(), e);
                    normalizedContent = new NormalizedFieldAndValue(field.getKey(), field.getValue());
                    normalizedContent.setError(e);
                }
                results.put(normalizedContent.getIndexedFieldName(), normalizedContent);
            }
        }
        return results;
    }
    
    @Override
    public Multimap<String,NormalizedContentInterface> normalizeMap(Multimap<String,NormalizedContentInterface> fields) {
        Multimap<String,NormalizedContentInterface> results = HashMultimap.create();
        // if handling all fields, then we need to navigate the entire list
        for (Entry<String,NormalizedContentInterface> field : fields.entries()) {
            if (field.getValue() != null) {
                NormalizedContentInterface normalizedContent = field.getValue();
                try {
                    normalizedContent = normalize(field.getValue());
                } catch (Exception e) {
                    log.error("Failed to normalize " + field.getValue().getIndexedFieldName() + '=' + field.getValue().getIndexedFieldValue(), e);
                    normalizedContent.setError(e);
                }
                results.put(normalizedContent.getIndexedFieldName(), normalizedContent);
            }
        }
        return results;
    }
    
    @Override
    public int hashCode() {
        // Use the concrete TextNormalizer's full name to ensure that we don't get multiple
        // instances of the same class (as Object#hashCode is based on virtual memory location)
        return this.getClass().getName().hashCode();
    }
    
    @Override
    public boolean equals(Object o) {
        if (o == null)
            return false;
        Class<?> otherClz = o.getClass();
        
        // Since TextNormalizers are considered to be stateless,
        // we can treat equality as the same class
        return otherClz.equals(this.getClass());
        
    }
}
