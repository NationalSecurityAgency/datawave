package datawave.ingest.data.config.ingest;

import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import datawave.data.type.NoOpType;
import datawave.data.type.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.ConfigurationHelper;
import datawave.ingest.data.config.DataTypeHelper;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

/**
 * This class can be used to help normalize the event field values akin to how the BaseIngestHelper can normalize the indexed field values. This was not cooked
 * into the BaseIngestHelper because this should never be done except in rare circumstances. In general, the data presented for ingest is the data that should
 * be preserved. However, in some cases we need to normalize some of the values to ensure the values are dedupped appropriately between two corresponding input
 * data formats, ie, distinct formats generated from the same instance of a raw data artifact.
 *
 * To use this class, construct it in the setup(config) of your ingest helper:
 *
 * public void setup(Configuration config) { ... eventFieldNormalizerHelper = new EventFieldNormalizerHelper(config); ... }
 *
 * then override the normalize(NormalizedContentInterface) as follows:
 *
 * public NormalizedContentInterface normalize(NormalizedContentInterface nci) {
 *
 * // normalize the event field value as required TextNormalizer n = eventFieldNormalizerHelper.getNormalizer(nci.getEventFieldName());
 * nci.setEventFieldValue(n.normalizeFieldValue(nci.getEventFieldName(), nci.getEventFieldValue()));
 *
 * // now normalize the index field value as required return super.normalize(nci); }
 *
 */
public class EventFieldNormalizerHelper {

    private datawave.ingest.data.Type type = null;
    private TypeRegistry registry = null;

    /* Map of field names to types, null key is the default type */
    private Map<String,Type<?>> typeFieldMap = null;
    private Map<String,Type<?>> typePatternMap = null;
    private Map<Pattern,Type<?>> typeCompiledPatternMap = null;
    private static final Type<?> NO_OP_TYPE = new NoOpType();

    private static final Logger log = Logger.getLogger(EventFieldNormalizerHelper.class);

    /**
     *
     * Configuration parameter to specify the name of the normalizer that should be used to normalize the event field. This parameter supports multiple
     * datatypes and fields, so a valid value would be something like mydatatype.somefieldname.event.field.type.class
     */
    public static final String FIELD_TYPE = ".event.field.type.class";

    public EventFieldNormalizerHelper(Configuration config) {
        String t = ConfigurationHelper.isNull(config, DataTypeHelper.Properties.DATA_NAME, String.class);
        if (registry == null) {
            registry = TypeRegistry.getInstance(config);
        }
        type = TypeRegistry.getType(t);

        // Create the normalizers
        typeFieldMap = Maps.newHashMap();
        typePatternMap = Maps.newHashMap();

        for (Entry<String,String> property : config) {

            // Make sure we are only processing normalizers for this type
            if (!property.getKey().startsWith(this.getType().typeName() + '.'))
                continue;

            String fieldName = null;
            if (property.getKey().endsWith(FIELD_TYPE)) {
                if ((fieldName = getFieldName(property.getKey(), FIELD_TYPE)) == null) {
                    continue;
                }
                String normalizerClass = property.getValue();
                Type<?> normalizer = Type.Factory.createType(normalizerClass);
                // Add the normalizer to the map, the null key is the default key
                if (fieldName.indexOf('*') >= 0) {
                    typePatternMap.put(fieldName, normalizer);
                } else {
                    typeFieldMap.put(fieldName, normalizer);
                }
                log.debug("Registered a " + normalizerClass + " for type[" + this.getType().typeName() + "], EVENT (not index) field[" + fieldName + "]");
            }
        }
    }

    /**
     * Get the normalizer for the event field value
     *
     * @param fieldName
     *            the name of the field
     * @return the normalizer. NoOpNormalizer instance of none is configured.
     */
    public Type<?> getType(String fieldName) {
        fieldName = fieldName.toUpperCase();

        // first look for a field specific normalizer
        Type<?> normer = typeFieldMap.get(fieldName);

        // then look for a pattern
        if (normer == null) {
            if (typeCompiledPatternMap == null)
                compilePatterns();
            if (!typeCompiledPatternMap.isEmpty()) {
                for (Pattern pattern : typeCompiledPatternMap.keySet()) {
                    if (pattern.matcher(fieldName).matches()) {
                        normer = typeCompiledPatternMap.get(pattern);
                        break;
                    }
                }
            }
        }

        if (normer == null) {
            normer = NO_OP_TYPE;
        }

        return normer;
    }

    private void compilePatterns() {
        Map<Pattern,Type<?>> patterns = Maps.newHashMap();
        if (typePatternMap != null) {
            for (String pattern : typePatternMap.keySet()) {
                patterns.put(Pattern.compile(pattern.replace("*", ".*")), typePatternMap.get(pattern));
            }
        }
        typeCompiledPatternMap = patterns;
    }

    /**
     * Get a field name from a property name given the pattern. Returns null if not an actually match
     *
     * @param property
     *            the property name to check
     * @param propertyPattern
     *            the pattern of the property name to match against
     * @return the field name extracted from the property name
     */
    protected String getFieldName(String property, String propertyPattern) {
        String fieldName = property.substring(this.getType().typeName().length() + 1, property.length() - propertyPattern.length());
        // if the field name extracted contains a '.', then there is another typeName out there that
        // starts with this typeName but has multiple parts. So ignore this entry
        // and continue.
        if (fieldName.indexOf('.') >= 0) {
            // if this type already has a '.', then we have a malformed property name
            if (this.getType().typeName().indexOf('.') >= 0) {
                log.error(propertyPattern + " property malformed: " + property);
                throw new IllegalArgumentException(propertyPattern + " property malformed: " + property);
            }
            fieldName = null;
        } else {
            fieldName = fieldName.toUpperCase();
        }
        return fieldName;
    }

    public datawave.ingest.data.Type getType() {
        return type;
    }

    public TypeRegistry getRegistry() {
        return registry;
    }

    public Map<String,Type<?>> getTypeFieldMap() {
        return typeFieldMap;
    }

    public Map<String,Type<?>> getTypePatternMap() {
        return typePatternMap;
    }

    public Map<Pattern,Type<?>> getTypeCompiledPatternMap() {
        return typeCompiledPatternMap;
    }

}
