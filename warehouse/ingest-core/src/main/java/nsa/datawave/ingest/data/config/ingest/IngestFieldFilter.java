package nsa.datawave.ingest.data.config.ingest;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import nsa.datawave.ingest.data.Type;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.Map;

/**
 * The field filter can drop fields based on the presence of other preferred fields. This is configuration driven and expects colon delimited pairs, A:B, where
 * if A is present in the fields, then any occurrences of B will be removed.
 * <p>
 * Example:
 * 
 * <pre>
 * {@code
 * 
 *     <property>
 *         <name>datatype1.data.field.filter</name>
 *         <value>KEEP1:DROP1,KEEP2:DROP2</value>
 *     </property>
 * }
 * </pre>
 */
public class IngestFieldFilter {
    
    private static final Logger logger = Logger.getLogger(IngestFieldFilter.class);
    
    public static final String FILTER_FIELD_SUFFIX = ".data.field.filter";
    public static final String PAIR_DELIM = ",";
    public static final String VALUE_DELIM = ":";
    
    private final Type dataType;
    private Map<String,Collection<String>> filters;
    
    public IngestFieldFilter(Type dataType) {
        this.dataType = dataType;
    }
    
    /**
     * Configures the field filter.
     *
     * @param conf
     */
    public void setup(Configuration conf) {
        Multimap<String,String> temp = TreeMultimap.create();
        String fieldsStr = conf.get(dataType.typeName() + FILTER_FIELD_SUFFIX);
        
        if (StringUtils.isNotBlank(fieldsStr)) {
            try {
                for (String pair : fieldsStr.split(PAIR_DELIM)) {
                    String[] tokens = pair.split(VALUE_DELIM);
                    if (tokens.length == 2) {
                        temp.put(tokens[0], tokens[1]);
                    } else {
                        logger.warn("Expected a " + VALUE_DELIM + " delimited pair but received: " + pair + ", ignoring this config.");
                    }
                }
                
            } catch (Exception e) {
                logger.warn("Could not configure with the string: " + fieldsStr + ", Disabling field filtering.");
            }
        }
        
        filters = temp.asMap();
        
        logger.info("Filters for " + dataType.typeName() + ": " + filters);
    }
    
    /**
     * Applies the configured filter rules to the given fields.
     *
     * @param fields
     */
    public void apply(Multimap<String,?> fields) {
        for (Map.Entry<String,Collection<String>> entry : filters.entrySet()) {
            if (fields.containsKey(entry.getKey())) {
                for (String toRemove : entry.getValue()) {
                    fields.removeAll(toRemove);
                }
            }
        }
    }
}
