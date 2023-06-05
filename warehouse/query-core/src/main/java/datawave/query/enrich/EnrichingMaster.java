package datawave.query.enrich;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.log4j.Logger;

/**
 * <p>
 * A driver class for handling multiple {@link DataEnricher}s. The source must be configured directly onto the underlying tablet. In other words, the source
 * cannot have another custom iterator in between itself and the first configured iterator on the table. The enricher must have full free to seek anywhere
 * inside of the current tablet, and thus a sub-iterator cannot impede its ability to do so.
 * </p>
 * 
 * <p>
 * A list of class names is passed into the EnrichingMaster which get instantitated and added to a list of DataEnrichers. The provided class name must implement
 * DataEnricher or else it will be ignored. When instantiating each DataEnricher, the provided source will be deepCopy'd
 * </p>
 * 
 * <p>
 * After init() is called, the user will call the enrich method with the current key-value pair. Each DataEnricher's enrich method will be run on the provided
 * pair. If a DataEnricher alters the value, the new value will be used for the next DataEnricher. It is up to the DataEnricher to fail fast, as each configured
 * DataEnricher will be run over each key-value
 * </p>
 * 
 * 
 * 
 */
public class EnrichingMaster {
    private static final Logger log = Logger.getLogger(EnrichingMaster.class);
    
    public static final String ENRICHMENT_ENABLED = "enrichment.enabled";
    public static final String ENRICHMENT_CLASSES = "enrichment.classes";
    public static final String QUERY = "query";
    public static final String UNEVALUATED_FIELDS = "enriching.unevaluated.fields";
    
    private List<DataEnricher> enrichers = null;
    private Value topValue = null;
    
    /**
     * Instantiates each DataEnricher with the class names provided, deepCopy'ing the source and passing along the options map.
     * 
     * Data enrichers will be instantiated in the order the class names are provided and any class name that does not implement DataEnricher will be ignored (a
     * log message will be generated).
     * 
     * @param source
     *            the iterator source
     * @param env
     *            the iterator environment
     * @param classNames
     *            list of class names
     * @param options
     *            mapping of options
     */
    public EnrichingMaster(SortedKeyValueIterator<Key,Value> source, IteratorEnvironment env, String[] classNames, Map<String,Object> options) {
        this.enrichers = new ArrayList<>();
        
        // Instantiate each DataEnricher class
        for (String className : classNames) {
            try {
                Class<?> clz = Class.forName(className);
                
                if (clz.isAssignableFrom(DataEnricher.class)) {
                    log.warn("Ignoring enrichment class '" + className + "' as it does not extend DataEnricher");
                    continue;
                }
                
                DataEnricher enricher = (DataEnricher) clz.getDeclaredConstructor().newInstance();
                try {
                    enricher.init(source.deepCopy(env), options, env);
                } catch (ParseException e) {
                    log.error("Could not init " + className + ".", e);
                    continue;
                }
                
                enrichers.add(enricher);
            } catch (ClassNotFoundException e) {
                log.error("ClassNotFoundException when trying to instantiate the enrichers.", e);
                log.error("Ignoring provided enricher class name: " + className);
            } catch (InstantiationException | NoSuchMethodException | InvocationTargetException e) {
                log.error("InstantiationException when trying to instantiate the enrichers.", e);
                log.error("Ignoring provided enricher class name: " + className);
            } catch (IllegalAccessException e) {
                log.error("IllegalAccessException when trying to instantiate the enrichers.", e);
                log.error("Ignoring provided enricher class name: " + className);
            }
        }
    }
    
    /**
     * Calls the enrich method on each configured DataEnricher. Uses the enriched value for each new DataEnricher and sets topValue after all enrichers have
     * been run.
     * 
     * @param key
     *            the key
     * @param value
     *            the value
     */
    public void enrich(Key key, Value value) {
        if (key == null && value == null) {
            log.warn("Received null key and value, setting topValue to null and exiting");
            this.topValue = null;
            
            return;
        }
        
        for (DataEnricher enricher : this.enrichers) {
            boolean workDone = enricher.enrich(key, value);
            
            if (!workDone) {
                continue;
            }
            
            if (log.isTraceEnabled()) {
                log.trace("Updating the current value to: " + enricher.getEnrichedValue());
            }
            
            // Get the updated value, continue enriching
            value = enricher.getEnrichedValue();
        }
        
        this.topValue = value;
    }
    
    public Value getValue() {
        return this.topValue;
    }
}
