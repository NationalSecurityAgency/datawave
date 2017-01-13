package nsa.datawave.query.filter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nsa.datawave.query.parser.EventFields;
import nsa.datawave.query.parser.QueryEvaluator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.log4j.Logger;

/**
 * <p>
 * A driver class for handling multiple {@link DataFilter}s. Initialized with a SortedKeyValueIterator configured on the table, an iterator environment, a
 * <code>String[]</code> of {@link DataFilter} class names, the submitted query, and a <code>String[]</code> of unevaluated fields for the query.
 * </p>
 * 
 * <p>
 * For both {@link #accept(Key, Value)} and {@link #getContextMap(Key, Value)}, all of the filters will be run over the given key value pair. DataFilters will
 * be run in the order that they were provided to this class. For accept(), if and only if all of the DataFilters return true on their accept methods will it
 * also return true. If the accept() method on any DataFilter returns false, the key-value pair must be filtered out. Accept() fails fast, meaning that once one
 * DataFilter returns false on its accept method, accept immediately returns false.
 * </p>
 * 
 * <p>
 * For getContextMap(), all of the DataFilters' getContextMap calls will be called. Evaluation is not done on the key value pair, so all DataFilters must run
 * over the key value pair. It is up to the DataFilter itself to fail fast if it can be determined that the key value pair should be filtered out and return a
 * null or empty map.
 * </p>
 * 
 * 
 * 
 */
public class FilteringMaster {
    private static final Logger log = Logger.getLogger(FilteringMaster.class);
    
    private List<DataFilter> filters = null;
    
    public FilteringMaster(SortedKeyValueIterator<Key,Value> source, IteratorEnvironment env, String[] classNames, Map<String,Object> options,
                    QueryEvaluator evaluator) {
        this.filters = new ArrayList<>();
        
        // Instantiate each DataEnricher class
        for (String className : classNames) {
            try {
                Class<?> clz = Class.forName(className);
                
                if (!DataFilter.class.isAssignableFrom(clz)) {
                    log.warn("Ignoring filter class '" + className + "' as it does not extend DataFilter");
                    continue;
                }
                
                DataFilter filter = (DataFilter) clz.newInstance();
                
                try {
                    filter.init(source.deepCopy(env), options, evaluator);
                } catch (ParseException e) {
                    log.error("Could not init " + className + ".", e);
                    continue;
                }
                
                if (log.isDebugEnabled()) {
                    log.debug("Adding " + filter.getClass().getName() + " to the list of filters");
                }
                
                filters.add(filter);
            } catch (ClassNotFoundException e) {
                log.error("ClassNotFoundException when trying to instantiate the enrichers.", e);
                log.error("Ignoring provided enricher class name: " + className);
            } catch (InstantiationException e) {
                log.error("InstantiationException when trying to instantiate the enrichers.", e);
                log.error("Ignoring provided enricher class name: " + className);
            } catch (IllegalAccessException e) {
                log.error("IllegalAccessException when trying to instantiate the enrichers.", e);
                log.error("Ignoring provided enricher class name: " + className);
            }
        }
    }
    
    /**
     * Given the provided key and value, call the accept method on each configured {@link DataFilter}. This method will fail fast when the first filter rejects
     * a key value pair.
     * 
     * @param key
     *            The key to filter
     * @param value
     *            The value to filter
     * @return True if all of the {@link DataFilter}s accepted the key-value pair, false otherwise.
     */
    public boolean accept(Key key, Value value) {
        if (key == null && value == null) {
            return false;
        }
        
        for (DataFilter filter : this.filters) {
            boolean accept = filter.accept(key, value);
            
            if (!accept) {
                if (log.isDebugEnabled()) {
                    log.debug("Received false from " + filter.getClass().getName() + ". Invalidating the current key/value pair");
                }
                
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * Given a populated, {@link EventFields} object, {@link #accept(EventFields)} determines if the event should be returned to the caller or be filtered out
     * and not returned.
     *
     * @param event
     *            The current aggregated {@link EventFields}
     * @return True if the value should be returned to the caller, false otherwise
     */
    public boolean accept(EventFields event) {
        if (event == null) {
            return false;
        }
        
        for (DataFilter filter : this.filters) {
            if (!(filter instanceof DataEventFilter))
                continue;
            
            boolean accept = ((DataEventFilter) filter).accept(event);
            
            if (!accept) {
                if (log.isDebugEnabled()) {
                    log.debug("Received false from " + filter.getClass().getName() + ". Invalidating the current event");
                }
                
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * Runs the provided key and value through the getContextMap method of each configured {@link DataFilter} and merges all of the output together into one
     * <code>Map&lt;String, Object&gt;</code>.
     * 
     * If there is a duplicate context map key generated by more than one {@link DataFilter}, the <code>Object</code> generated by the last {@link DataFilter}
     * for a duplicate context map key will be the value that is returned.
     * 
     * @param key
     * @param value
     * @return
     */
    public Map<String,Object> getContextMap(Key key, Value value) {
        if (key == null && value == null) {
            if (log.isDebugEnabled()) {
                log.debug("Returning null from getContextMap because of a null key and value");
            }
            return null;
        }
        
        Map<String,Object> allFiltersMap = new HashMap<>();
        
        for (DataFilter filter : this.filters) {
            Map<String,Object> tempMap = filter.getContextMap(key, value);
            
            if (tempMap != null) {
                for (String mapKey : tempMap.keySet()) {
                    if (allFiltersMap.containsKey(mapKey)) {
                        log.warn("The " + filter.getClass().getName() + " filter is going to overwrite an existing value in the context map for the key '"
                                        + mapKey + "'!!");
                    }
                }
                
                allFiltersMap.putAll(tempMap);
            }
        }
        
        return allFiltersMap;
    }
}
