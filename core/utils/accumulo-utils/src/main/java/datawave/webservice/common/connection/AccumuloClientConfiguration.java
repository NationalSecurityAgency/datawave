package datawave.webservice.common.connection;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.log4j.Logger;

/**
 * This class will capture scanner hints per table and the consistency level per table. This will be used by the WrappedAccumuloClient to setup scanners as they
 * are created.
 */
public class AccumuloClientConfiguration {
    private Logger log = Logger.getLogger(AccumuloClientConfiguration.class);
    private Map<String,Map<String,String>> hintsByTable = new HashMap<>();
    private Map<String,ScannerBase.ConsistencyLevel> consistencyByTable = new HashMap<>();
    
    /**
     * Construct an empty configuration
     */
    public AccumuloClientConfiguration() {}
    
    /**
     * Construct a configuration with the specified hints per table
     * 
     * @param hints
     *            A map of table to the hints map
     */
    public AccumuloClientConfiguration(Map<String,Map<String,String>> hints) {
        this(hints, Collections.emptyMap());
    }
    
    /**
     * Construct a configuration with the specified hints and consistency levels
     * 
     * @param hints
     *            A map of table to hints map
     * @param levels
     *            A map of table to consistency level
     */
    public AccumuloClientConfiguration(Map<String,Map<String,String>> hints, Map<String,ScannerBase.ConsistencyLevel> levels) {
        for (String table : hints.keySet()) {
            putHints(table, hints.get(table));
        }
        for (String table : levels.keySet()) {
            setConsistency(table, levels.get(table));
        }
    }
    
    /**
     * This will apply the configuration to a scanner
     * 
     * @param scanner
     *            The scanner to configure
     * @param tableName
     *            The table name being scanned
     */
    public void apply(ScannerBase scanner, String tableName) {
        if (hintsByTable.containsKey(tableName)) {
            try {
                scanner.setExecutionHints(hintsByTable.get(tableName));
            } catch (Exception e) {
                log.warn("Failed to set execution hints for " + tableName, e);
            }
        }
        if (consistencyByTable.containsKey(tableName)) {
            scanner.setConsistencyLevel(consistencyByTable.get(tableName));
        }
    }
    
    /**
     * This will take a second configuration and overlay it on top of this one.
     * 
     * @param config
     *            The second configuration
     */
    public void applyOverrides(AccumuloClientConfiguration config) {
        for (String table : config.hintsByTable.keySet()) {
            putHints(table, config.hintsByTable.get(table));
        }
        for (String table : config.consistencyByTable.keySet()) {
            setConsistency(table, config.consistencyByTable.get(table));
        }
    }
    
    /**
     * Set the consistency configuration for a table
     * 
     * @param table
     *            The table name
     * @param level
     *            The consistency level
     */
    public void setConsistency(String table, ScannerBase.ConsistencyLevel level) {
        consistencyByTable.put(table, level);
    }
    
    /**
     * Add a configured hint for a table
     * 
     * @param table
     *            The table
     * @param prop
     *            The hint property
     * @param value
     *            The hint value
     */
    public void addHint(String table, String prop, String value) {
        addHints(table, Collections.singletonMap(prop, value));
    }
    
    /**
     * Add a set of hints for a table. Note this will not remove other hints for this table.
     * 
     * @param table
     *            The table
     * @param hints
     *            The hints
     */
    public void addHints(String table, Map<String,String> hints) {
        if (hints != null && !hints.isEmpty()) {
            if (hintsByTable.containsKey(table)) {
                hintsByTable.get(table).putAll(hints);
            } else {
                hintsByTable.put(table, new HashMap<>(hints));
            }
        }
    }
    
    /**
     * Replace a set of hints for a table. This will remove previously configured hints for this table.
     * 
     * @param table
     *            The table
     * @param hints
     *            The hints
     */
    public void putHints(String table, Map<String,String> hints) {
        if (hints == null || hints.isEmpty()) {
            hintsByTable.remove(table);
        } else {
            hintsByTable.put(table, new HashMap<>(hints));
        }
    }
    
}
