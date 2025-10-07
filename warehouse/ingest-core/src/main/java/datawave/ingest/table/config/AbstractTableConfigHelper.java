package datawave.ingest.table.config;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.constraints.DefaultKeySizeConstraint;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import datawave.ingest.table.aggregator.CombinerConfiguration;
import datawave.iterators.PropogatingIterator;

public abstract class AbstractTableConfigHelper implements TableConfigHelper {

    public static final String DISABLE_VERSIONING_ITERATOR = ".disable.versioning.iterator";
    protected Configuration config;

    protected AbstractTableConfigHelper() {}

    @Override
    public abstract void setup(String tableName, Configuration config, Logger log) throws IllegalArgumentException;

    @Override
    public abstract void configure(TableOperations tops) throws AccumuloException, AccumuloSecurityException, TableNotFoundException;

    /**
     * Sets {@code propertyName} to {@code propertyValue} on table {@code tableName}, unless the property is already set to {@code propertyValue}.
     *
     * @param tableName
     *            the name of the table whose properties are to be modified
     * @param propertyName
     *            the name of the property to conditionally set
     * @param propertyValue
     *            the value to which {@code propertyName} will be set
     * @param tops
     *            accumulo table operations helper for configuring tables
     * @param log
     *            a {@link Logger} for diagnostic messages
     *
     * @throws AccumuloException
     *             for issues with accumulo
     * @throws AccumuloSecurityException
     *             for issues authenticating with accumulo
     * @throws TableNotFoundException
     *             if the table is not found
     */
    public static void setPropertyIfNecessary(String tableName, String propertyName, String propertyValue, TableOperations tops, Logger log)
                    throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

        boolean propertySet = false;
        for (Entry<String,String> prop : tops.getProperties(tableName)) {
            if (prop.getKey().equals(propertyName) && prop.getValue().equals(propertyValue)) {
                propertySet = true;
                break;
            }
        }
        if (!propertySet) {
            tops.setProperty(tableName, propertyName, propertyValue);
        }
    }

    /**
     * Sets the aggregator configuration on table {@code tableName} to that contained in {@code aggregators}, if {@code tableName} is not already configured
     * with the specified aggregators.
     *
     * @param tableName
     *            the name of the table whose configuration is to be modified
     * @param aggregators
     *            the aggregators that should be set on {@code tableName}
     * @param tops
     *            accumulo table operations helper for configuring tables
     * @param config
     *            the configuration
     * @param log
     *            a {@link Logger} for diagnostic messages
     *
     * @throws AccumuloException
     *             for issues with accumulo
     * @throws AccumuloSecurityException
     *             for issues authenticating with accumulo
     * @throws TableNotFoundException
     *             if the table is not found
     */
    protected void setAggregatorConfigurationIfNecessary(String tableName, List<CombinerConfiguration> aggregators, TableOperations tops, Configuration config,
                    Logger log) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        if (areAggregatorsConfigured(tableName, aggregators, tops, config)) {
            log.debug(tableName + " appears to have its aggregators configured already.");
            return;
        }

        log.info("Configuring aggregators for " + tableName);
        Map<String,String> props = generateInitialTableProperties(config, tableName);
        props.putAll(generateAggTableProperties(aggregators));
        for (Entry<String,String> prop : props.entrySet()) {
            tops.setProperty(tableName, prop.getKey(), prop.getValue());
        }
    }

    /**
     * Copied from Accumulo 1.9 IteratorUtil
     *
     * @param config
     *            the configuration
     * @param tableName
     *            the table name
     *
     * @return a map of the table properties
     */
    public static Map<String,String> generateInitialTableProperties(Configuration config, String tableName) {
        TreeMap<String,String> props = new TreeMap<>();

        boolean disableVersioning = config != null && config.getBoolean(tableName + DISABLE_VERSIONING_ITERATOR, false);
        if (!disableVersioning) {
            for (IteratorScope iterScope : IteratorScope.values()) {
                props.put(Property.TABLE_ITERATOR_PREFIX + iterScope.name() + ".vers", "20," + VersioningIterator.class.getName());
                props.put(Property.TABLE_ITERATOR_PREFIX + iterScope.name() + ".vers.opt.maxVersions", "1");
            }
        }

        props.put(Property.TABLE_CONSTRAINT_PREFIX + "1", DefaultKeySizeConstraint.class.getName());

        return props;
    }

    /**
     * Indicates whether or not the aggregators specified in {@code aggregators} have been configured on the table {@code tableName}. Note that this does not
     * check whether other aggregators are configured on {@code tableName} are configured, only whether the specified ones are or are not.
     *
     * @param tableName
     *            the name of the table to check for {@code aggregators}
     * @param aggregators
     *            the aggregators to check for on {@code tableName}
     * @param tops
     *            accumulo table operations helper for configuring tables
     * @param config
     *            the configuration
     * @return {@code true} if {@code aggregators} are configured on {@code tableName} and {@code false} if not
     *
     * @throws TableNotFoundException
     *             if the table is not found
     */
    protected boolean areAggregatorsConfigured(String tableName, List<CombinerConfiguration> aggregators, TableOperations tops, Configuration config)
                    throws TableNotFoundException {
        boolean aggregatorsConfigured = false;
        Map<String,String> props = generateInitialTableProperties(config, tableName);
        props.putAll(generateAggTableProperties(aggregators));
        Iterable<Entry<String,String>> properties;
        try {
            properties = tops.getProperties(tableName);
        } catch (Exception ex) {
            throw new RuntimeException("Unexpected error checking on aggregators", ex);
        }
        for (Entry<String,String> entry : properties) {
            String key = entry.getKey();
            String actualValue = entry.getValue();

            // Removes the properties already defined on the table
            // from set of properties being defined by the caller
            String requiredValue = props.remove(key);

            // Check to make sure that caller's expected settings are
            // actually going to be set on the table
            if (requiredValue != null && !requiredValue.equals(actualValue)) {
                // mismatch in value -- put it back in the props map
                props.put(key, actualValue);
                break;
            }
        }
        // We removed each property from the map if it was already set on the table.
        // So, if the aggregators are configured on the table, the props map should be
        // empty when we're done iterating over the table configuration.
        aggregatorsConfigured = props.isEmpty();
        return aggregatorsConfigured;
    }

    /**
     * Set the locality group configuration for a table if necessary. If the specified configuration is not already included in the current group configuration,
     * then the new locality groups are merged with the current set and the locality groups are reset for the table.
     *
     * @param tableName
     *            the table name
     * @param newLocalityGroups
     *            map of the locality groups
     * @param tops
     *            the table operations
     * @param log
     *            a logger
     * @throws AccumuloException
     *             for issues with accumulo
     * @throws AccumuloSecurityException
     *             for issues authenticating with accumulo
     * @throws TableNotFoundException
     *             if the table is not found
     */
    protected void setLocalityGroupConfigurationIfNecessary(String tableName, Map<String,Set<Text>> newLocalityGroups, TableOperations tops, Logger log)
                    throws AccumuloException, TableNotFoundException, AccumuloSecurityException {
        if (areLocalityGroupsConfigured(tableName, newLocalityGroups, tops)) {
            log.debug("Verified the following locality groups are configured for " + tableName + ": " + newLocalityGroups);
            return;
        }

        log.info("Creating the locality groups for " + tableName + ": " + newLocalityGroups);
        Map<String,Set<Text>> localityGroups = tops.getLocalityGroups(tableName);
        for (Map.Entry<String,Set<Text>> entry : newLocalityGroups.entrySet()) {
            Set<Text> families = localityGroups.get(entry.getKey());
            if (families == null) {
                families = new HashSet<>();
                localityGroups.put(entry.getKey(), families);
            }
            families.addAll(entry.getValue());
        }
        tops.setLocalityGroups(tableName, localityGroups);
        log.info("Reset the locality groups for " + tableName + " to " + localityGroups);
    }

    /**
     * Is the specified configuration already included in the current table configuration for locality groups.
     *
     * @param tableName
     *            the table name
     * @param newLocalityGroups
     *            map of the locality group
     * @param tops
     *            the table operations
     * @return true if the new configuration is already included in the current configuration
     * @throws AccumuloException
     *             for issues with accumulo
     * @throws AccumuloSecurityException
     *             for issues authenticating with accumulo
     * @throws TableNotFoundException
     *             if the table is not found
     */
    protected boolean areLocalityGroupsConfigured(String tableName, Map<String,Set<Text>> newLocalityGroups, TableOperations tops)
                    throws AccumuloException, TableNotFoundException, AccumuloSecurityException {
        Map<String,Set<Text>> localityGroups = tops.getLocalityGroups(tableName);
        for (Map.Entry<String,Set<Text>> entry : newLocalityGroups.entrySet()) {
            Set<Text> families = localityGroups.get(entry.getKey());
            if (families == null) {
                return false;
            }
            if (!families.containsAll(entry.getValue())) {
                return false;
            }
        }
        return true;
    }

    public static Map<String,String> generateAggTableProperties(List<CombinerConfiguration> aggregators) {

        Map<String,String> props = new TreeMap<>();

        for (IteratorScope iterScope : IteratorScope.values()) {
            if (!aggregators.isEmpty()) {
                props.put(Property.TABLE_ITERATOR_PREFIX + iterScope.name() + ".agg", "10," + PropogatingIterator.class.getName());
            }
        }

        for (CombinerConfiguration ac : aggregators) {
            for (IteratorSetting.Column column : ac.getColumns()) {
                for (IteratorScope iterScope : IteratorScope.values()) {

                    props.put(Property.TABLE_ITERATOR_PREFIX + iterScope.name() + ".agg.opt." + column.getColumnFamily(), ac.getSettings().getIteratorClass());
                }
            }
        }

        return props;
    }

}
