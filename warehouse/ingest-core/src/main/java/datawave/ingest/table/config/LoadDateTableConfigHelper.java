package datawave.ingest.table.config;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import datawave.tables.TableName;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.Set;

public class LoadDateTableConfigHelper extends AbstractTableConfigHelper {
    public static final String LOAD_DATES_ENABLED_PROP = "metadata.loaddates.enabled";
    public static final String LOAD_DATES_LOCALITY_GROUP_PROP = "metadata.loaddates.table.locality.groups";
    public static final String LOAD_DATES_TABLE_NAME_PROP = "metadata.loaddates.table.name";
    public static final String LOAD_DATES_TABLE_LOADER_PRIORITY_PROP = "metadata.loaddates.table.loader.priority";
    protected Logger log;
    protected Configuration conf;
    private Map<String,Set<Text>> localityGroups;
    
    @Override
    public void setup(String tableName, Configuration config, Logger log) throws IllegalArgumentException {
        this.log = log;
        this.conf = config;
        this.localityGroups = createMapOfLocalityGroups(config);
    }
    
    @Override
    public void configure(TableOperations tops) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        for (IteratorScope scope : IteratorScope.values()) {
            setCombinerForLoadDateCounts(tops, scope.name());
            configureToDropBadData(tops, scope.name());
        }
        setLocalityGroupConfigurationIfNecessary(getLoadDatesTableName(conf), localityGroups, tops, log);
    }
    
    private Map<String,Set<Text>> createMapOfLocalityGroups(Configuration config) {
        Map<String,Set<Text>> localityGroups = Maps.newHashMap();
        
        String localityPropertyValue = config.get(LOAD_DATES_LOCALITY_GROUP_PROP);
        if (null != localityPropertyValue) {
            for (String rawGroup : localityPropertyValue.split(",")) {
                rawGroup = rawGroup.replace("\\u0000", "\u0000");
                String[] components = rawGroup.split(":");
                String groupName = components[0];
                String[] columnFamiliesStr = components[1].split(";");
                Set<Text> columnFamilies = Sets.newHashSet();
                
                for (String colFamilyStr : columnFamiliesStr) {
                    columnFamilies.add(new Text(colFamilyStr));
                }
                localityGroups.put(groupName, columnFamilies);
            }
        }
        return localityGroups;
    }
    
    public static boolean isLoadDatesEnabled(Configuration conf) {
        return conf.getBoolean(LOAD_DATES_ENABLED_PROP, false);
    }
    
    public static String getLoadDatesTableName(Configuration conf) {
        return conf.get(LOAD_DATES_TABLE_NAME_PROP, TableName.LOAD_DATES);
    }
    
    public static int getLoadDatesTableLoaderPriority(Configuration conf) {
        return conf.getInt(LOAD_DATES_TABLE_LOADER_PRIORITY_PROP, 40);
    }
    
    private void setCombinerForLoadDateCounts(TableOperations tops, String scopeName) throws AccumuloException, AccumuloSecurityException,
                    TableNotFoundException {
        String stem = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX, scopeName, "LoadDateMetadataCombiner");
        String tableName = LoadDateTableConfigHelper.getLoadDatesTableName(conf);
        setPropertyIfNecessary(tableName, stem, "18,org.apache.accumulo.core.iterators.user.SummingCombiner", tops, log);
        setPropertyIfNecessary(tableName, stem + ".opt.all", "true", tops, log);
        setPropertyIfNecessary(tableName, stem + ".opt.type", LongCombiner.Type.VARLEN.name(), tops, log);
    }
    
    // MetricsFileProtoIngestHelper creates nonsense field names, each containing a '.'
    private void configureToDropBadData(TableOperations tops, String scopeName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        String stem = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX, scopeName, "dropBadData");
        String tableName = LoadDateTableConfigHelper.getLoadDatesTableName(conf);
        setPropertyIfNecessary(tableName, stem, "30,org.apache.accumulo.core.iterators.user.RegExFilter", tops, log);
        setPropertyIfNecessary(tableName, stem + ".opt.negate", "true", tops, log);
        setPropertyIfNecessary(tableName, stem + ".opt.rowRegex", ".*\\..*", tops, log);
        setPropertyIfNecessary(tableName, stem + ".opt.encoding", "UTF-8", tops, log);
    }
}
