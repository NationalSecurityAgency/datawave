package datawave.ingest.table.config;

import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialTemporalDimensionalityTypeProvider;
import mil.nga.giat.geowave.datastore.accumulo.BasicOptionProvider;
import mil.nga.giat.geowave.datastore.accumulo.IteratorConfig;
import mil.nga.giat.geowave.datastore.accumulo.MergingCombiner;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.ingest.mapreduce.handler.geowave.GeoWaveDataTypeHandler;
import datawave.ingest.metadata.GeoWaveMetadata;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.conf.ColumnSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GeoWaveTableConfigHelper implements TableConfigHelper {
    
    private static final int STATS_COMBINER_PRIORITY = 10;
    private static final String STATISTICS_COMBINER_NAME = "STATS_COMBINER";
    
    private enum GeoWaveTableType {
        UNKNOWN, GEOWAVE_METADATA, SPATIAL_IDX, SPATIAL_TEMPORAL_IDX
    }
    
    private Logger log = null;
    private Configuration conf = null;
    private String tableName = null;
    private String adapterId = null;
    
    private GeoWaveTableType tableType = GeoWaveTableType.UNKNOWN;
    
    @Override
    public void setup(final String tableName, final Configuration config, final Logger log) throws IllegalArgumentException {
        this.log = log;
        conf = config;
        this.tableName = tableName;
        
        final TypeRegistry registry = TypeRegistry.getInstance(conf);
        final String dataName = conf.get(DataTypeHelper.Properties.DATA_NAME_OVERRIDE, conf.get(DataTypeHelper.Properties.DATA_NAME));
        final Type type = registry.get(dataName);
        
        adapterId = conf.get(type.typeName() + GeoWaveDataTypeHandler.GEOWAVE_FEATURE_TYPE_NAME);
        
        // make sure that this is a valid geowave table
        if (tableName.endsWith(GeoWaveMetadata.METADATA_TABLE)) {
            tableType = GeoWaveTableType.GEOWAVE_METADATA;
        } else if (tableName.endsWith(new SpatialDimensionalityTypeProvider().createPrimaryIndex().getId().getString())) {
            tableType = GeoWaveTableType.SPATIAL_IDX;
        } else if (tableName.endsWith(new SpatialTemporalDimensionalityTypeProvider().createPrimaryIndex().getId().getString())) {
            tableType = GeoWaveTableType.SPATIAL_TEMPORAL_IDX;
        } else {
            throw new IllegalArgumentException("Invalid GeoWave table [" + tableName + "]");
        }
    }
    
    @Override
    public void configure(final TableOperations tops) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        switch (tableType) {
            case GEOWAVE_METADATA:
                configureMetadata(tops);
                break;
            case SPATIAL_IDX:
                configureSpatial(tops);
                break;
            case SPATIAL_TEMPORAL_IDX:
                configureSpatialTemporal(tops);
                break;
            case UNKNOWN:
            default:
                log.error("Unable to configure table [" + tableName + "]");
                break;
        }
    }
    
    private void configureMetadata(final TableOperations tops) {
        // Add Iterators
        final List<IteratorConfig> iterators = new ArrayList<>();
        
        // Stats Merging Iterator
        final IteratorSetting.Column adapterColumn = new IteratorSetting.Column(GeoWaveMetadata.STATISTICS_CF);
        final Map<String,String> options = new HashMap<>();
        options.put(MergingCombiner.COLUMNS_OPTION, ColumnSet.encodeColumns(adapterColumn.getFirst(), adapterColumn.getSecond()));
        iterators.add(new IteratorConfig(EnumSet.allOf(IteratorUtil.IteratorScope.class), STATS_COMBINER_PRIORITY, STATISTICS_COMBINER_NAME,
                        MergingCombiner.class.getName(), new BasicOptionProvider(options)));
        
        try {
            attachIterators(tops, tableName, iterators.toArray(new IteratorConfig[iterators.size()]));
        } catch (final Exception e) {
            log.error("Unable to attach iterators to table [" + tableName + "]", e);
        }
        
        // Configure Locality Groups
        // None!
    }
    
    private void configureSpatial(final TableOperations tops) {
        // Add Iterators
        // None!
        
        // Configure Locality Groups
        try {
            addLocalityGroup(tops, tableName, adapterId);
        } catch (final Exception e) {
            log.error("Unable to add locality group [" + adapterId + "] to table [" + tableName + "]");
        }
    }
    
    private void configureSpatialTemporal(final TableOperations tops) {
        // Add Iterators
        // None!
        
        // Configure Locality Groups
        try {
            addLocalityGroup(tops, tableName, adapterId);
        } catch (final Exception e) {
            log.error("Unable to add locality group [" + adapterId + "] to table [" + tableName + "]");
        }
    }
    
    private void attachIterators(final TableOperations tops, final String tableName, final IteratorConfig[] iterators) throws TableNotFoundException,
                    AccumuloSecurityException, AccumuloException {
        try {
            if ((iterators != null) && (iterators.length > 0)) {
                final Map<String,EnumSet<IteratorUtil.IteratorScope>> iteratorScopes = tops.listIterators(tableName);
                for (final IteratorConfig iteratorConfig : iterators) {
                    boolean mustDelete = false;
                    boolean exists = false;
                    final EnumSet<IteratorUtil.IteratorScope> existingScopes = iteratorScopes.get(iteratorConfig.getIteratorName());
                    EnumSet<IteratorUtil.IteratorScope> configuredScopes;
                    if (iteratorConfig.getScopes() == null) {
                        configuredScopes = EnumSet.allOf(IteratorUtil.IteratorScope.class);
                    } else {
                        configuredScopes = iteratorConfig.getScopes();
                    }
                    Map<String,String> configuredOptions = null;
                    if (existingScopes != null) {
                        if (existingScopes.size() == configuredScopes.size()) {
                            exists = true;
                            for (final IteratorUtil.IteratorScope s : existingScopes) {
                                if (!configuredScopes.contains(s)) {
                                    // this iterator exists with the wrong
                                    // scope, we will assume we want to remove
                                    // it and add the new configuration
                                    log.warn("found iterator '" + iteratorConfig.getIteratorName() + "' missing scope '" + s.name()
                                                    + "', removing it and re-attaching");
                                    
                                    mustDelete = true;
                                    break;
                                }
                            }
                        }
                        if (!existingScopes.isEmpty()) {
                            // see if the options are the same, if they are not
                            // the same, apply a merge with the existing options
                            // and the configured options
                            final Iterator<IteratorUtil.IteratorScope> it = existingScopes.iterator();
                            while (it.hasNext()) {
                                final IteratorUtil.IteratorScope scope = it.next();
                                final IteratorSetting setting = tops.getIteratorSetting(tableName, iteratorConfig.getIteratorName(), scope);
                                if (setting != null) {
                                    final Map<String,String> existingOptions = setting.getOptions();
                                    configuredOptions = iteratorConfig.getOptions(existingOptions);
                                    // we found the setting existing in one
                                    // scope, assume the options are the same
                                    // for each scope
                                    mustDelete = true;
                                    break;
                                }
                            }
                        }
                    }
                    if (mustDelete) {
                        tops.removeIterator(tableName, iteratorConfig.getIteratorName(), existingScopes);
                        exists = false;
                    }
                    if (!exists) {
                        if (configuredOptions == null) {
                            configuredOptions = iteratorConfig.getOptions(new HashMap<>());
                        }
                        tops.attachIterator(tableName, new IteratorSetting(iteratorConfig.getIteratorPriority(), iteratorConfig.getIteratorName(),
                                        iteratorConfig.getIteratorClass(), configuredOptions), configuredScopes);
                    }
                }
            }
        } catch (AccumuloException | AccumuloSecurityException e) {
            log.warn("Unable to create table '" + tableName + "'", e);
        }
    }
    
    private void addLocalityGroup(final TableOperations tops, final String tableName, final String localityGroup) throws AccumuloException,
                    TableNotFoundException, AccumuloSecurityException {
        if (tops.exists(tableName)) {
            final Map<String,Set<Text>> localityGroups = tops.getLocalityGroups(tableName);
            
            final Set<Text> groupSet = new HashSet<>();
            
            groupSet.add(new Text(localityGroup));
            
            localityGroups.put(localityGroup, groupSet);
            
            tops.setLocalityGroups(tableName, localityGroups);
        }
    }
}
