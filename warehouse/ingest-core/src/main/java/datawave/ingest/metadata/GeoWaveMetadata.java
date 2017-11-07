package datawave.ingest.metadata;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.vividsolutions.jts.io.ParseException;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.mapreduce.handler.geowave.GeoWaveDataTypeHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureBoundingBoxStatistics;
import mil.nga.giat.geowave.adapter.vector.util.FeatureDataUtils;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.data.visibility.GlobalVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.visibility.UniformVisibilityWriter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.operation.MathTransform;

import java.util.HashSet;
import java.util.Set;

public class GeoWaveMetadata implements RawRecordMetadata {
    
    public static final String METADATA_TABLE = "GEOWAVE_METADATA";
    public static final String INDEX_CF = "INDEX";
    public static final String ADAPTER_CF = "ADAPTER";
    public static final String STATISTICS_CF = "STATS";
    
    private final String tableNamespace;
    private final PrimaryIndex index;
    private final FeatureDataAdapter dataAdapter;
    private final SimpleFeatureType originalType;
    private final SimpleFeatureType reprojectedType;
    private final MathTransform transform;
    private final boolean flattenGeometry;
    
    private SimpleFeatureBuilder builder = null;
    private FeatureBoundingBoxStatistics bboxStats = null;
    private RowRangeHistogramStatistics rowStats = null;
    
    public GeoWaveMetadata(final String tableNamespace, final PrimaryIndex index, final FeatureDataAdapter dataAdapter, final SimpleFeatureType originalType,
                    final SimpleFeatureType reprojectedType, final MathTransform transform, final boolean flattenGeometry) {
        this.tableNamespace = tableNamespace;
        this.index = index;
        this.dataAdapter = dataAdapter;
        this.originalType = originalType;
        this.reprojectedType = reprojectedType;
        this.transform = transform;
        this.flattenGeometry = flattenGeometry;
        init();
    }
    
    private void init() {
        builder = new SimpleFeatureBuilder(originalType);
        
        // loop through available data adapter statistics and find the bounding
        // box stat that matches our default geometry
        for (final ByteArrayId statsId : dataAdapter.getSupportedStatisticsTypes()) {
            final DataStatistics stat = dataAdapter.createDataStatistics(statsId);
            if (stat instanceof FeatureBoundingBoxStatistics) {
                final FeatureBoundingBoxStatistics bboxStats = (FeatureBoundingBoxStatistics) stat;
                if (bboxStats.getFieldName().equals(dataAdapter.getFeatureType().getGeometryDescriptor().getLocalName())) {
                    this.bboxStats = bboxStats;
                }
            }
        }
        
        // handle data store specific statistics
        rowStats = new RowRangeHistogramStatistics(dataAdapter.getAdapterId(), index.getId());
    }
    
    @Override
    public void addEvent(final IngestHelperInterface helper, final RawRecordContainer event, final Multimap<String,NormalizedContentInterface> fields,
                    final long loadTimeInMillis) {
        updateStats(helper, event, fields);
    }
    
    @Override
    public void addEvent(final IngestHelperInterface helper, final RawRecordContainer event, final Multimap<String,NormalizedContentInterface> fields) {
        updateStats(helper, event, fields);
    }
    
    @Override
    public void addEventWithoutLoadDates(final IngestHelperInterface helper, final RawRecordContainer event,
                    final Multimap<String,NormalizedContentInterface> fields) {
        updateStats(helper, event, fields);
    }
    
    @Override
    public void addEvent(final IngestHelperInterface helper, final RawRecordContainer event, final Multimap<String,NormalizedContentInterface> fields,
                    final boolean frequency) {
        updateStats(helper, event, fields);
    }
    
    private void updateStats(final IngestHelperInterface helper, final RawRecordContainer event, final Multimap<String,NormalizedContentInterface> fields) {
        Set<SimpleFeature> features = null;
        try {
            features = createSimpleFeature(helper, event, fields);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        
        final byte[] visibility = (event.getVisibility() != null) ? event.getVisibility().getExpression() : new byte[] {};
        
        if (features != null) {
            for (final SimpleFeature feature : features) {
                if (feature.getDefaultGeometry() != null) {
                    final DataStoreEntryInfo ingestInfo = DataStoreUtils.getIngestInfo(dataAdapter, index, feature, new UniformVisibilityWriter<SimpleFeature>(
                                    new GlobalVisibilityHandler<SimpleFeature,Object>(StringUtils.stringFromBinary(visibility))));
                    
                    bboxStats.entryIngested(ingestInfo, feature);
                    rowStats.entryIngested(ingestInfo, feature);
                }
            }
        }
    }
    
    private Set<SimpleFeature> createSimpleFeature(final IngestHelperInterface helper, final RawRecordContainer event,
                    final Multimap<String,NormalizedContentInterface> fields) throws ParseException {
        final Set<SimpleFeature> features = new HashSet<SimpleFeature>();
        for (final SimpleFeature feature : GeoWaveDataTypeHandler.eventToSimpleFeatures(helper, event, fields, builder, flattenGeometry))
            features.add(FeatureDataUtils.defaultCRSTransform(feature, originalType, reprojectedType, transform));
        return features;
    }
    
    private KeyValue adapterToKeyValue(final FeatureDataAdapter adapter) {
        return new KeyValue(new Key(new Text(adapter.getAdapterId().getBytes()), new Text(GeoWaveMetadata.ADAPTER_CF), new Text()), new Value(
                        PersistenceUtils.toBinary(adapter)));
    }
    
    private KeyValue indexToKeyValue(final Index index) {
        return new KeyValue(new Key(new Text(index.getId().getBytes()), new Text(GeoWaveMetadata.INDEX_CF), new Text()), new Value(
                        PersistenceUtils.toBinary(index)));
    }
    
    private KeyValue statToKeyValue(final DataStatistics stat) {
        return new KeyValue(new Key(new Text(stat.getStatisticsId().getBytes()), new Text(STATISTICS_CF), new Text(dataAdapter.getAdapterId().getString())),
                        new Value(PersistenceUtils.toBinary(stat)));
    }
    
    @Override
    public Multimap<BulkIngestKey,Value> getBulkMetadata() {
        final Multimap<BulkIngestKey,Value> values = HashMultimap.create();
        
        final Text tableName = new Text(DataStoreUtils.getQualifiedTableName(tableNamespace, GeoWaveMetadata.METADATA_TABLE));
        
        if (dataAdapter != null) {
            final KeyValue adapterKV = adapterToKeyValue(dataAdapter);
            values.put(new BulkIngestKey(tableName, adapterKV.getKey()), adapterKV.getValue());
        }
        
        if (index != null) {
            final KeyValue indexKV = indexToKeyValue(index);
            values.put(new BulkIngestKey(tableName, indexKV.getKey()), indexKV.getValue());
        }
        
        if (bboxStats != null) {
            final KeyValue statKV = statToKeyValue(bboxStats);
            values.put(new BulkIngestKey(tableName, statKV.getKey()), statKV.getValue());
        }
        
        if (rowStats != null) {
            final KeyValue statKV = statToKeyValue(rowStats);
            values.put(new BulkIngestKey(tableName, statKV.getKey()), statKV.getValue());
        }
        
        return values;
    }
    
    @Override
    public void clear() {
        builder = null;
        bboxStats = null;
        rowStats = null;
        init();
    }
}
