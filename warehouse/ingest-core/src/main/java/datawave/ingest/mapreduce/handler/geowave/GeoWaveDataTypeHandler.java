package datawave.ingest.mapreduce.handler.geowave;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.ParseException;
import datawave.data.normalizer.AbstractGeometryNormalizer;
import datawave.data.type.DateType;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.ingest.data.config.DataTypeHelperImpl;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.mapreduce.handler.DataTypeHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.metadata.GeoWaveMetadata;
import datawave.ingest.metadata.RawRecordMetadata;
import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveGTDataStore;
import mil.nga.giat.geowave.adapter.vector.util.FeatureDataUtils;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialTemporalDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.visibility.GlobalVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.visibility.UniformVisibilityWriter;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.MathTransform;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class GeoWaveDataTypeHandler<KEYIN> implements DataTypeHandler<KEYIN> {
    
    private static final Logger log = Logger.getLogger(GeoWaveDataTypeHandler.class);
    
    // These GeoWave properties are set globally
    public static final String GEOWAVE_NAMESPACE = ".geowave.namespace";
    public static final String GEOWAVE_INDEX_TYPE = ".geowave.index.type";
    public static final String GEOWAVE_FLATTEN_GEOMETRY = ".geowave.flattengeometry";
    
    // These GeoWave properties are set per-data-type
    public static final String GEOWAVE_FEATURE_TYPE = ".geowave.feature.type";
    public static final String GEOWAVE_FEATURE_TYPE_NAME = GEOWAVE_FEATURE_TYPE + ".name";
    public static final String GEOWAVE_FEATURE_TYPE_SPEC = GEOWAVE_FEATURE_TYPE + ".spec";
    public static final String GEOWAVE_FEATURE_TYPE_NAMESPACE_URI = GEOWAVE_FEATURE_TYPE + ".namespaceuri";
    public static final String GEOWAVE_FEATURE_TYPE_DEFAULT_GEOMETRY = GEOWAVE_FEATURE_TYPE + ".defaultgeometry";
    public static final String GEOWAVE_FEATURE_TYPE_ATTRIBUTE = GEOWAVE_FEATURE_TYPE + ".attribute.";
    public static final String BINDING = ".binding";
    public static final String CRS_CODE = ".crs";
    public static final String DESCRIPTION = ".description";
    public static final String NAME = ".name";
    public static final String NILLABLE = ".nillable";
    
    public static final ByteArrayId COMPOSITE_CQ = new ByteArrayId(StringUtils.stringToBinary("composite"));
    
    private SimpleFeatureType originalType = null;
    private SimpleFeatureType reprojectedType = null;
    private MathTransform transform = null;
    private SimpleFeatureBuilder builder = null;
    
    private FeatureDataAdapter dataAdapter = null;
    private PrimaryIndex index = null;
    
    private String tableNamespace = null;
    private boolean flattenGeometry = true;
    
    private GeoWaveMetadata metadata = null;
    
    private Configuration conf = null;
    
    @Override
    public void setup(final TaskAttemptContext context) {
        final TypeRegistry registry = TypeRegistry.getInstance(context.getConfiguration());
        final Type type = registry.get(context.getConfiguration().get(DataTypeHelperImpl.Properties.DATA_NAME));
        
        conf = context.getConfiguration();
        
        final String sftName = conf.get(type.typeName() + GEOWAVE_FEATURE_TYPE_NAME);
        final String sftSpec = conf.get(type.typeName() + GEOWAVE_FEATURE_TYPE_SPEC);
        try {
            originalType = (sftSpec != null) ? DataUtilities.createType(sftName, sftSpec) : buildSimpleFeatureType(type.typeName());
        } catch (SchemaException e) {
            log.error("Unable to parse Simple Feature Type from spec [" + sftSpec + "]");
        }
        
        // create a simple feature data adapter
        dataAdapter = new FeatureDataAdapter(originalType);
        
        // determine whether features need to be reprojected
        if (!GeoWaveGTDataStore.DEFAULT_CRS.equals(originalType.getCoordinateReferenceSystem())) {
            reprojectedType = SimpleFeatureTypeBuilder.retype(originalType, GeoWaveGTDataStore.DEFAULT_CRS);
            if (originalType.getCoordinateReferenceSystem() != null) {
                try {
                    transform = CRS.findMathTransform(originalType.getCoordinateReferenceSystem(), GeoWaveGTDataStore.DEFAULT_CRS, true);
                } catch (final FactoryException e) {
                    log.warn("Unable to create coordinate reference system transform", e);
                }
            }
        }
        
        // features are built using the original projection
        builder = new SimpleFeatureBuilder(originalType);
        
        // create an index
        index = initIndex(conf.get(type.typeName() + GEOWAVE_INDEX_TYPE, null));
        
        // namespace
        tableNamespace = conf.get(type.typeName() + GEOWAVE_NAMESPACE, null);
        
        // flatten geometry
        flattenGeometry = conf.getBoolean(type.typeName() + GEOWAVE_FLATTEN_GEOMETRY, flattenGeometry);
        
        // GeoWave metadata
        metadata = new GeoWaveMetadata(tableNamespace, index, dataAdapter, originalType, reprojectedType, transform, flattenGeometry);
    }
    
    private SimpleFeatureType buildSimpleFeatureType(final String typeName) {
        
        final SimpleFeatureTypeBuilder sftb = new SimpleFeatureTypeBuilder();
        
        // Simple Feature Type Name - required
        sftb.setName(conf.get(typeName + GEOWAVE_FEATURE_TYPE_NAME));
        
        // Simple Feature Type Namespace URI - optional
        final String namespaceURI = conf.get(typeName + GEOWAVE_FEATURE_TYPE_NAMESPACE_URI);
        if (namespaceURI != null)
            sftb.setNamespaceURI(namespaceURI);
        
        // parse Simple Feature Type Attributes
        int attribIdx = 1;
        String name = conf.get(typeName + GEOWAVE_FEATURE_TYPE_ATTRIBUTE + attribIdx + NAME);
        String binding = conf.get(typeName + GEOWAVE_FEATURE_TYPE_ATTRIBUTE + attribIdx + BINDING);
        while (name != null && binding != null) {
            
            // optional attribute properties
            final String crs = conf.get(typeName + GEOWAVE_FEATURE_TYPE_ATTRIBUTE + attribIdx + CRS_CODE);
            if (crs != null) {
                try {
                    sftb.crs(CRS.decode(crs, true));
                } catch (FactoryException e) {
                    log.warn("Unable to decode CRS [" + crs + "] for attribute [" + name + "]", e);
                }
            }
            
            final String description = conf.get(typeName + GEOWAVE_FEATURE_TYPE_ATTRIBUTE + attribIdx + DESCRIPTION);
            if (description != null)
                sftb.description(description);
            
            final String nillable = conf.get(typeName + GEOWAVE_FEATURE_TYPE_ATTRIBUTE + attribIdx + NILLABLE);
            if (nillable != null)
                sftb.nillable(Boolean.parseBoolean(nillable));
            
            // Add the Attribute Descriptor to the Simple Feature Type
            try {
                sftb.add(name, Class.forName(binding));
            } catch (ClassNotFoundException e) {
                log.warn("Unable to infer binding [" + binding + "] for attribute [" + name + "]", e);
            }
            
            // Get the name and binding for the next attribute
            name = conf.get(typeName + GEOWAVE_FEATURE_TYPE_ATTRIBUTE + ++attribIdx + NAME);
            binding = conf.get(typeName + GEOWAVE_FEATURE_TYPE_ATTRIBUTE + attribIdx + BINDING);
        }
        
        // If specified, set the default geometry
        final String defaultGeometry = conf.get(typeName + GEOWAVE_FEATURE_TYPE_DEFAULT_GEOMETRY);
        if (defaultGeometry != null)
            sftb.setDefaultGeometry(defaultGeometry);
        
        return sftb.buildFeatureType();
    }
    
    PrimaryIndex initIndex(final String indexType) {
        // create an index
        PrimaryIndex index = null;
        if (indexType != null) {
            if (new SpatialDimensionalityTypeProvider().getDimensionalityTypeName().equalsIgnoreCase(indexType)) {
                index = new SpatialDimensionalityTypeProvider().createPrimaryIndex();
            } else if (new SpatialTemporalDimensionalityTypeProvider().getDimensionalityTypeName().equalsIgnoreCase(indexType)) {
                index = new SpatialTemporalDimensionalityTypeProvider().createPrimaryIndex();
            } else {
                log.error("Unable to determine index type [" + indexType + "]");
            }
        }
        return index;
    }
    
    @Override
    public String[] getTableNames(final Configuration conf) {
        final List<String> tableNames = new ArrayList<>(2);
        
        final TypeRegistry registry = TypeRegistry.getInstance(conf);
        final String dataName = conf.get(DataTypeHelper.Properties.DATA_NAME_OVERRIDE, conf.get(DataTypeHelper.Properties.DATA_NAME));
        if (dataName != null && registry.containsKey(dataName)) {
            final Type type = registry.get(dataName);
            
            final String tableNamespace = conf.get(type.typeName() + GEOWAVE_NAMESPACE, null);
            final PrimaryIndex index = initIndex(conf.get(type.typeName() + GEOWAVE_INDEX_TYPE, null));
            
            if ((tableNamespace != null) && (index != null)) {
                tableNames.add(DataStoreUtils.getQualifiedTableName(tableNamespace, GeoWaveMetadata.METADATA_TABLE));
                tableNames.add(DataStoreUtils.getQualifiedTableName(tableNamespace, index.getId().getString()));
            }
        }
        return tableNames.toArray(new String[tableNames.size()]);
    }
    
    @Override
    public int[] getTableLoaderPriorities(final Configuration conf) {
        final int[] priorities = new int[2];
        int index = 0;
        
        final TypeRegistry registry = TypeRegistry.getInstance(conf);
        final String dataName = conf.get(DataTypeHelper.Properties.DATA_NAME_OVERRIDE, conf.get(DataTypeHelper.Properties.DATA_NAME));
        if (dataName != null && registry.containsKey(dataName)) {
            final Type type = registry.get(dataName);
            
            final String tableNamespace = conf.get(type.typeName() + GEOWAVE_NAMESPACE, null);
            if (tableNamespace != null) {
                // METADATA_TABLE
                priorities[index++] = 20;
                
                // SPATIAL/SPATIAL_TEMPORAL TABLE
                priorities[index++] = 30;
            }
        }
        
        if (index != priorities.length) {
            return Arrays.copyOf(priorities, index);
        } else {
            return priorities;
        }
    }
    
    @Override
    public Multimap<BulkIngestKey,Value> processBulk(final KEYIN key, final RawRecordContainer event, final Multimap<String,NormalizedContentInterface> fields,
                    final StatusReporter reporter) {
        final Multimap<BulkIngestKey,Value> values = HashMultimap.create();
        final IngestHelperInterface helper = getHelper(event.getDataType());
        
        Set<SimpleFeature> features = null;
        try {
            features = createSimpleFeatures(helper, event, fields);
        } catch (ParseException e) {
            log.error(e);
        }
        
        // Entry Key/Value Pairs
        if (features != null) {
            for (final SimpleFeature feature : features) {
                final List<KeyValue> entryKV = entryToKeyValues(feature, (event.getVisibility() != null) ? event.getVisibility().getExpression()
                                : new byte[] {});
                for (final KeyValue kv : entryKV) {
                    final BulkIngestKey biKey = new BulkIngestKey(new Text(DataStoreUtils.getQualifiedTableName(tableNamespace, index.getId().getString())),
                                    kv.getKey());
                    values.put(biKey, kv.getValue());
                }
            }
        }
        
        return values;
    }
    
    private Set<SimpleFeature> createSimpleFeatures(final IngestHelperInterface helper, final RawRecordContainer event,
                    final Multimap<String,NormalizedContentInterface> fields) throws ParseException {
        final Set<SimpleFeature> features = new HashSet<>();
        for (final SimpleFeature feature : eventToSimpleFeatures(helper, event, fields, builder, flattenGeometry))
            features.add(FeatureDataUtils.defaultCRSTransform(feature, originalType, reprojectedType, transform));
        return features;
    }
    
    public static Set<SimpleFeature> eventToSimpleFeatures(final IngestHelperInterface helper, final RawRecordContainer event,
                    final Multimap<String,NormalizedContentInterface> fields, final SimpleFeatureBuilder builder, boolean flattenGeometry)
                    throws ParseException {
        final Set<SimpleFeature> features = new HashSet<>();
        final SimpleFeatureType simpleFeatureType = builder.getFeatureType();
        final AttributeDescriptor geomAttrib = simpleFeatureType.getGeometryDescriptor();
        
        // make sure we have geometries to work with, otherwise we quit now
        if (geomAttrib == null || !fields.containsKey(geomAttrib.getLocalName()) || fields.get(geomAttrib.getLocalName()).isEmpty())
            return features;
        
        final ArrayList<String> geomValues = new ArrayList<>();
        
        // process non-geometry attributes from the event
        for (final AttributeDescriptor attrib : simpleFeatureType.getAttributeDescriptors()) {
            
            // make sure field exists, and is not the default geometry
            if (!fields.containsKey(attrib.getLocalName()))
                continue;
            
            // get the values for this field
            final Collection<NormalizedContentInterface> values = fields.get(attrib.getLocalName());
            
            if (values.isEmpty())
                continue;
            
            final ArrayList<String> stringValues = new ArrayList<>(values.size());
            for (final NormalizedContentInterface value : values)
                stringValues.add(value.getEventFieldValue());
            
            // save the default geometry for later processing
            if (geomAttrib.equals(attrib)) {
                geomValues.addAll(stringValues);
                continue;
            }
            
            final Class binding = attrib.getType().getBinding();
            
            // single or multivalue?
            if (values.size() == 1) {
                final String stringValue = stringValues.get(0);
                if (String.class.isAssignableFrom(binding)) {
                    builder.set(attrib.getName(), stringValue);
                } else if (Integer.class.isAssignableFrom(binding)) {
                    builder.set(attrib.getName(), Integer.parseInt(stringValue));
                } else if (Long.class.isAssignableFrom(binding)) {
                    builder.set(attrib.getName(), Long.parseLong(stringValue));
                } else if (Short.class.isAssignableFrom(binding)) {
                    builder.set(attrib.getName(), Short.parseShort(stringValue));
                } else if (Double.class.isAssignableFrom(binding)) {
                    builder.set(attrib.getName(), Double.parseDouble(stringValue));
                } else if (Float.class.isAssignableFrom(binding)) {
                    builder.set(attrib.getName(), Float.parseFloat(stringValue));
                } else if (Boolean.class.isAssignableFrom(binding)) {
                    builder.set(attrib.getName(), Boolean.parseBoolean(stringValue));
                } else if (Date.class.isAssignableFrom(binding)) {
                    for (final datawave.data.type.Type<?> type : helper.getDataTypes(attrib.getLocalName())) {
                        if (type instanceof DateType) {
                            builder.set(attrib.getName(), type.denormalize(stringValue));
                            break;
                        }
                    }
                } else if (Geometry.class.isAssignableFrom(binding)) {
                    builder.set(attrib.getName(), AbstractGeometryNormalizer.parseGeometry(stringValue));
                } else {
                    log.error("Unable to map field [" + attrib.getLocalName() + "] to desired type [" + binding.getName() + "]");
                }
            } else if (values.size() > 1) {
                if (String.class.isAssignableFrom(binding)) {
                    builder.set(attrib.getName(), Joiner.on(';').skipNulls().join(stringValues));
                } else if (Geometry.class.isAssignableFrom(binding)) {
                    final ArrayList<Geometry> geomList = new ArrayList<>(stringValues.size());
                    for (final String stringValue : stringValues)
                        geomList.add(AbstractGeometryNormalizer.parseGeometry(stringValue));
                    builder.set(attrib.getName(), new GeometryFactory().createGeometryCollection(geomList.toArray(new Geometry[geomList.size()])));
                } else {
                    log.warn("Multi-value support for field [" + attrib.getLocalName() + "] with type [" + binding.getName()
                                    + "] is unavailable.  Consider typing as String for multi-value fields.");
                }
            }
        }
        
        // create a base feature which will be used to seed all subsequent features
        final SimpleFeature baseFeature = builder.buildFeature(event.getId().toString());
        
        // now handle default geometry
        if (flattenGeometry || geomValues.size() == 1) {
            // create a unique feature per geometry
            for (final String geomValue : geomValues) {
                builder.init(baseFeature);
                builder.set(geomAttrib.getName(), AbstractGeometryNormalizer.parseGeometry(geomValue));
                features.add(builder.buildFeature(Joiner.on(";").skipNulls().join(baseFeature.getID(), (geomValues.size() == 1) ? null : geomValue)));
            }
        } else {
            // create a single feature with all geometries
            final ArrayList<Geometry> geomList = new ArrayList<>(geomValues.size());
            for (final String geomValue : geomValues)
                geomList.add(AbstractGeometryNormalizer.parseGeometry(geomValue));
            builder.set(geomAttrib.getName(), new GeometryFactory().createGeometryCollection(geomList.toArray(new Geometry[geomList.size()])));
            features.add(builder.buildFeature(baseFeature.getID()));
        }
        
        return features;
    }
    
    private List<KeyValue> entryToKeyValues(final SimpleFeature feature, final byte[] visibility) {
        final List<KeyValue> keyValues = new ArrayList<>();
        if (feature.getDefaultGeometry() != null) {
            final DataStoreEntryInfo ingestInfo = DataStoreUtils.getIngestInfo(dataAdapter, index, feature, new UniformVisibilityWriter<>(
                            new GlobalVisibilityHandler<>(StringUtils.stringFromBinary(visibility))));
            
            final List<DataStoreEntryInfo.FieldInfo<?>> fieldInfoList = DataStoreUtils.composeFlattenedFields(ingestInfo.getFieldInfo(), index.getIndexModel(),
                            dataAdapter);
            
            for (final ByteArrayId row : ingestInfo.getRowIds()) {
                for (final DataStoreEntryInfo.FieldInfo fieldInfo : fieldInfoList) {
                    final Key key = new Key(new Text(row.getBytes()), new Text(dataAdapter.getAdapterId().getBytes()), new Text(fieldInfo.getDataValue()
                                    .getId().getBytes()), new Text(fieldInfo.getVisibility()));
                    
                    keyValues.add(new KeyValue(key, new Value(fieldInfo.getWrittenValue())));
                }
            }
        }
        return keyValues;
    }
    
    private static <T> List<DataStoreEntryInfo.FieldInfo<?>> composeFlattenedFields(final List<DataStoreEntryInfo.FieldInfo<?>> originalList,
                    final PrimaryIndex index) {
        final List<DataStoreEntryInfo.FieldInfo<?>> retVal = new ArrayList<>();
        final Map<ByteArrayId,List<DataStoreEntryInfo.FieldInfo<?>>> vizToFieldMap = new HashMap<>();
        boolean sharedVisibility = false;
        // organize FieldInfos by unique visibility
        for (final DataStoreEntryInfo.FieldInfo<?> fieldInfo : originalList) {
            final ByteArrayId currViz = new ByteArrayId(fieldInfo.getVisibility());
            if (vizToFieldMap.containsKey(currViz)) {
                sharedVisibility = true;
                final List<DataStoreEntryInfo.FieldInfo<?>> listForViz = vizToFieldMap.get(currViz);
                final FieldReader<CommonIndexValue> fieldReader = index.getIndexModel().getReader(fieldInfo.getDataValue().getId());
                if (fieldReader != null) {
                    // put common index values up front
                    listForViz.add(0, fieldInfo);
                } else {
                    listForViz.add(fieldInfo);
                }
            } else {
                final List<DataStoreEntryInfo.FieldInfo<?>> listForViz = new ArrayList<>();
                listForViz.add(fieldInfo);
                vizToFieldMap.put(currViz, listForViz);
            }
        }
        if (!sharedVisibility) {
            return originalList;
        }
        for (final Entry<ByteArrayId,List<DataStoreEntryInfo.FieldInfo<?>>> entry : vizToFieldMap.entrySet()) {
            final List<byte[]> fieldInfoBytesList = new ArrayList<>();
            int totalLength = 0;
            for (final DataStoreEntryInfo.FieldInfo<?> fieldInfo : entry.getValue()) {
                final byte[] fieldIdBytes = fieldInfo.getDataValue().getId().getBytes();
                final ByteBuffer fieldInfoBytes = ByteBuffer.allocate(8 + fieldIdBytes.length + fieldInfo.getWrittenValue().length);
                fieldInfoBytes.putInt(fieldIdBytes.length);
                fieldInfoBytes.put(fieldIdBytes);
                fieldInfoBytes.putInt(fieldInfo.getWrittenValue().length);
                fieldInfoBytes.put(fieldInfo.getWrittenValue());
                fieldInfoBytesList.add(fieldInfoBytes.array());
                totalLength += fieldInfoBytes.array().length;
            }
            final ByteBuffer allFields = ByteBuffer.allocate(4 + totalLength);
            allFields.putInt(entry.getValue().size());
            for (final byte[] bytes : fieldInfoBytesList) {
                allFields.put(bytes);
            }
            final DataStoreEntryInfo.FieldInfo<?> composite = new DataStoreEntryInfo.FieldInfo<T>(new PersistentValue<>(COMPOSITE_CQ, null), // unnecessary
                            allFields.array(), entry.getKey().getBytes());
            retVal.add(composite);
        }
        return retVal;
    }
    
    @Override
    public IngestHelperInterface getHelper(final Type datatype) {
        return datatype.getIngestHelper(conf);
    }
    
    @Override
    public void close(final TaskAttemptContext context) {}
    
    @Override
    public RawRecordMetadata getMetadata() {
        return metadata;
    }
    
    public SimpleFeatureType getOriginalType() {
        return originalType;
    }
    
    public SimpleFeatureType getReprojectedType() {
        return reprojectedType;
    }
}
