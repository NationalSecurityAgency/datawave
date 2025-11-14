package datawave.annotation.data.v1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.annotation.data.AnnotationSerializationException;
import datawave.annotation.data.AnnotationSerializer;
import datawave.annotation.data.transform.DefaultTimestampTransformer;
import datawave.annotation.data.transform.DefaultVisibilityTransformer;
import datawave.annotation.data.transform.TimestampTransformer;
import datawave.annotation.data.transform.VisibilityTransformer;
import datawave.annotation.protobuf.v1.AnnotationSource;

public class AccumuloAnnotationSourceSerializer implements AnnotationSerializer<Iterator<Map.Entry<Key,Value>>,AnnotationSource> {
    public static final char NULL = 0x0;
    public static final Value EMPTY = new Value();

    protected static final Logger log = LoggerFactory.getLogger(AccumuloAnnotationSourceSerializer.class);
    private static final VisibilityTransformer DEFAULT_VISIBILITY_TRANSFORMER = new DefaultVisibilityTransformer();
    private static final TimestampTransformer DEFAULT_TIMESTAMP_TRANSFORMER = new DefaultTimestampTransformer();

    public static final String CONFIG_COLUMN_FAMILY = "config";
    public static final String ENGINE_COLUMN_FAMILY = "engine";
    public static final String MODEL_COLUMN_FAMILY = "model";
    public static final String SOURCE_LABEL_COLUMN_FAMILY = "sourceLabel";

    public static final List<String> COLUMN_FAMILIES = List.of(ENGINE_COLUMN_FAMILY, MODEL_COLUMN_FAMILY, SOURCE_LABEL_COLUMN_FAMILY, CONFIG_COLUMN_FAMILY);

    final VisibilityTransformer visibilityTransformer;
    final TimestampTransformer timestampTransformer;

    public AccumuloAnnotationSourceSerializer() {
        this(DEFAULT_VISIBILITY_TRANSFORMER, DEFAULT_TIMESTAMP_TRANSFORMER);
    }

    public AccumuloAnnotationSourceSerializer(VisibilityTransformer visibilityTransformer, TimestampTransformer timestampTransformer) {
        this.visibilityTransformer = visibilityTransformer;
        this.timestampTransformer = timestampTransformer;
    }

    @Override
    public Iterator<Map.Entry<Key,Value>> serialize(AnnotationSource annotationSource) throws AnnotationSerializationException {
        Key baseKey = generateBaseKey(annotationSource);
        List<Map.Entry<Key,Value>> serializedResults = new ArrayList<>();
        serializeAnnotationSourceFields(baseKey, annotationSource, serializedResults);

        if (!annotationSource.getConfigurationMap().isEmpty()) {
            serializeConfiguration(baseKey, annotationSource.getAnalyticHash(), annotationSource.getConfigurationMap(), serializedResults);
        }

        return serializedResults.iterator();
    }

    @Override
    public AnnotationSource deserialize(Iterator<Map.Entry<Key,Value>> elements) throws AnnotationSerializationException {
        if (elements == null || !elements.hasNext()) {
            return null;
        }

        final AnnotationSource.Builder annotationSourceBuilder = AnnotationSource.newBuilder();
        final Map<String,String> configuration = new HashMap<>();
        Key baseKey = null;

        // validate that the constituent part of the source have been found
        boolean seenEngine = false;
        boolean seenModel = false;
        boolean seenSourceLabel = false;
        boolean seenConfig = false;

        while (elements.hasNext()) {
            Map.Entry<Key,Value> e = elements.next();
            final Key key = e.getKey();

            if (baseKey == null) {
                baseKey = key;
                annotationSourceBuilder.setAnalyticHash(key.getRow().toString());
                configuration.putAll(timestampTransformer.toMetadataMap(key.getTimestamp()));
            } else if (!correctAnnotationSource(baseKey, key)) {
                throw new AnnotationSerializationException("The key provided isn't from the same annotation as the " + "first key provided: baseKey: ["
                                + baseKey + "] currentKey: [" + key + "]");
            }

            if (log.isTraceEnabled()) {
                log.trace("Iterated key: '{}'", e.getKey());
            }

            final String columnFamily = key.getColumnFamily().toString();
            final String columnQualifier = key.getColumnQualifier().toString();

            if (StringUtils.isBlank(columnQualifier)) {
                throw new AnnotationSerializationException("Column qualifier was empty: '" + key + "'");

            }

            switch (columnFamily) {
                case ENGINE_COLUMN_FAMILY:
                    if (seenEngine) {
                        throw new AnnotationSerializationException("Multiple 'engine' entries seen in columnFamily: '" + key + "'");
                    }
                    annotationSourceBuilder.setEngine(columnQualifier);
                    seenEngine = true;
                    break;
                case MODEL_COLUMN_FAMILY:
                    if (seenModel) {
                        throw new AnnotationSerializationException("Multiple 'model' entries seen in columnFamily: " + key + "'");
                    }
                    annotationSourceBuilder.setModel(columnQualifier);
                    seenModel = true;
                    break;
                case SOURCE_LABEL_COLUMN_FAMILY:
                    if (seenSourceLabel) {
                        throw new AnnotationSerializationException("Multiple 'sourceLabel' entries seen in columnFamily: " + key + "'");
                    }
                    annotationSourceBuilder.setSourceLabel(columnQualifier);
                    seenSourceLabel = true;
                    break;
                case CONFIG_COLUMN_FAMILY:
                    String[] cqParts = key.getColumnQualifier().toString().split("\0");
                    if (cqParts.length != 2) {
                        throw new AnnotationSerializationException("Column qualifier in key didn't have 2 parts, key: '" + key + "'");
                    }
                    configuration.put(cqParts[0], cqParts[1]);
                    seenConfig = true;
                    break;
                default:
                    throw new AnnotationSerializationException("Column family '" + columnFamily + "' was not in list of expected column families: '"
                                    + COLUMN_FAMILIES + "', '" + key + "'");
            }
        }

        if (seenEngine && seenModel && seenSourceLabel && seenConfig) {
            log.debug("annotation source is complete for: '{}'", baseKey);
        } else {
            //@formatter:off
            throw new AnnotationSerializationException("Did not observe expected portion of annotation source:" +
                    " engine: " + seenEngine +
                    " model: " + seenModel +
                    " sourceLabel: " + seenSourceLabel +
                    " config: " + seenConfig + ". ");
            //@formatter:on
        }

        return annotationSourceBuilder.putAllConfiguration(configuration).build();
    }

    /**
     * Validate that the two keys specified are form the same annotation source in that they share the same row
     *
     * @param expected
     *            the key we expect
     * @param target
     *            the key to compare
     * @return true if the keys share the same annotation source.
     */
    protected static boolean correctAnnotationSource(Key expected, Key target) {
        return expected.getRowData().equals(target.getRowData());
    }

    /**
     * Generate the base key that will be used for serialization throughout this class
     *
     * @param annotationSource
     *            annotation source for key generation
     * @return base accumulo key for this annotation
     * @throws AnnotationSerializationException
     *             if there's a problem with the visibility transformer.
     */
    protected Key generateBaseKey(AnnotationSource annotationSource) throws AnnotationSerializationException {
        String rowId = annotationSource.getAnalyticHash();
        String columnFamily = "source";
        ColumnVisibility cv = visibilityTransformer.toColumnVisibility(annotationSource.getConfigurationMap());
        long timestamp = timestampTransformer.toTimestamp(annotationSource.getConfigurationMap());
        return Key.builder().row(rowId).family(columnFamily).visibility(cv).timestamp(timestamp).build();
    }

    /**
     * Serialize the engine, model and sourceLabel fields of the AnnotationSource
     *
     * @param baseKey
     *            the base key for this source
     * @param annotationSource
     *            the annotation source to serialize
     * @param serializedResults
     *            the array used to store the serialization results.
     */
    protected static void serializeAnnotationSourceFields(Key baseKey, AnnotationSource annotationSource, List<Map.Entry<Key,Value>> serializedResults) {
        //@formatter:off
        final Key engineKey = Key.builder()
                .row(baseKey.getRowData().getBackingArray())
                .family(ENGINE_COLUMN_FAMILY)
                .qualifier(annotationSource.getEngine())
                .visibility(baseKey.getColumnVisibilityData().getBackingArray())
                .timestamp(baseKey.getTimestamp())
                .build();
        serializedResults.add(Map.entry(engineKey, EMPTY));

        final Key modelKey = Key.builder()
                .row(baseKey.getRowData().getBackingArray())
                .family(MODEL_COLUMN_FAMILY)
                .qualifier(annotationSource.getModel())
                .visibility(baseKey.getColumnVisibilityData().getBackingArray())
                .timestamp(baseKey.getTimestamp())
                .build();
        serializedResults.add(Map.entry(modelKey, EMPTY));

        final Key sourceLabelKey = Key.builder()
                .row(baseKey.getRowData().getBackingArray())
                .family(SOURCE_LABEL_COLUMN_FAMILY)
                .qualifier(annotationSource.getSourceLabel())
                .visibility(baseKey.getColumnVisibilityData().getBackingArray())
                .timestamp(baseKey.getTimestamp())
                .build();
        serializedResults.add(Map.entry(sourceLabelKey, EMPTY));
        //@formatter:on
    }

    /**
     * Serialize an Annotation's configuration map to a series of Accumulo key, value pairs written to the list provided.
     *
     * @param baseKey
     *            the base key for the annotation.
     * @param annotationId
     *            the annotation id we are serializing.
     * @param configuration
     *            the metadata map to serialize.
     * @param serializedResults
     *            serialized pairs will be written to a provided list.
     */
    protected void serializeConfiguration(Key baseKey, String annotationId, Map<String,String> configuration, List<Map.Entry<Key,Value>> serializedResults) {

        Set<String> timestampFields = timestampTransformer.getTimestampFields();
        for (Map.Entry<String,String> entry : configuration.entrySet()) {
            if (timestampFields.contains(entry.getKey())) {
                continue; // don't serialize the timestamp field in metadata, we just use them for the Accumulo timestamp.
            }
            serializedResults.add(serializeConfiguration(baseKey, annotationId, entry.getKey(), entry.getValue()));
        }
    }

    /**
     * Serialize a single map entry to an Accumulo key, value pair.
     *
     * @param baseKey
     *            key shared by all rows in a single annotation.
     * @param annotationId
     *            this annotation's UID.
     * @param configurationKey
     *            a single metadata key.
     * @param configurationValue
     *            a single metadata value.
     * @return the key and value pair for the serialized metadata key value pair.
     */
    protected static Map.Entry<Key,Value> serializeConfiguration(Key baseKey, String annotationId, String configurationKey, String configurationValue) {
        final String columnQualifier = configurationKey + NULL + configurationValue;
        //@formatter:off
        final Key key = Key.builder()
                .row(baseKey.getRowData().getBackingArray())
                .family(CONFIG_COLUMN_FAMILY)
                .qualifier(columnQualifier)
                .visibility(baseKey.getColumnVisibilityData().getBackingArray())
                .timestamp(baseKey.getTimestamp())
                .build();
        //@formatter:on

        return Map.entry(key, EMPTY);
    }
}
