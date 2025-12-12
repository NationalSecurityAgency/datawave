package datawave.annotation.data.v1;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.hash.HashFunction;
import com.google.protobuf.InvalidProtocolBufferException;

import datawave.annotation.data.AnnotationReadException;
import datawave.annotation.data.AnnotationSerializationException;
import datawave.annotation.data.AnnotationSerializer;
import datawave.annotation.data.AnnotationWriteException;
import datawave.annotation.data.transform.DefaultTimestampTransformer;
import datawave.annotation.data.transform.DefaultVisibilityTransformer;
import datawave.annotation.data.transform.TimestampTransformer;
import datawave.annotation.data.transform.VisibilityTransformer;
import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.annotation.util.Validator;
import datawave.annotation.util.v1.AnnotationValidators;

public class AccumuloAnnotationSourceSerializer implements AnnotationSerializer<Iterator<Map.Entry<Key,Value>>,AnnotationSource> {
    public static final char NULL = 0x0;

    protected static final Logger log = LoggerFactory.getLogger(AccumuloAnnotationSourceSerializer.class);

    private static final VisibilityTransformer DEFAULT_VISIBILITY_TRANSFORMER = new DefaultVisibilityTransformer();
    private static final TimestampTransformer DEFAULT_TIMESTAMP_TRANSFORMER = new DefaultTimestampTransformer();

    public static final String DATA_COLUMN_FAMILY = "d";

    final VisibilityTransformer visibilityTransformer;
    final TimestampTransformer timestampTransformer;
    final Set<String> managedFields;

    public AccumuloAnnotationSourceSerializer() {
        this(DEFAULT_VISIBILITY_TRANSFORMER, DEFAULT_TIMESTAMP_TRANSFORMER);
    }

    public AccumuloAnnotationSourceSerializer(VisibilityTransformer visibilityTransformer, TimestampTransformer timestampTransformer) {
        this.visibilityTransformer = visibilityTransformer;
        this.timestampTransformer = timestampTransformer;

        final Set<String> managedFields = new HashSet<>();
        managedFields.addAll(visibilityTransformer.getMetadataFields());
        managedFields.addAll(timestampTransformer.getMetadataFields());
        this.managedFields = ImmutableSet.copyOf(managedFields);
    }

    /**
     * The entirety of the annotationSource is serialized to bytes and stored in the value, whereas portions of the source are stored in the rowId and column
     * qualifier.
     *
     * @param annotationSource
     *            the source to serialize
     * @return an interator containing the elements to write to accumulo
     * @throws AnnotationSerializationException
     *             if there's a problem serializing the annotation (e.g., if the annotation did not contain required fields)
     */
    @Override
    public Iterator<Map.Entry<Key,Value>> serialize(AnnotationSource annotationSource) throws AnnotationSerializationException {
        Validator.ValidationState<AnnotationSource> validationState = AnnotationValidators.checkAnnotationSource(annotationSource);
        if (!validationState.isValid()) {
            throw new AnnotationWriteException("Annotation source is not valid: " + validationState.getErrors());
        }

        final List<Map.Entry<Key,Value>> serializedResults = new ArrayList<>();

        Value value = new Value(annotationSource.toByteArray());
        String rowId = annotationSource.getAnalyticSourceHash();
        String columnQualifier = annotationSource.getEngine() + NULL + annotationSource.getModel() + NULL + annotationSource.getAnalyticHash();

        // use the configured transformers to provide the column visibility and timestamp.
        ColumnVisibility cv = visibilityTransformer.fromMetadataMap(annotationSource.getMetadataMap());
        long timestamp = timestampTransformer.fromMetadataMap(annotationSource.getMetadataMap());

        Key key = Key.builder().row(rowId).family(DATA_COLUMN_FAMILY).qualifier(columnQualifier).visibility(cv).timestamp(timestamp).build();

        serializedResults.add(Map.entry(key, value));
        return serializedResults.iterator();
    }

    /**
     * The entirety of the annotationSource is deserialized from the value.
     *
     * @param elements
     *            the elements to deserialize
     * @return the deserialized annotation
     * @throws AnnotationSerializationException
     *             if there's a problem deserializing the annotation source.
     */
    @Override
    public AnnotationSource deserialize(Iterator<Map.Entry<Key,Value>> elements) throws AnnotationSerializationException {
        if (elements == null) {
            return null;
        }

        final List<AnnotationSource> deserializedSources = new ArrayList<>();
        while (elements.hasNext()) {
            Map.Entry<Key,Value> e = elements.next();
            String row = e.getKey().getRow().toString();

            try {
                Value v = e.getValue();
                AnnotationSource source = AnnotationSource.parseFrom(v.get());
                deserializedSources.add(source);
            } catch (InvalidProtocolBufferException ipe) {
                throw new AnnotationReadException("Unable to decode value for annotation source: " + row + ".");
            }
        }
        return maybeMergeSourceMetadata(deserializedSources);
    }

    /**
     * Given a list of zero to many annotation sources, sharing the same analyticHash and analyticSourceHash, merge them into a single source. We need this
     * because it's possible that a single annotation source may have multiple entries in Accumulo because we set the visibility of the source based on the
     * visibility of the annotation.
     *
     * @param sources
     *            the sources to merge
     * @return the merged sources. null if not sources were provided.
     * @throws AnnotationSerializationException
     *             if the sources provided don't all have the same analyticHash or analyticSourceHash which indicates that they should not be merged.
     */
    protected AnnotationSource maybeMergeSourceMetadata(List<AnnotationSource> sources) throws AnnotationSerializationException {
        if (sources == null || sources.isEmpty()) {
            // null
            return null;
        } else if (sources.size() == 1) {
            // just return the single source
            return sources.get(0);
        }

        // multiple sources - merge them together,
        AnnotationSource baseSource = sources.get(0);
        for (int i = 1; i < sources.size(); i++) {
            baseSource = mergeSourceMetadata(baseSource, sources.get(i));
        }
        return baseSource;
    }

    /**
     * Merge two sources that share the same analyticHash and analyticSourceHash. Based in the logic in
     * {@link datawave.annotation.util.v1.AnnotationUtils#calculateSourceHash(HashFunction, AnnotationSource)} if these two hashes are the same, the engine,
     * model and configuration are the same, so the main task is to merge the metadata from both sources into a single map.
     *
     * @param a
     *            the first source to merge
     * @param b
     *            the second source to merge
     * @return the merged source or null if both input sources are null.
     * @throws AnnotationSerializationException
     *             if the analyticHash or analyticSourceHashes don't match.
     */
    protected AnnotationSource mergeSourceMetadata(AnnotationSource a, AnnotationSource b) throws AnnotationSerializationException {
        if (a == null && b == null) {
            return null;
        } else if (b == null) {
            return a;
        } else if (a == null) {
            return b;
        }

        //@formatter:off
        if (!a.getAnalyticSourceHash().equals(b.getAnalyticSourceHash())) {
            throw new AnnotationSerializationException("Found multiple sources, but can't merge sources with different source hashes: " +
                    "a:'" + a.getAnalyticSourceHash() + "' " + "b:'" + b.getAnalyticSourceHash());
        }

        if (!a.getAnalyticHash().equals(b.getAnalyticHash())) {
            throw new AnnotationSerializationException("Found multiple sources, but can't merge sources with different analytic hashes: " +
                    "a:'" + a.getAnalyticHash() + "' " + "b:'" + b.getAnalyticHash());
        }
        //@formatter:on

        final Map<String,String> newBaseMetadata = mergeSourceMetadata(a.getMetadataMap(), b.getMetadataMap());
        return a.toBuilder().clearMetadata().putAllMetadata(newBaseMetadata).build();
    }

    /**
     * Merge the metadata maps from two annotation sources, paying special attention to the fields that were added by the {@code visibilityTransformer} and
     * {@code timestampTransformer}.
     * <p>
     * The visibilityTransformer and timestampTransformer each implement special logic for merging multiple metadata maps based on the fields the
     * implementations use and their values. All fields not managed by one of the transformers are simply copied as is to the merged map. When both maps contain
     * an entry with the same key, the first map takes precedence over the second map. This means that if a metadata entry exists in the first map and the same
     * key exists in the second map, we will take the first map's metadata entry.
     * </p>
     * If the visibilityTransformer and timestampTransformer manage the same fields, the result is undefined - so implementations that so this should be aware.
     *
     * @param first
     *            the first map to merge
     * @param second
     *            the second map to merge.
     * @return the merged metadata map.
     */
    protected Map<String,String> mergeSourceMetadata(Map<String,String> first, Map<String,String> second) {

        final boolean firstEmpty = first == null || first.isEmpty();
        final boolean secondEmpty = second == null || second.isEmpty();

        if (firstEmpty && secondEmpty) {
            return Collections.emptyMap();
        } else if (firstEmpty) {
            return second;
        } else if (secondEmpty) {
            return first;
        }

        // both maps have data, so we have to merge.
        final Map<String,String> target = new HashMap<>();
        first.forEach(target::putIfAbsent);
        second.forEach(target::putIfAbsent);
        target.putAll(visibilityTransformer.mergeMetadataMaps(first, second));
        target.putAll(timestampTransformer.mergeMetadataMaps(first, second));
        return target;
    }
}
