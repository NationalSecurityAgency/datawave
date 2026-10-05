package datawave.annotation.data.v1;

import static datawave.annotation.util.v1.AnnotationStringUtils.annotationIdContext;
import static datawave.annotation.util.v1.AnnotationStringUtils.annotationSourceString;
import static datawave.annotation.util.v1.AnnotationStringUtils.segmentWithAnnotationContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.annotation.data.AccumuloAnnotationUtil;
import datawave.annotation.data.AnnotationReadException;
import datawave.annotation.data.AnnotationSerializationException;
import datawave.annotation.data.AnnotationSerializer;
import datawave.annotation.data.AnnotationUpdateException;
import datawave.annotation.data.AnnotationWriteException;
import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.util.v1.AnnotationUtils;
import datawave.security.util.ScannerHelper;

/**
 * Accumulo-backed reader and writer for annotation data.
 * <p>
 * Annotations are addressed by document shard. The annotation table stores document-scoped annotations under column families composed of {@code datatype},
 * {@code uid}, and annotation type, separated by {@link #NULL}. Column qualifiers begin with the annotation id, which allows scans to target either a known
 * annotation type or all types for a document. Annotation sources are stored separately and addressed by analytic hash.
 */
public class AnnotationDataAccess implements AnnotationReader, AnnotationWriter {

    public static final char NULL = '\u0000';
    public static final char MAX = '\uFFFF';

    protected static final Logger log = LoggerFactory.getLogger(AnnotationDataAccess.class);

    final AccumuloClient accumuloClient;
    final Set<Authorizations> authorizations;
    final String annotationTableName;
    final String annotationSourceTableName;
    final AnnotationSerializer<Iterator<Map.Entry<Key,Value>>,Annotation> annotationSerializer;
    final AnnotationSerializer<Iterator<Map.Entry<Key,Value>>,AnnotationSource> annotationSourceSerializer;

    // TODO: feature flags for now, ultimately these might get externalized in a configuration object.
    boolean blockAnnotationSourceOverwrites = false;
    boolean blockAnnotationOverwrites = false;

    /**
     * Creates an Accumulo annotation data access object.
     * <p>
     * The supplied serializers define the physical key/value representation used by all read and write operations. The same authorizations are used for every
     * scanner created by this instance.
     *
     * @param accumuloClient
     *            the Accumulo client used to create scanners and batch writers
     * @param authorizations
     *            the authorizations used when scanning annotation data
     * @param annotationTableName
     *            the Accumulo table that stores annotations
     * @param annotationSourceTableName
     *            the Accumulo table that stores annotation sources
     * @param annotationSerializer
     *            the serializer that transforms annotations to and from Accumulo entries
     * @param annotationSourceSerializer
     *            the serializer that transforms annotation sources to and from Accumulo entries
     */
    public AnnotationDataAccess(AccumuloClient accumuloClient, Set<Authorizations> authorizations, String annotationTableName, String annotationSourceTableName,
                    AnnotationSerializer<Iterator<Map.Entry<Key,Value>>,Annotation> annotationSerializer,
                    AnnotationSerializer<Iterator<Map.Entry<Key,Value>>,AnnotationSource> annotationSourceSerializer) {
        this.accumuloClient = accumuloClient;
        this.authorizations = authorizations;
        this.annotationTableName = annotationTableName;
        this.annotationSourceTableName = annotationSourceTableName;
        this.annotationSerializer = annotationSerializer;
        this.annotationSourceSerializer = annotationSourceSerializer;
    }

    /**
     * Indicates whether this instance rejects writes for annotations whose generated id already exists.
     *
     * @return {@code true} when annotation id conflicts cause writes to fail
     */
    public boolean isBlockAnnotationOverwrites() {
        return blockAnnotationOverwrites;
    }

    /**
     * Configures whether this instance rejects writes for annotations whose generated id already exists.
     *
     * @param blockAnnotationOverwrites
     *            {@code true} to reject annotation id conflicts, or {@code false} to allow overwrites
     */
    public void setBlockAnnotationOverwrites(boolean blockAnnotationOverwrites) {
        this.blockAnnotationOverwrites = blockAnnotationOverwrites;
    }

    /**
     * Indicates whether this instance rejects writes for annotation sources whose generated analytic hash already exists.
     *
     * @return {@code true} when annotation source id conflicts cause writes to fail
     */
    public boolean isBlockAnnotationSourceOverwrites() {
        return blockAnnotationSourceOverwrites;
    }

    /**
     * Configures whether this instance rejects writes for annotation sources whose generated analytic hash already exists.
     *
     * @param blockAnnotationSourceOverwrites
     *            {@code true} to reject annotation source id conflicts, or {@code false} to allow overwrites
     */
    public void setBlockAnnotationSourceOverwrites(boolean blockAnnotationSourceOverwrites) {
        this.blockAnnotationSourceOverwrites = blockAnnotationSourceOverwrites;
    }

    /**
     * Reads an annotation source from the annotation source table using an exact row range on the analytic hash.
     * <p>
     * The source serializer consumes all entries in the row and returns {@code null} when the row contains no visible source data.
     *
     * @param analyticHash
     *            the analytic hash row to scan
     * @return the deserialized annotation source, or {@link Optional#empty()} when no source is found
     * @throws AnnotationReadException
     *             if the source table is missing or the entries cannot be deserialized
     */
    @Override
    public Optional<AnnotationSource> getAnnotationSource(String analyticHash) {
        try (Scanner scanner = ScannerHelper.createScanner(accumuloClient, annotationSourceTableName, authorizations)) {
            final Range range = new Range(analyticHash);
            scanner.setRange(range);

            Iterator<Map.Entry<Key,Value>> it = scanner.iterator();
            AnnotationSource as = annotationSourceSerializer.deserialize(it);
            return as == null ? Optional.empty() : Optional.of(as);
        } catch (TableNotFoundException | AnnotationSerializationException e) {
            throw new AnnotationReadException(e.getClass().getSimpleName() + " reading annotation source for analyticHash: " + analyticHash, e);
        }
    }

    /**
     * Reads a single annotation when the annotation type and id are both known.
     * <p>
     * This method narrows the scan to a single shard, the exact annotation column family ({@code datatype\0uid\0annotationType}), and the column qualifier
     * range for {@code annotationUid}. A {@link RegExFilter} is also applied so only entries for the requested annotation id are deserialized.
     *
     * @param shard
     *            the shard for the annotated document
     * @param datatype
     *            the datatype for the annotated document
     * @param uid
     *            the unique id for the annotated document
     * @param annotationType
     *            the annotation type to search
     * @param annotationUid
     *            the annotation id to retrieve
     * @return the deserialized annotation, or {@link Optional#empty()} when no matching entries are visible
     * @throws AnnotationReadException
     *             if the annotation table is missing or the entries cannot be deserialized
     */
    @Override
    public Optional<Annotation> getAnnotation(String shard, String datatype, String uid, String annotationType, String annotationUid) {
        try (Scanner scanner = ScannerHelper.createScanner(accumuloClient, annotationTableName, authorizations)) {
            final String columnFamily = datatype + NULL + uid + NULL + annotationType;
            final String columnQualifierPrefix = annotationUid + NULL;
            final String columnQualifierRegex = columnQualifierPrefix + ".*";

            final Key startKey = new Key(shard, columnFamily, columnQualifierPrefix + NULL);
            final Key endKey = new Key(shard, columnFamily, columnQualifierPrefix + MAX);
            final Range range = new Range(startKey, true, endKey, false);
            scanner.setRange(range);

            final IteratorSetting cfg = new IteratorSetting(50, "AnnotationDataAccess#get", RegExFilter.class);
            RegExFilter.setRegexs(cfg, shard, columnFamily, columnQualifierRegex, null, false, false);
            scanner.addScanIterator(cfg);

            Iterator<Map.Entry<Key,Value>> it = scanner.iterator();

            Annotation a = annotationSerializer.deserialize(it);
            return a == null ? Optional.empty() : Optional.of(a);
        } catch (TableNotFoundException | AnnotationSerializationException e) {
            throw new AnnotationReadException(e.getClass().getSimpleName() + " reading annotation for document: " + shard + "/" + datatype + "/" + uid
                            + " annotationType: " + annotationType + " annotationUid: " + annotationUid, e);
        }
    }

    /**
     * Reads a single annotation by id without requiring the caller to know its annotation type.
     * <p>
     * This method scans all annotation type column families for the document ({@code datatype\0uid\0*}) and filters column qualifiers that begin with
     * {@code annotationId\0}. If entries deserialize into more than one annotation, the data is considered ambiguous and an {@link AnnotationReadException} is
     * thrown.
     *
     * @param shard
     *            the shard for the annotated document
     * @param datatype
     *            the datatype for the annotated document
     * @param uid
     *            the unique id for the annotated document
     * @param annotationId
     *            the id of the annotation we want to retrieve
     * @return the matching annotation, or {@link Optional#empty()} when no matching entries are visible
     * @throws AnnotationReadException
     *             if the annotation table is missing, the entries cannot be deserialized, or multiple annotations share the requested id for the document
     */
    @Override
    public Optional<Annotation> getAnnotation(String shard, String datatype, String uid, String annotationId) {
        try (Scanner scanner = ScannerHelper.createScanner(accumuloClient, annotationTableName, authorizations)) {
            final String columnFamily = datatype + NULL + uid + NULL;
            final String columnFamilyRegex = columnFamily + ".*";
            final String columnQualifierRegex = annotationId + NULL + ".*";

            final Key startKey = new Key(shard, columnFamily);
            final Key endKey = new Key(shard, columnFamily + MAX);
            final Range range = new Range(startKey, true, endKey, false);
            scanner.setRange(range);

            final IteratorSetting cfg = new IteratorSetting(50, "AnnotationDataAccess#getAnnotation", RegExFilter.class);
            RegExFilter.setRegexs(cfg, shard, columnFamilyRegex, columnQualifierRegex, null, false, false);
            scanner.addScanIterator(cfg);

            Iterator<Map.Entry<Key,Value>> it = scanner.iterator();
            List<Annotation> annotations = extractAnnotationsFromIterator(it);
            if (annotations.isEmpty()) {
                return Optional.empty();
            } else if (annotations.size() > 1) {
                throw new AnnotationReadException("Multiple entries (" + annotations.size() + "), found for annotationId " + annotationId + " for document: "
                                + shard + "/" + datatype + "/" + uid);
            }

            return Optional.of(annotations.get(0));
        } catch (TableNotFoundException | AnnotationSerializationException e) {
            throw new AnnotationReadException(
                            e.getClass().getSimpleName() + " reading annotationId " + annotationId + " for document: " + shard + "/" + datatype + "/" + uid, e);
        }
    }

    /**
     * Reads the distinct annotation types for a document.
     * <p>
     * The scan is bounded to the document's annotation column-family prefix ({@code datatype\0uid\0}) and extracts the last column-family component from each
     * visible entry. Returned values are distinct and naturally ordered by the backing {@link TreeSet}.
     *
     * @param shard
     *            the shard for the annotated document
     * @param datatype
     *            the datatype for the annotated document
     * @param uid
     *            the unique id for the annotated document
     * @return the distinct annotation types visible for the document, never {@code null}
     * @throws AnnotationReadException
     *             if the annotation table is missing
     */
    @Override
    public Collection<String> getAnnotationTypes(String shard, String datatype, String uid) {
        try (Scanner scanner = ScannerHelper.createScanner(accumuloClient, annotationTableName, authorizations)) {
            final String columnFamilyPrefix = datatype + NULL + uid + NULL;
            final String columnFamilyRegex = columnFamilyPrefix + ".*";

            final Key startKey = new Key(shard, columnFamilyPrefix + NULL);
            final Key endKey = new Key(shard, columnFamilyPrefix + MAX);
            final Range range = new Range(startKey, true, endKey, false);
            scanner.setRange(range);

            final IteratorSetting cfg = new IteratorSetting(50, "AnnotationDataAccess#getAll", RegExFilter.class);
            RegExFilter.setRegexs(cfg, shard, columnFamilyRegex, null, null, false, false);
            scanner.addScanIterator(cfg);

            Iterator<Map.Entry<Key,Value>> it = scanner.iterator();
            return extractTypesFromIterator(it);
        } catch (TableNotFoundException e) {
            throw new AnnotationReadException(e.getClass().getSimpleName() + " reading annotation types for document: " + shard + "/" + datatype + "/" + uid,
                            e);
        }
    }

    /**
     * Reads all annotations for a document across all annotation types.
     * <p>
     * The scan is bounded to the document's annotation column-family prefix ({@code datatype\0uid\0}) and filtered to entries in the requested shard. Entries
     * are grouped by annotation id before deserialization.
     *
     * @param shard
     *            the shard for the annotated document
     * @param datatype
     *            the datatype for the annotated document
     * @param uid
     *            the unique id for the annotated document
     * @return the annotations visible for the document, never {@code null}
     * @throws AnnotationReadException
     *             if the annotation table is missing or any grouped annotation cannot be deserialized
     */
    @Override
    public Collection<Annotation> getAnnotations(String shard, String datatype, String uid) {
        try (Scanner scanner = ScannerHelper.createScanner(accumuloClient, annotationTableName, authorizations)) {
            final String columnFamilyPrefix = datatype + NULL + uid + NULL;
            final String columnFamilyRegex = columnFamilyPrefix + ".*";

            final Key startKey = new Key(shard, columnFamilyPrefix + NULL);
            final Key endKey = new Key(shard, columnFamilyPrefix + MAX);
            final Range range = new Range(startKey, true, endKey, false);
            scanner.setRange(range);

            final IteratorSetting cfg = new IteratorSetting(50, "AnnotationDataAccess#getAll", RegExFilter.class);
            RegExFilter.setRegexs(cfg, shard, columnFamilyRegex, null, null, false, false);
            scanner.addScanIterator(cfg);

            Iterator<Map.Entry<Key,Value>> it = scanner.iterator();
            return extractAnnotationsFromIterator(it);
        } catch (TableNotFoundException | AnnotationSerializationException e) {
            throw new AnnotationReadException(e.getClass().getSimpleName() + " reading all annotations for document: " + shard + "/" + datatype + "/" + uid, e);
        }
    }

    /**
     * Reads all annotations of a specific type for a document.
     * <p>
     * This method scans the exact annotation column family ({@code datatype\0uid\0annotationType}) for the requested shard. Entries are grouped by annotation
     * id before deserialization.
     *
     * @param shard
     *            the shard for the annotated document
     * @param datatype
     *            the datatype for the annotated document
     * @param uid
     *            the unique id for the annotated document
     * @param annotationType
     *            the annotation type to retrieve
     * @return the matching annotations visible for the document and type, never {@code null}
     * @throws AnnotationReadException
     *             if the annotation table is missing or any grouped annotation cannot be deserialized
     */
    @Override
    public Collection<Annotation> getAnnotationsForType(String shard, String datatype, String uid, String annotationType) {
        try (Scanner scanner = ScannerHelper.createScanner(accumuloClient, annotationTableName, authorizations)) {
            final String columnFamily = datatype + NULL + uid + NULL + annotationType;

            final Key startKey = new Key(shard, columnFamily);
            final Key endKey = new Key(shard, columnFamily + MAX);
            final Range range = new Range(startKey, true, endKey, false);
            scanner.setRange(range);

            final IteratorSetting cfg = new IteratorSetting(50, "AnnotationDataAccess#getAllForType", RegExFilter.class);
            RegExFilter.setRegexs(cfg, shard, columnFamily, null, null, false, false);
            scanner.addScanIterator(cfg);

            Iterator<Map.Entry<Key,Value>> it = scanner.iterator();
            return extractAnnotationsFromIterator(it);
        } catch (TableNotFoundException | AnnotationSerializationException e) {
            throw new AnnotationReadException(e.getClass().getSimpleName() + " reading " + annotationType + " type annotations for document: " + shard + "/"
                            + datatype + "/" + uid, e);
        }
    }

    /**
     * Writes an annotation source to the annotation source table.
     * <p>
     * Before writing, this method validates that the caller has not supplied a store-managed id, injects annotation source hashes, and checks for id conflicts
     * according to {@link #isBlockAnnotationSourceOverwrites()}. The prepared source is serialized and written as a non-delete mutation.
     *
     * @param annotationSource
     *            the annotation source to save
     * @return the saved annotation source, including generated hashes
     * @throws AnnotationWriteException
     *             if validation, serialization, table access, or mutation writing fails
     */
    @Override
    public Optional<AnnotationSource> addAnnotationSource(AnnotationSource annotationSource) {
        AnnotationSource addedAnnotationSource = prepareAnnotationSourceForAdd(annotationSource);
        try (BatchWriter writer = accumuloClient.createBatchWriter(annotationSourceTableName)) {
            Iterator<Map.Entry<Key,Value>> it = annotationSourceSerializer.serialize(addedAnnotationSource);
            Mutation m = AccumuloAnnotationUtil.mutationAdapter(it, false);
            writer.addMutation(m);
            return Optional.of(addedAnnotationSource);
        } catch (TableNotFoundException | MutationsRejectedException | AnnotationSerializationException e) {
            throw new AnnotationWriteException(e.getClass().getSimpleName() + " saving annotation source " + addedAnnotationSource, e);
        }
    }

    /**
     * Writes an annotation to the annotation table.
     * <p>
     * Before writing, this method validates that the caller has not supplied store-managed annotation or segment ids, injects all annotation and segment
     * hashes, and checks for id conflicts according to {@link #isBlockAnnotationOverwrites()}. The prepared annotation is serialized and written as a
     * non-delete mutation.
     *
     * @param annotation
     *            the annotation to save
     * @return the saved annotation, including generated annotation and segment ids
     * @throws AnnotationWriteException
     *             if validation, serialization, table access, or mutation writing fails
     */
    @Override
    public Optional<Annotation> addAnnotation(Annotation annotation) {
        Annotation addedAnnotation = prepareAnnotationForAdd(annotation);
        try (BatchWriter writer = accumuloClient.createBatchWriter(annotationTableName)) {
            Iterator<Map.Entry<Key,Value>> it = annotationSerializer.serialize(addedAnnotation);
            Mutation m = AccumuloAnnotationUtil.mutationAdapter(it, false);
            writer.addMutation(m);
            return Optional.of(addedAnnotation);
        } catch (TableNotFoundException | MutationsRejectedException | AnnotationSerializationException e) {
            throw new AnnotationWriteException(e.getClass().getSimpleName() + " saving annotation " + addedAnnotation, e);
        }
    }

    /**
     * Writes a new annotation version that references an existing annotation id.
     * <p>
     * This implementation first verifies that {@code targetAnnotationId} exists for the document identified by the replacement annotation. It then injects an
     * update reference into the replacement annotation and delegates to {@link #addAnnotation(Annotation)}. The original annotation remains stored; this method
     * does not overwrite or delete it.
     *
     * @param targetAnnotationId
     *            the identifier for the existing annotation being updated
     * @param annotation
     *            the replacement annotation data to save as a new version
     * @return the saved update annotation, including generated annotation and segment ids
     * @throws AnnotationUpdateException
     *             if the target annotation cannot be found or the update annotation cannot be saved
     * @throws AnnotationWriteException
     *             if the delegated add operation fails validation, serialization, table access, or mutation writing
     */
    @Override
    public Optional<Annotation> updateAnnotation(String targetAnnotationId, Annotation annotation) {
        String shard = annotation.getShard();
        String datatype = annotation.getDataType();
        String uid = annotation.getUid();

        Optional<Annotation> targetAnnotation = getAnnotation(shard, datatype, uid, targetAnnotationId);
        if (targetAnnotation.isEmpty()) {
            throw new AnnotationUpdateException("Unable to find annotation to update for document: " + shard + "/" + datatype + "/" + uid
                            + " and annotation id: " + targetAnnotationId);
        }

        Annotation referenceAnnotation = AnnotationUtils.injectUpdateReference(annotation, targetAnnotationId);

        Optional<Annotation> addedAnnotation = addAnnotation(referenceAnnotation);
        if (addedAnnotation.isEmpty()) {
            throw new AnnotationUpdateException(
                            "Unable to add annotation for document: " + shard + "/" + datatype + "/" + uid + " and annotation id: " + targetAnnotationId);
        }

        return addedAnnotation;
    }

    /**
     * Deletes all Accumulo entries for an annotation id on a document.
     * <p>
     * This method scans all annotation type column families for the document and filters entries whose column qualifier begins with {@code annotationId\0}. The
     * matching entries are converted to delete mutations and written back to the annotation table.
     *
     * @param shard
     *            the shard for the annotated document
     * @param datatype
     *            the datatype for the annotated document
     * @param uid
     *            the unique id for the annotated document
     * @param annotationId
     *            the annotation id to delete
     * @throws AnnotationUpdateException
     *             if table access, serialization, or mutation writing fails
     */
    @Override
    public void deleteAnnotation(String shard, String datatype, String uid, String annotationId) {
        try (Scanner scanner = ScannerHelper.createScanner(accumuloClient, annotationTableName, authorizations);
                        BatchWriter writer = accumuloClient.createBatchWriter(annotationTableName)) {
            final String columnFamily = datatype + NULL + uid + NULL;
            final String columnFamilyRegex = columnFamily + ".*";
            final String columnQualifierRegex = annotationId + NULL + ".*";

            final Key startKey = new Key(shard, columnFamily);
            final Key endKey = new Key(shard, columnFamily + MAX);
            final Range range = new Range(startKey, true, endKey, false);
            scanner.setRange(range);

            final IteratorSetting cfg = new IteratorSetting(50, "AnnotationDataAccess#deleteAnnotation", RegExFilter.class);
            RegExFilter.setRegexs(cfg, shard, columnFamilyRegex, columnQualifierRegex, null, false, false);
            scanner.addScanIterator(cfg);

            Iterator<Map.Entry<Key,Value>> it = scanner.iterator(); // these contain the entries for which we need to generate delete mutations.
            Mutation m = AccumuloAnnotationUtil.mutationAdapter(it, true);
            writer.addMutation(m);
        } catch (TableNotFoundException | AnnotationSerializationException | MutationsRejectedException e) {
            throw new AnnotationUpdateException(
                            e.getClass().getSimpleName() + " deleting annotationId " + annotationId + " for document: " + shard + "/" + datatype + "/" + uid,
                            e);
        }
    }

    /**
     * Prepares an annotation source for insertion into Accumulo.
     * <ol>
     * <li>validate that caller-managed data does not include store-managed ids</li>
     * <li>assign deterministic annotation source hashes</li>
     * <li>check the generated analytic hash for conflicts</li>
     * </ol>
     *
     * @param annotationSource
     *            the annotation source to prepare
     * @return the annotation source that should be serialized and written
     * @throws AnnotationWriteException
     *             if validation or conflict checks fail
     */
    protected AnnotationSource prepareAnnotationSourceForAdd(AnnotationSource annotationSource) {
        validateAnnotationSourceForAdd(annotationSource);
        AnnotationSource identifiedAnnotationSource = AnnotationUtils.injectAnnotationSourceHashes(annotationSource);
        checkAnnotationSourceForConflicts(identifiedAnnotationSource);
        return identifiedAnnotationSource;
    }

    /**
     * Validates source data before this implementation assigns annotation source hashes.
     * <p>
     * Callers are not allowed to provide the analytic hash for a new source because the hash is a store-managed identifier derived by
     * {@link AnnotationUtils#injectAnnotationSourceHashes(AnnotationSource)}.
     *
     * @param annotationSource
     *            the annotation source to validate
     * @throws AnnotationWriteException
     *             if the annotation source already has an analytic hash
     */
    protected void validateAnnotationSourceForAdd(AnnotationSource annotationSource) {
        if (StringUtils.isNotBlank(annotationSource.getAnalyticHash())) {
            throw new AnnotationWriteException(
                            "Cannot add annotation source because it already has an id assigned '" + annotationSourceString(annotationSource));
        }
    }

    /**
     * Checks whether a prepared annotation source can be written.
     * <ol>
     * <li>the generated analytic hash must be present</li>
     * <li>when overwrite blocking is enabled, no visible annotation source may already exist for the same analytic hash</li>
     * </ol>
     *
     * @param annotationSource
     *            the prepared annotation source to check
     * @throws AnnotationWriteException
     *             if the analytic hash is missing or a blocked conflict is found
     */
    protected void checkAnnotationSourceForConflicts(AnnotationSource annotationSource) {
        // check that the annotation has an id assigned.
        if (StringUtils.isBlank(annotationSource.getAnalyticHash())) {
            throw new AnnotationWriteException(
                            "Cannot add annotation source because the id could not be automatically assigned '" + annotationSourceString(annotationSource));
        }

        Optional<AnnotationSource> conflicting = getAnnotationSource(annotationSource.getAnalyticHash());
        if (conflicting.isPresent()) {
            if (blockAnnotationSourceOverwrites) {
                throw new AnnotationWriteException("Cannot add annotation source because an annotation source with the same id already exists.");
            } else if (log.isDebugEnabled()) {
                log.debug("Allowing annotation source overwrite: {}", annotationSourceString(annotationSource));
            }
        }

    }

    /**
     * Prepares an annotation for insertion into Accumulo.
     * <ol>
     * <li>validate that caller-managed data does not include store-managed ids</li>
     * <li>assign deterministic annotation and segment hashes</li>
     * <li>check the generated annotation and segment ids for conflicts</li>
     * </ol>
     *
     * @param annotation
     *            the annotation to prepare
     * @return the annotation that should be serialized and written
     * @throws AnnotationWriteException
     *             if validation or conflict checks fail
     */
    protected Annotation prepareAnnotationForAdd(Annotation annotation) {
        validateAnnotationForAdd(annotation);
        Annotation identifiedAnnotation = AnnotationUtils.injectAllHashes(annotation);
        checkAnnotationForConflicts(identifiedAnnotation);
        return identifiedAnnotation;
    }

    /**
     * Validates annotation data before this implementation assigns annotation and segment hashes.
     * <p>
     * Callers are not allowed to provide annotation or segment ids for a new annotation because those are store-managed identifiers derived by
     * {@link AnnotationUtils#injectAllHashes(Annotation)}.
     *
     * @param annotation
     *            the annotation to validate
     * @throws AnnotationWriteException
     *             if the annotation already has an annotation id or any segment already has a segment hash
     */
    protected void validateAnnotationForAdd(Annotation annotation) {
        if (StringUtils.isNotBlank(annotation.getAnnotationId())) {
            throw new AnnotationWriteException("Cannot add annotation because it already has an id assigned " + annotationIdContext(annotation));
        }
        for (Segment segment : annotation.getSegmentsList()) {
            if (StringUtils.isNotBlank(segment.getSegmentHash())) {
                throw new AnnotationWriteException(
                                "Cannot add segment for annotation because it already has an id assigned " + segmentWithAnnotationContext(segment, annotation));
            }
        }
    }

    /**
     * Checks whether a prepared annotation can be written.
     * <ol>
     * <li>the generated annotation id must be present</li>
     * <li>every segment must have a generated id</li>
     * <li>segment ids must be unique within the annotation</li>
     * <li>when overwrite blocking is enabled, no visible annotation may already exist for the same document and annotation id</li>
     * </ol>
     *
     * @param annotation
     *            the prepared annotation to check
     * @throws AnnotationWriteException
     *             if ids are missing, duplicate segment ids are found, or a blocked annotation conflict is found
     */
    protected void checkAnnotationForConflicts(Annotation annotation) {
        // check that the annotation has an id assigned.
        if (StringUtils.isBlank(annotation.getAnnotationId())) {
            throw new AnnotationWriteException("Cannot add annotation because the id could not be automatically assigned " + annotationIdContext(annotation));
        }

        // ensure that the segments have ids assigned and are unique.
        final Set<String> observedSegmentIds = new HashSet<>();
        for (Segment segment : annotation.getSegmentsList()) {
            if (StringUtils.isBlank(segment.getSegmentHash())) {
                throw new AnnotationWriteException(
                                "Cannot add segment because the id could not be automatically assigned " + segmentWithAnnotationContext(segment, annotation));
            }
            final String segmentHash = segment.getSegmentHash();
            if (!observedSegmentIds.add(segmentHash)) {
                throw new AnnotationWriteException("Cannot add annotation because it contains multiple segments with the same id " + annotation);
            }
        }
        Optional<Annotation> conflicting = getAnnotation(annotation.getShard(), annotation.getDataType(), annotation.getUid(), annotation.getAnnotationId());
        if (conflicting.isPresent()) {
            if (blockAnnotationOverwrites) {
                throw new AnnotationWriteException("Cannot add annotation because an annotation with the same id already exists.");
            } else if (log.isDebugEnabled()) {
                log.debug("Allowing annotation overwrite: {}", annotationIdContext(conflicting.get()));
            }
        }
    }

    /**
     * Extracts distinct annotation types from Accumulo entries.
     * <p>
     * Annotation column families are encoded as {@code datatype\0uid\0annotationType}; this method returns the component after the last {@link #NULL} delimiter
     * for each entry. Results are returned in natural order.
     *
     * @param it
     *            the Accumulo entry iterator to inspect
     * @return distinct annotation types found in the iterator, never {@code null}
     */
    public static Collection<String> extractTypesFromIterator(Iterator<Map.Entry<Key,Value>> it) {
        final Set<String> annotationTypes = new TreeSet<>();
        while (it.hasNext()) {
            Map.Entry<Key,Value> e = it.next();
            Key k = e.getKey();
            String cf = k.getColumnFamily().toString();
            int typeSep = cf.lastIndexOf(NULL);
            annotationTypes.add(cf.substring(typeSep + 1));
        }
        return annotationTypes;
    }

    /**
     * Groups Accumulo entries by annotation id and deserializes each group into an annotation.
     * <p>
     * This method expects entries to be ordered so all key/value pairs for a given annotation id are contiguous. The annotation id is read from the first
     * component of the column qualifier before the first {@link #NULL} delimiter. Each completed group is passed to the configured annotation serializer.
     *
     * @param it
     *            the Accumulo entry iterator to process
     * @return annotations deserialized from the iterator, never {@code null}
     * @throws AnnotationSerializationException
     *             if any grouped annotation entries cannot be deserialized
     */
    public List<Annotation> extractAnnotationsFromIterator(Iterator<Map.Entry<Key,Value>> it) throws AnnotationSerializationException {
        final List<Map.Entry<Key,Value>> buffer = new ArrayList<>();
        final List<Annotation> results = new ArrayList<>();
        String currentAnnotationId = null;

        // TODO: interpret ColumnVisibilities and set the visibility metadata using the AnnotationVisibilityTransformer interface.

        while (it.hasNext()) {
            Map.Entry<Key,Value> e = it.next();
            Key k = e.getKey();
            String cq = k.getColumnQualifier().toString();
            int idSep = cq.indexOf(NULL);
            String annotationId = cq.substring(0, idSep);
            if (currentAnnotationId == null) {
                // new iterator of keys to process
                currentAnnotationId = annotationId;
                buffer.add(e);
            } else if (currentAnnotationId.equals(annotationId)) {
                // still collecting keys for the current annotation.
                buffer.add(e);
            } else {
                // we've found new annotation, process the buffer,
                // clear it and add the current entry to the cleared empty buffer.
                Annotation a = annotationSerializer.deserialize(buffer.iterator());
                results.add(a);
                buffer.clear();

                currentAnnotationId = annotationId;
                buffer.add(e);

            }
        }

        if (!buffer.isEmpty()) {
            Annotation a = annotationSerializer.deserialize(buffer.iterator());
            results.add(a);
            buffer.clear();
        }

        return results;
    }
}
