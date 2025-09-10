package datawave.annotation.data;

import java.util.ArrayList;
import java.util.Collection;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Used to read and write annotation data to Accumulo */
public class AnnotationDataAccess<A,S> {

    public static final char NULL = '\0';
    public static final char MAX = '\uFFFF';

    protected static final Logger log = LoggerFactory.getLogger(AnnotationDataAccess.class);

    final AccumuloClient accumuloClient;
    final Authorizations authorizations;
    final String tableName;
    final AnnotationSerializer<Iterator<Map.Entry<Key,Value>>,A> annotationSerializer;

    public AnnotationDataAccess(AccumuloClient accumuloClient, Authorizations authorizations, String tableName, AnnotationSerializer<Iterator<Map.Entry<Key,Value>>,A> annotationSerializer) {
        this.accumuloClient = accumuloClient;
        this.authorizations = authorizations;
        this.tableName = tableName;
        this.annotationSerializer = annotationSerializer;
    }

    /** Get a specific annotation */
    public Optional<A> get(String shard, String datatype, String uid, String annotationType, String annotationUid) {
        try (Scanner scanner = accumuloClient.createScanner(tableName, authorizations)) {
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

            A a = annotationSerializer.deserialize(it);
            return a == null ? Optional.empty() : Optional.of(a);
        } catch (TableNotFoundException | AnnotationSerializationException e) {
            throw new AnnotationReadException(e.getClass().getSimpleName() + " reading annotation for document: " + shard + "/" + datatype + "/" + uid
                            + " annotationType: " + annotationType + " annotationUid: " + annotationUid, e);
        }
    }

    /** Get the annotation types for a document */
    public Collection<String> getTypes(String shard, String datatype, String uid) {
        try (Scanner scanner = accumuloClient.createScanner(tableName, authorizations)) {
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
            return extractTypes(it);
        } catch (TableNotFoundException e) {
            throw new AnnotationReadException(e.getClass().getSimpleName() + " reading annotation types for document: " + shard + "/" + datatype + "/" + uid, e);
        }
    }


    /** Get all annotations for a document */
    public List<A> getAll(String shard, String datatype, String uid) {
        try (Scanner scanner = accumuloClient.createScanner(tableName, authorizations)) {
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
            return processIterator(it);
        } catch (TableNotFoundException | AnnotationSerializationException e) {
            throw new AnnotationReadException(e.getClass().getSimpleName() + " reading all annotations for document: " + shard + "/" + datatype + "/" + uid, e);
        }
    }

    /** Get all annotations of a specific type for a document */
    public List<A> getAllForType(String shard, String datatype, String uid, String annotationType) {
        try (Scanner scanner = accumuloClient.createScanner(tableName, authorizations)) {
            final String columnFamily = datatype + NULL + uid + NULL + annotationType;

            final Key startKey = new Key(shard, columnFamily);
            final Key endKey = new Key(shard, columnFamily + MAX);
            final Range range = new Range(startKey, true, endKey, false);
            scanner.setRange(range);

            final IteratorSetting cfg = new IteratorSetting(50, "AnnotationDataAccess#getAllForType", RegExFilter.class);
            RegExFilter.setRegexs(cfg, shard, columnFamily, null, null, false, false);
            scanner.addScanIterator(cfg);

            Iterator<Map.Entry<Key,Value>> it = scanner.iterator();
            return processIterator(it);
        } catch (TableNotFoundException | AnnotationSerializationException e) {
            throw new AnnotationReadException(
                            e.getClass().getSimpleName() + " reading " + annotationType + " annotations for document: " + shard + "/" + datatype + "/" + uid,
                            e);
        }
    }

    public Optional<A> getAnnotation(String shard, String datatype, String uid, String annotationId) {

    }

    public Optional<S> getSegment(String shard, String datatype, String uid, String annotationId, String segmentId) {

    }

    /** Save an annotation */
    public void save(A a) {
        try (BatchWriter writer = accumuloClient.createBatchWriter(tableName)) {
            Iterator<Map.Entry<Key,Value>> it = annotationSerializer.serialize(a);
            Mutation m = AccumuloAnnotationUtil.mutationAdapter(it);
            writer.addMutation(m);
        } catch (TableNotFoundException | MutationsRejectedException | AnnotationSerializationException e) {
            throw new AnnotationWriteException(e.getClass().getSimpleName() + " saving annotation " + a, e);
        }
    }

    /** Update an annotation */
    public void update(A a) {
        // TODO: implement update
    }

    public void delete(A a) {
        // TODO: implement delete
    }

    /** The annotation type is always stored in the last slot of the column family, extract all of the types found
     *  in the iterator to a set.
     * @param it
     * @return a list of distinct annotation types.
     */
    public Collection<String> extractTypes(Iterator<Map.Entry<Key, Value>> it) {
        final Set<String> annotationTypes = new TreeSet<>();
        while (it.hasNext()) {
            Map.Entry<Key,Value> e = it.next();
            Key k = e.getKey();
            String cf = k.getColumnFamily().toString();
            int typeSep = cf.lastIndexOf(NULL);
            annotationTypes.add(cf.substring(typeSep+1));
        }
        return annotationTypes;
    }

    /** Extract the data referenced by the interator into a collection of Annotation objects.
     *
     * @param it
     * @return
     * @throws AnnotationSerializationException
     */
    public List<A> processIterator(Iterator<Map.Entry<Key,Value>> it) throws AnnotationSerializationException {
        final List<Map.Entry<Key,Value>> buffer = new ArrayList<>();
        final List<A> results = new ArrayList<>();
        String currentAnnotationId = null;

        // TODO: add visibility field to Annotation.
        // TODO: interpret ColumnVisibilities to set access controls.

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
                A a = annotationSerializer.deserialize(buffer.iterator());
                results.add(a);
                buffer.clear();

                currentAnnotationId = annotationId;
                buffer.add(e);

            }
        }

        if (!buffer.isEmpty()) {
            A a = annotationSerializer.deserialize(buffer.iterator());
            results.add(a);
            buffer.clear();
        }

        return results;
    }
}
