package datawave.query.transformer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.TreeSet;

import javax.annotation.Nullable;

import org.apache.accumulo.core.data.Key;
import org.apache.commons.collections4.keyvalue.UnmodifiableMapEntry;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

import datawave.core.iterators.filesystem.FileSystemCache;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.ResultPostprocessor;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.attributes.DocumentKey;
import datawave.query.attributes.UniqueFields;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.iterator.ivarator.IvaratorCacheDir;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.iterator.profile.FinalDocumentTrackingIterator;
import datawave.query.util.sortedset.ByteArrayComparator;
import datawave.query.util.sortedset.FileByteDocumentSortedSet;
import datawave.query.util.sortedset.FileKeyValueSortedSet;
import datawave.query.util.sortedset.FileSortedSet;
import datawave.query.util.sortedset.HdfsBackedSortedSet;
import datawave.query.util.sortedset.RewritableSortedSetImpl;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;

/**
 * This iterator will filter documents based on uniqueness across a set of configured fields. Only the first instance of an event with a unique set of those
 * fields will be returned unless mostRecentUnique is specified in which case the most recent instance of an event will be returned. This transform is thread
 * safe.
 */
public class UniqueTransform extends DocumentTransform.DefaultDocumentTransform implements ResultPostprocessor {

    private static final Logger log = Logger.getLogger(UniqueTransform.class);

    private BloomFilter<byte[]> bloom;
    private UniqueFields uniqueFields = new UniqueFields();
    private HdfsBackedSortedSet<Entry<byte[],Object>> set;
    private HdfsBackedSortedSet<Entry<Key,Document>> returnSet;
    private Iterator<Entry<Key,Document>> setIterator;

    /**
     * Length of time in milliseconds that a client will wait while results are collected. If a full page is not collected before the timeout, a blank page will
     * be returned to signal the request is still in progress.
     */
    private final long queryExecutionForPageTimeout;

    /**
     * Create a new {@link UniqueTransform} that will use a bloom filter to return on those results that are unique per the uniqueFields. Special uniqueness can
     * be requested for date/time fields (@see UniqueFields).
     *
     * @param uniqueFields
     *            The unique fields
     * @param queryExecutionForPageTimeout
     *            If this timeout is passed before since the last result was returned, then an "intermediate" result is returned denoting we are still looking
     *            for the next unique result.
     */
    public UniqueTransform(UniqueFields uniqueFields, long queryExecutionForPageTimeout) {
        this.queryExecutionForPageTimeout = queryExecutionForPageTimeout;
        this.uniqueFields = uniqueFields;
        this.bloom = BloomFilter.create(new ByteFunnel(), 500000, 1e-15);
        if (log.isTraceEnabled()) {
            log.trace("unique fields: " + this.uniqueFields.getFields());
        }
    }

    /**
     * reset the bloom filter with this one
     *
     * @param filter
     */
    public void setFilter(BloomFilter<byte[]> filter) {
        this.bloom = filter;
    }

    /**
     * Update the configuration of this transform. If the configuration is actually changing, then the bloom filter will be reset as well.
     *
     *
     * @param uniqueFields
     *            The new set of unique fields.
     */
    public void updateConfig(UniqueFields uniqueFields) {
        // only reset the bloom filter if changing the field set
        if (!this.uniqueFields.equals(uniqueFields)) {
            this.uniqueFields = uniqueFields.clone();
            log.info("Resetting unique fields on the unique transform");
            this.bloom = BloomFilter.create(new ByteFunnel(), 500000, 1e-15);
            if (log.isTraceEnabled()) {
                log.trace("unique fields: " + this.uniqueFields.getFields());
            }
        }
    }

    /**
     * Add phrase excerpts to the documents from the given iterator.
     *
     * @param in
     *            the iterator source
     * @return an iterator that will supply the enriched documents
     */
    public Iterator<Entry<Key,Document>> getIterator(final Iterator<Entry<Key,Document>> in) {
        return new UniqueTransformIterator(in);
    }

    /**
     * Apply uniqueness to a document.
     *
     * @param keyDocumentEntry
     *            document entry
     * @return The document if unique per the configured fields, null otherwise.
     */
    @Nullable
    @Override
    public Entry<Key,Document> apply(@Nullable Entry<Key,Document> keyDocumentEntry) {
        if (keyDocumentEntry != null) {
            if (FinalDocumentTrackingIterator.isFinalDocumentKey(keyDocumentEntry.getKey())) {
                return keyDocumentEntry;
            }

            try {
                if (set != null) {
                    byte[] signature = getBytes(keyDocumentEntry.getValue());
                    synchronized (set) {
                        this.set.add(new UnmodifiableMapEntry(signature, keyDocumentEntry.getValue()));
                    }
                    return null;
                } else if (!isDuplicate(keyDocumentEntry.getValue())) {
                    return keyDocumentEntry;
                }
            } catch (IOException ioe) {
                log.error("Failed to convert document to bytes.  Returning document as unique.", ioe);
            }

            long elapsedExecutionTimeForCurrentPage = System.currentTimeMillis() - this.queryExecutionForPageStartTime;
            if (elapsedExecutionTimeForCurrentPage > this.queryExecutionForPageTimeout) {
                Document intermediateResult = new Document();
                intermediateResult.setIntermediateResult(true);
                return Maps.immutableEntry(keyDocumentEntry.getKey(), intermediateResult);
            }
        }

        return null;
    }

    /**
     * Part of the ResultPostprocessor interface
     */
    @Override
    public void apply(List<Object> results) {
        // these results should be EventBase objects
        List<Object> resultsToKeep = new ArrayList<>();
        for (Object result : results) {
            EventBase event = (EventBase) result;
            if (!event.isIntermediateResult()) {
                log.info("Testing " + event.getMetadata());
                try {
                    if (set != null) {
                        byte[] signature = getBytes(event);
                        synchronized (set) {
                            this.set.add(new UnmodifiableMapEntry(signature, event));
                        }
                    } else if (!isDuplicate(event)) {
                        log.info("Keeping " + event.getMetadata());
                        resultsToKeep.add(event);
                    }
                } catch (IOException ioe) {
                    log.error("Failed to convert document to bytes.  Returning document as unique.", ioe);
                }
            }
        }
        results.clear();
        if (set != null) {
            set.stream().forEach(e -> results.add(e.getValue()));
            // clear the set for next round
            set.clear();
        } else {
            results.addAll(resultsToKeep);
            // reset the bloom filter for next round
            this.bloom = BloomFilter.create(new ByteFunnel(), 500000, 1e-15);
        }
    }

    /**
     * This will start pulling data from the hdfs backed set if one exists (only if mostRecent is true).
     *
     * @return The next unique document from the set.
     */
    @Override
    public Map.Entry<Key,Document> flush() {
        if (set != null) {
            synchronized (set) {
                if (setIterator == null) {
                    setupIterator();
                }
                if (setIterator.hasNext()) {
                    return (Map.Entry<Key,Document>) setIterator.next();
                }
            }
        }
        return null;
    }

    /**
     * This will run through the set and create a new set ordered by Key, Document
     */
    private void setupIterator() {
        for (Map.Entry<byte[],Object> entry : set) {
            Document d = (Document) entry.getValue();
            returnSet.add(new UnmodifiableMapEntry<>(getDocKey(d), d));
        }
        setIterator = returnSet.iterator();
    }

    /**
     * Determine if a document is unique per the fields specified. If we have seen this set of fields and values before, then it is not unique.
     *
     * @param result
     *            a document or event
     * @return if a document is unique per the fields specified
     * @throws IOException
     *             for issues with read/write
     */
    private boolean isDuplicate(Object result) throws IOException {
        byte[] bytes = getBytes(result);
        synchronized (bloom) {
            if (bloom.mightContain(bytes)) {
                return true;
            }
            bloom.put(bytes);
        }
        return false;
    }

    /**
     * Get a sequence of bytes that uniquely identifies this document using the configured unique fields.
     *
     * @param result
     *            a document or event
     * @return A document signature
     */
    byte[] getBytes(Object result) {
        // we need to pull the fields out of the document.
        StringBuilder output = new StringBuilder();
        if (result instanceof Document) {
            outputSortedFieldValues((Document) result, output);
        } else {
            outputSortedFieldValues((EventBase) result, output);
        }
        return output.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Take the fields from the document configured for the unique transform and output them to the data output stream.
     *
     * @param document
     *            a document
     * @param output
     *            the output string builder
     */
    private void outputSortedFieldValues(Document document, StringBuilder output) {
        int count = 0;
        String lastField = "";
        List<String> values = new ArrayList<>();
        for (String documentField : new TreeSet<>(document.getDictionary().keySet())) {
            String field = getUniqueField(documentField);
            if (field != null) {
                if (!field.equals(lastField)) {
                    count = dumpValues(count, lastField, values, output);
                    lastField = field;
                }
                addValues(field, document.get(documentField), values);
            }
        }
        dumpValues(count, lastField, values, output);
    }

    /**
     * Take the fields from the document configured for the unique transform and output them as a string
     *
     * @param event
     *            an event
     * @param output
     *            the string builder
     * @return The sorted field values as a string
     */
    private void outputSortedFieldValues(EventBase event, StringBuilder output) {
        // First see if we have stored the signature in the internal id metadata
        String id = event.getMetadata().getInternalId();
        int index = id.indexOf('\u0000');
        if (index >= 0) {
            output.append(id.substring(index + 1));
            return;
        }

        int count = 0;
        String lastField = "";
        List<String> values = new ArrayList<>();
        List<FieldBase> fields = event.getFields();
        for (FieldBase fieldBase : fields) {
            String field = getUniqueField(fieldBase.getName());
            if (field != null) {
                if (!field.equals(lastField)) {
                    count = dumpValues(count, lastField, values, output);
                    lastField = field;
                }
                addValue(field, fieldBase.getValueString(), values);
            }
        }
        dumpValues(count, lastField, values, output);

        // Cache the result in the event itself for next time
        event.getMetadata().setInternalId(event.getMetadata().getInternalId() + '\u0000' + output.toString());
    }

    /**
     * Dump a list of values, sorted, to the data output stream
     *
     * @param count
     *            value count
     * @param field
     *            a field
     * @param values
     *            the list of values
     * @param output
     *            the output buffer
     * @return The next field count
     */
    private int dumpValues(int count, String field, List<String> values, StringBuilder output) {
        if (!values.isEmpty()) {
            Collections.sort(values);
            String separator = "f-" + field + '/' + (count++) + ":";
            for (String value : values) {
                output.append(separator);
                output.append(value);
                separator = ",";
            }
            values.clear();
        }
        return count;
    }

    /**
     * Add the attribute values to the list of values.
     *
     * @param field
     *            The attribute field
     * @param attribute
     *            The attribute
     * @param values
     *            The list of values to be updated
     */
    private void addValues(final String field, Attribute<?> attribute, List<String> values) {
        if (attribute instanceof Attributes) {
            // @formatter:off
            ((Attributes) attribute).getAttributes().stream()
                    .forEach(a -> addValues(field, a, values));
            // @formatter:on
        } else {
            addValue(field, String.valueOf(attribute.getData()), values);
        }
    }

    /**
     * Add the attribute values to the list of values.
     *
     * @param field
     *            The attribute field
     * @param value
     *            The value
     * @param values
     *            The list of values to be updated
     */
    private void addValue(final String field, String value, List<String> values) {
        values.add(uniqueFields.transformValue(field, value));
    }

    /**
     * Return the query-specified field that the provided document matches, if one exists, or otherwise return null.
     *
     * @param documentField
     *            The document field
     * @return The query specified field
     */
    private String getUniqueField(String documentField) {
        String baseDocumentField = getFieldWithoutGrouping(documentField);
        return uniqueFields.getFields().stream().filter((field) -> isMatchingField(baseDocumentField, field)).findFirst().orElse(null);
    }

    /**
     * Return the provided field with any grouping context removed.
     *
     * @param field
     *            The field
     * @return The field with grouping stripped
     */
    private String getFieldWithoutGrouping(String field) {
        int index = field.indexOf('.');
        if (index < 0) {
            return field;
        } else {
            return field.substring(0, index);
        }
    }

    /**
     * Return whether or not the provided document field is considered a case-insensitive match for the provided field
     *
     * @param baseField
     *            The base field
     * @param field
     *            The field to match with
     * @return true if matching
     */
    private boolean isMatchingField(String baseField, String field) {
        return field.equalsIgnoreCase(baseField);
    }

    /**
     * A funnel to use for the bloom filter
     */
    public static class ByteFunnel implements Funnel<byte[]>, Serializable {

        private static final long serialVersionUID = -2126172579955897986L;

        @Override
        public void funnel(byte[] from, PrimitiveSink into) {
            into.putBytes(from);
        }
    }

    /**
     * An iterator of documents for this unique transform given an underlying iterator of documents.
     */
    public class UniqueTransformIterator implements Iterator<Map.Entry<Key,Document>> {
        private final Iterator<Map.Entry<Key,Document>> iterator;
        private Map.Entry<Key,Document> next = null;

        public UniqueTransformIterator(Iterator<Map.Entry<Key,Document>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            if (next == null) {
                next = getNext();
            }
            return (next != null);
        }

        @Override
        public Map.Entry<Key,Document> next() {
            Map.Entry<Key,Document> o = null;
            if (next == null) {
                o = getNext();
            } else {
                o = next;
                next = null;
            }
            return o;
        }

        private Map.Entry<Key,Document> getNext() {
            Map.Entry<Key,Document> o = null;
            while (o == null && iterator.hasNext()) {
                o = apply(iterator.next());
            }
            // see if there are any results cached by the transform
            if (o == null) {
                o = flush();
            }
            return o;
        }

    }

    /**
     * A builder of unique transforms
     */
    public static class Builder {
        private UniqueFields uniqueFields;
        private Comparator<Entry<byte[],Object>> keyComparator;
        private RewritableSortedSetImpl.RewriteStrategy<Map.Entry<byte[],Object>> keyValueComparator;
        private int bufferPersistThreshold;
        private List<IvaratorCacheDirConfig> ivaratorCacheDirConfigs;
        private String hdfsSiteConfigURLs;
        private String subDirectory;
        private int maxOpenFiles;
        private int numRetries;
        private long queryExecutionForPageTimeout;
        private FileSortedSet.PersistOptions persistOptions;
        private BloomFilter<byte[]> filter;

        public Builder() {
            keyComparator = new Comparator<>() {
                private Comparator<byte[]> comparator = new ByteArrayComparator();

                @Override
                public int compare(Map.Entry<byte[],Object> o1, Map.Entry<byte[],Object> o2) {
                    return comparator.compare(o1.getKey(), o2.getKey());
                }
            };

            keyValueComparator = (original, update) -> {
                int comparison = keyComparator.compare(original, update);
                if (comparison == 0) {
                    long ts1 = getTimestamp(original.getValue());
                    long ts2 = getTimestamp(update.getValue());
                    return (ts2 > ts1);
                }
                return comparison < 0;
            };
        }

        /**
         * Build a list of potential hdfs directories based on each ivarator cache dir configs.
         *
         * @param ivaratorCacheDirConfigs
         * @param hdfsSiteConfigURLs
         * @param subdirectory
         * @return A path
         * @throws IOException
         *             for issues with read/write
         */
        private static List<IvaratorCacheDir> getIvaratorCacheDirs(List<IvaratorCacheDirConfig> ivaratorCacheDirConfigs, String hdfsSiteConfigURLs,
                        String subdirectory) throws IOException {
            // build a list of ivarator cache dirs from the configs
            List<IvaratorCacheDir> pathAndFs = new ArrayList<>();
            if (ivaratorCacheDirConfigs != null && !ivaratorCacheDirConfigs.isEmpty()) {
                for (IvaratorCacheDirConfig config : ivaratorCacheDirConfigs) {

                    // first, make sure the cache configuration is valid
                    if (config.isValid()) {
                        Path path = new Path(config.getBasePathURI(), subdirectory);
                        URI uri = path.toUri();
                        FileSystem fs = new FileSystemCache(hdfsSiteConfigURLs).getFileSystem(uri);
                        pathAndFs.add(new IvaratorCacheDir(config, fs, uri.toString()));
                    }
                }
            }

            if (pathAndFs.isEmpty())
                throw new IOException("Unable to find a usable hdfs cache dir out of " + ivaratorCacheDirConfigs);

            return pathAndFs;
        }

        public Builder withUniqueFields(UniqueFields fields) {
            this.uniqueFields = fields;
            return this;
        }

        public Builder withBufferPersistThreshold(int bufferPersistThreshold) {
            this.bufferPersistThreshold = bufferPersistThreshold;
            return this;
        }

        public Builder withIvaratorCacheDirConfigs(List<IvaratorCacheDirConfig> ivaratorCacheDirConfigs) {
            this.ivaratorCacheDirConfigs = ivaratorCacheDirConfigs;
            return this;
        }

        public Builder withHdfsSiteConfigURLs(String hdfsSiteConfigURLs) {
            this.hdfsSiteConfigURLs = hdfsSiteConfigURLs;
            return this;
        }

        public Builder withSubDirectory(String subDirectory) {
            this.subDirectory = subDirectory;
            return this;
        }

        public Builder withMaxOpenFiles(int maxOpenFiles) {
            this.maxOpenFiles = maxOpenFiles;
            return this;
        }

        public Builder withNumRetries(int numRetries) {
            this.numRetries = numRetries;
            return this;
        }

        public Builder withPersistOptions(FileSortedSet.PersistOptions persistOptions) {
            this.persistOptions = persistOptions;
            return this;
        }

        public Builder withQueryExecutionForPageTimeout(long timeout) {
            this.queryExecutionForPageTimeout = timeout;
            return this;
        }

        public Builder withFilter(BloomFilter<byte[]> filter) {
            this.filter = filter;
            return this;
        }

        public UniqueTransform build() throws IOException {
            UniqueTransform transform = new UniqueTransform(uniqueFields, queryExecutionForPageTimeout);
            if (filter != null) {
                transform.setFilter(filter);
            }

            if (transform.uniqueFields.isMostRecent()) {
                // @formatter:off
                // noinspection unchecked
                transform.set = (HdfsBackedSortedSet<Entry<byte[],Object>>) HdfsBackedSortedSet.builder()
                        .withComparator(keyComparator)
                        .withRewriteStrategy(keyValueComparator)
                        .withBufferPersistThreshold(bufferPersistThreshold)
                        .withIvaratorCacheDirs(getIvaratorCacheDirs(ivaratorCacheDirConfigs, hdfsSiteConfigURLs, subDirectory))
                        .withUniqueSubPath("byUniqueKey")
                        .withMaxOpenFiles(maxOpenFiles)
                        .withNumRetries(numRetries)
                        .withPersistOptions(persistOptions)
                        .withSetFactory(new FileByteDocumentSortedSet.Factory())
                        .build();

                transform.returnSet = (HdfsBackedSortedSet<Entry<Key,Document>>) HdfsBackedSortedSet.builder()
                        .withBufferPersistThreshold(bufferPersistThreshold)
                        .withIvaratorCacheDirs(getIvaratorCacheDirs(ivaratorCacheDirConfigs, hdfsSiteConfigURLs, subDirectory))
                        .withUniqueSubPath("byDocKey")
                        .withMaxOpenFiles(maxOpenFiles)
                        .withNumRetries(numRetries)
                        .withPersistOptions(persistOptions)
                        .withSetFactory(new FileKeyValueSortedSet.Factory())
                        .build();
                // @formatter:on
            }

            return transform;
        }
    }

    private static long getTimestamp(Object o) {
        if (o instanceof Document) {
            return getDocKeyAttr((Document) o).getTimestamp();
        } else {
            return getTimestamp((EventBase) o);
        }
    }

    private static DocumentKey getDocKeyAttr(Document doc) {
        return (DocumentKey) (doc.get(Document.DOCKEY_FIELD_NAME));
    }

    private static Key getDocKey(Document doc) {
        return getDocKeyAttr(doc).getDocKey();
    }

    private static long getTimestamp(EventBase e) {
        List<FieldBase> fields = e.getFields();
        if (fields.isEmpty()) {
            return -1;
        } else {
            return fields.get(0).getTimestamp();
        }
    }

}
