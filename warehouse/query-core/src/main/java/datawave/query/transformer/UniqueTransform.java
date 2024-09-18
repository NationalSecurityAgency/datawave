package datawave.query.transformer;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Logger;

import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

import datawave.core.iterators.filesystem.FileSystemCache;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.ResultPostprocessor;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.attributes.UniqueFields;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.iterator.ivarator.IvaratorCacheDir;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.iterator.profile.FinalDocumentTrackingIterator;
import datawave.query.model.QueryModel;
import datawave.query.tables.ShardQueryLogic;
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
 * fields will be returned. This transform is thread safe.
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
        this.uniqueFields.deconstructIdentifierFields();
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
     *            The unique fields
     * @param queryExecutionForPageTimeout
     *            If this timeout is passed before since the last result was returned, then an "intermediate" result is returned denoting we are still looking
     *            for the next unique result.
     */
    public UniqueTransform(BaseQueryLogic<Entry<Key,Value>> logic, UniqueFields uniqueFields, long queryExecutionForPageTimeout) {
        this(uniqueFields, queryExecutionForPageTimeout);
        QueryModel model = ((ShardQueryLogic) logic).getQueryModel();
        if (model != null) {
            modelMapping = HashMultimap.create();
            // reverse the reverse query mapping which will give us a mapping from the final field name to the original field name(s)
            for (Map.Entry<String,String> entry : model.getReverseQueryMapping().entrySet()) {
                modelMapping.put(entry.getValue(), entry.getKey());
            }
        }
    }

    public void updateConfig(UniqueFields uniqueFields, QueryModel model) {
        if (this.uniqueFields != uniqueFields) {
            uniqueFields.deconstructIdentifierFields();
            if (!this.uniqueFields.equals(uniqueFields)) {
                this.uniqueFields = uniqueFields;
                log.info("Resetting unique fields on the unique transform");
                this.bloom = BloomFilter.create(new ByteFunnel(), 500000, 1e-15);
                if (log.isTraceEnabled()) {
                    log.trace("unique fields: " + this.uniqueFields.getFields());
                }
            }
        }
        if (model != null) {
            modelMapping = HashMultimap.create();
            // reverse the reverse query mapping which will give us a mapping from the final field name to the original field name(s)
            for (Map.Entry<String,String> entry : model.getReverseQueryMapping().entrySet()) {
                modelMapping.put(entry.getValue(), entry.getKey());
            }
        }
    }

    /**
     * Get a predicate that will apply this transform.
     *
     * @return A unique transform predicate
     */
    public Predicate<Entry<Key,Document>> getUniquePredicate() {
        return input -> UniqueTransform.this.apply(input) != null;
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
                if (isDuplicate(keyDocumentEntry.getValue())) {
                    keyDocumentEntry = null;
                } else {
                    return keyDocumentEntry;
                }
            } catch (IOException ioe) {
                log.error("Failed to convert document to bytes.  Returning document as unique.", ioe);
            }
        }

        long elapsedExecutionTimeForCurrentPage = System.currentTimeMillis() - this.queryExecutionForPageStartTime;
        if (elapsedExecutionTimeForCurrentPage > this.queryExecutionForPageTimeout) {
            Document intermediateResult = new Document();
            intermediateResult.setIntermediateResult(true);
            return Maps.immutableEntry(new Key(), intermediateResult);
        }

        return null;
    }

    /**
     * Part of the ResultPostprocessor interface
     */
    @Override
    public void apply(List<Object> results, Object newResult) {
        // these results should be EventBase objects
        EventBase event = (EventBase) newResult;
        if (!event.isIntermediateResult()) {
            log.info("Testing " + event.getMetadata() + " for uniqueness");
            try {
                if (set != null) {
                    log.info("Adding " + event.getMetadata() + " to most recent unique set");
                    byte[] signature = getBytes(event);
                    synchronized (set) {
                        this.set.add(new UnmodifiableMapEntry(signature, event));
                    }
                } else if (!isDuplicate(event)) {
                    log.info("Keeping unique " + event.getMetadata());
                    results.add(newResult);
                }
            } catch (IOException ioe) {
                log.error("Failed to convert document to bytes.  Returning document as unique.", ioe);
            }
        }
    }

    @Override
    public Iterator<Object> flushResults(GenericQueryConfiguration config) {
        if (set != null) {
            return set.stream().map(e -> e.getValue()).iterator();
        } else {
            return Collections.emptyIterator();
        }
    }

    @Override
    public void saveState(GenericQueryConfiguration config) {
        ShardQueryConfiguration sConfig = (ShardQueryConfiguration) config;
        sConfig.setBloom(bloom);
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
        Multimap<String,String> values = HashMultimap.create();
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
        // Always dump the fields in the same order (uniqueFields.getFields is a sorted collection)
        for (String field : uniqueFields.getFields()) {
            dumpValues(field, values.get(field), output);
        }
    }

    /**
     * Take the fields from the document configured for the unique transform and output them to the data output stream.
     *
     * @param document
     *            a document
     * @param output
     *            the output string builder
     */
    private void outputSortedFieldValues(EventBase document, StringBuilder output) {
        Multimap<String,String> values = HashMultimap.create();
        for (Object fieldBase : document.getFields()) {
            String field = getUniqueField(((FieldBase) fieldBase).getName());
            if (field != null) {
                addValue(field, ((FieldBase) fieldBase).getValueString(), values);
            }
        }
        // Always dump the fields in the same order (uniqueFields.getFields is a sorted collection)
        for (String field : uniqueFields.getFields()) {
            dumpValues(field, values.get(field), output);
        }
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
    private void dumpValues(String field, Collection<String> values, StringBuilder output) {
        String separator = "f-" + field + ":";
        if (!values.isEmpty()) {
            List<String> valueList = new ArrayList<>(values);
            // always output values in sorted order.
            Collections.sort(valueList);
            for (String value : valueList) {
                output.append(separator);
                output.append(value);
                separator = ",";
            }
        } else {
            // dump at least a header for empty value sets to ensure we have some bytes to check against
            // in the bloom filter.
            output.append(separator);
        }
        return count;
    }

    // Return the set of values for the provided attribute.
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

    public void addValue(final String field, final String value, Multimap<String,String> values) {
        values.put(field, uniqueFields.transformValue(field, value));
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

    // Return the provided field with any grouping context removed.
    private String getFieldWithoutGrouping(String field) {
        int index = field.indexOf('.');
        if (index < 0) {
            return field;
        } else {
            return field.substring(0, index);
        }
    }

    // Return whether or not the provided document field is considered a case-insensitive match for the provided field, applying reverse model mappings if
    // configured.
    private boolean isMatchingField(String baseField, String field) {
        baseField = baseField.toUpperCase();
        field = field.toUpperCase();
        return field.equals(baseField) || (modelMapping != null && modelMapping.get(field).contains(baseField));
    }

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
                log.info("Creating most recent unique transform");
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
            } else {
                log.info("Creating unique transform");
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
        Attribute a = doc.get(Document.DOCKEY_FIELD_NAME);
        if (a instanceof Attributes) {
            StringBuilder s = new StringBuilder();
            for (Attribute a2 : ((Attributes) a).getAttributes()) {
                if (s.length() >= 0) {
                    s.append('\n');
                }
                s.append(a2.getMetadata());
                log.info("Found multiple dockey attributes: \n" + s);
                a = a2;
            }
        }
        return (DocumentKey) a;
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
