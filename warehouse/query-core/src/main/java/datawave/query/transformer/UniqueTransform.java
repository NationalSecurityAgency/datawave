package datawave.query.transformer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import javax.annotation.Nullable;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

import datawave.core.iterators.filesystem.FileSystemCache;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.attributes.DocumentKey;
import datawave.query.attributes.UniqueFields;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.iterator.ivarator.IvaratorCacheDir;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.iterator.profile.FinalDocumentTrackingIterator;
import datawave.query.util.sortedmap.FileByteDocumentSortedMap;
import datawave.query.util.sortedmap.FileKeyDocumentSortedMap;
import datawave.query.util.sortedmap.FileSortedMap;
import datawave.query.util.sortedmap.HdfsBackedSortedMap;
import datawave.query.util.sortedset.ByteArrayComparator;
import datawave.query.util.sortedset.FileSortedSet;

/**
 * This iterator will filter documents based on uniqueness across a set of configured fields. Only the first instance of an event with a unique set of those
 * fields will be returned unless mostRecentUnique is specified in which case the most recent instance of an event will be returned. This transform is thread
 * safe.
 */
public class UniqueTransform extends DocumentTransform.DefaultDocumentTransform {

    private static final Logger log = Logger.getLogger(UniqueTransform.class);

    private BloomFilter<byte[]> bloom;
    private UniqueFields uniqueFields;
    private HdfsBackedSortedMap<byte[],Document> documentMap;
    private HdfsBackedSortedMap<Key,Document> returnSet;
    private HdfsBackedSortedMap<byte[],Integer> countMap;
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
        if (log.isTraceEnabled()) {
            log.trace("unique fields: " + this.uniqueFields.getFields());
        }
    }

    /**
     * Update the configuration of this transform. If the configuration is actually changing, then the bloom filter will be reset as well.
     *
     * @param uniqueFields
     *            The new set of unique fields.
     */
    public void updateConfig(UniqueFields uniqueFields) {
        // only reset the bloom filter if changing the field set
        if (!this.uniqueFields.equals(uniqueFields)) {
            this.uniqueFields = uniqueFields.clone();
            log.info("Resetting unique fields on the unique transform");
            if (documentMap != null) {
                documentMap.clear();
                returnSet.clear();
            } else {
                bloom = BloomFilter.create(new ByteFunnel(), 500000, 1e-15);
            }
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

            if (keyDocumentEntry.getValue().isIntermediateResult()) {
                return keyDocumentEntry;
            }

            try {
                // If the document map is not null, we are tracking the most recent unique and/or the max count of each unique result.
                if (documentMap != null) {
                    byte[] signature = getBytes(keyDocumentEntry.getValue());
                    // If the count map is not null, check if we have exceeded the max count.
                    if (countMap != null) {
                        synchronized (countMap) {
                            // Increment the count by one in the count map.
                            Integer count = countMap.getOrDefault(signature, 0);
                            count++;
                            countMap.put(signature, count);
                            synchronized (documentMap) {
                                // If the count exceeds the max count, ensure the document is removed from the document map.
                                if (count > uniqueFields.getMaxCount()) {
                                    documentMap.remove(signature);
                                } else {
                                    // Otherwise, update the document map.
                                    documentMap.put(signature, keyDocumentEntry.getValue());
                                }
                            }
                        }
                    } else {
                        // If the count map is null, we are only fetching the most recent result, and are not concerned about the max count.
                        synchronized (documentMap) {
                            this.documentMap.put(signature, keyDocumentEntry.getValue());
                        }
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
     * This will start pulling data from the hdfs backed set if one exists (only if mostRecent is true).
     *
     * @return The next unique document from the set.
     */
    @Override
    public Map.Entry<Key,Document> flush() {
        if (documentMap != null) {
            synchronized (documentMap) {
                // persist the map so that we do not loose these results and we compact the files for the final iteration.
                try {
                    documentMap.persist();
                } catch (IOException ioe) {
                    throw new DatawaveFatalQueryException("Unable to persist the most recent unique maps", ioe);
                }
                if (setIterator == null) {
                    setupIterator();
                }
                if (setIterator.hasNext()) {
                    return setIterator.next();
                }
            }
        }
        return null;
    }

    /**
     * This will run through the set and create a new set ordered by Key, Document
     */
    private void setupIterator() {
        for (Map.Entry<byte[],Document> entry : documentMap.entrySet()) {
            returnSet.put(getDocKey(entry.getValue()), entry.getValue());
        }
        // now persist the return set so that we don't lose the results and compact the sets
        try {
            returnSet.persist();
        } catch (IOException ioe) {
            throw new DatawaveFatalQueryException("Could not persist unique document return set", ioe);
        }
        setIterator = returnSet.entrySet().iterator();
    }

    /**
     * Determine if a document is unique per the fields specified. If we have seen this set of fields and values before, then it is not unique.
     *
     * @param document
     *            a document
     * @return if a document is unique per the fields specified
     * @throws IOException
     *             for issues with read/write
     */
    private boolean isDuplicate(Document document) throws IOException {
        byte[] bytes = getBytes(document);
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
     * @param document
     *            a document
     * @return A document signature
     * @throws IOException
     *             if we failed to generate the byte array
     */
    byte[] getBytes(Document document) throws IOException {
        // we need to pull the fields out of the document.
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(bytes);
        outputSortedFieldValues(document, output);
        return bytes.toByteArray();
    }

    /**
     * Take the fields from the document configured for the unique transform and output them to the data output stream.
     *
     * @param document
     *            a document
     * @param output
     *            the output stream
     * @throws IOException
     *             if we failed to generate the byte array
     */
    private void outputSortedFieldValues(Document document, DataOutputStream output) throws IOException {
        Multimap<String,String> values = HashMultimap.create();
        for (String documentField : new TreeSet<>(document.getDictionary().keySet())) {
            String field = getUniqueField(documentField);
            if (field != null) {
                addValues(field, document.get(documentField), values);
            }
        }
        // Always dump the fields in the same order (uniqueFields.getFields is a sorted collection)
        for (String field : uniqueFields.getFields()) {
            dumpValues(field, values.get(field), output);
        }
        output.flush();
    }

    /**
     * Dump a list of values, sorted, to the data output stream
     *
     * @param field
     *            a field
     * @param values
     *            the list of values
     * @param output
     *            the output stream
     * @throws IOException
     *             for issues with read/write
     */
    private void dumpValues(String field, Collection<String> values, DataOutputStream output) throws IOException {
        String separator = "f-" + field + ":";
        if (!values.isEmpty()) {
            List<String> valueList = new ArrayList<>(values);
            // always output values in sorted order.
            Collections.sort(valueList);
            for (String value : valueList) {
                output.writeUTF(separator);
                output.writeUTF(value);
                separator = ",";
            }
        } else {
            // dump at least a header for empty value sets to ensure we have some bytes to check against
            // in the bloom filter.
            output.writeUTF(separator);
        }
    }

    /**
     * Add the attribute values to the list of values.
     *
     * @param field
     *            The attribute field
     * @param attribute
     *            The attribute
     * @param values
     *            The map of values to be updated
     */
    private void addValues(final String field, Attribute<?> attribute, Multimap<String,String> values) {
        if (attribute instanceof Attributes) {
            // @formatter:off
            ((Attributes) attribute).getAttributes()
                    .forEach(a -> addValues(field, a, values));
            // @formatter:on
        } else {
            values.put(field, uniqueFields.transformValue(field, String.valueOf(attribute.getData())));
        }
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
        return baseField.equalsIgnoreCase(field);
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
        private final Comparator<byte[]> keyComparator;
        private final FileSortedMap.RewriteStrategy<byte[],Document> rewriteNewerTimestamp;
        private final FileSortedMap.RewriteStrategy<byte[],Document> neverRewrite;
        private int bufferPersistThreshold;
        private List<IvaratorCacheDirConfig> ivaratorCacheDirConfigs;
        private String hdfsSiteConfigURLs;
        private String subDirectory;
        private int maxOpenFiles;
        private int numRetries;
        private long queryExecutionForPageTimeout;
        private FileSortedSet.PersistOptions persistOptions;

        public Builder() {
            keyComparator = new ByteArrayComparator();

            rewriteNewerTimestamp = (key, original, update) -> {
                long ts1 = getTimestamp(original);
                long ts2 = getTimestamp(update);
                return (ts2 > ts1);
            };

            neverRewrite = (key, original, update) -> false;
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

            if (pathAndFs.isEmpty()) {
                throw new IOException("Unable to find a usable hdfs cache dir out of " + ivaratorCacheDirConfigs);
            }

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

        public UniqueTransform build() throws IOException {
            UniqueTransform transform = new UniqueTransform(uniqueFields, queryExecutionForPageTimeout);

            // If we either need to fetch the most recent unique result, or we have a max unique count, we will require the hdfs backed sets.
            if (transform.uniqueFields.isMostRecent() || transform.uniqueFields.getMaxCount() > 0) {
                // @formatter:off
                // noinspection unchecked
                transform.returnSet = (HdfsBackedSortedMap<Key,Document>) HdfsBackedSortedMap.builder()
                                .withBufferPersistThreshold(bufferPersistThreshold)
                                .withIvaratorCacheDirs(getIvaratorCacheDirs(ivaratorCacheDirConfigs, hdfsSiteConfigURLs, subDirectory))
                                .withUniqueSubPath("byDocKey")
                                .withMaxOpenFiles(maxOpenFiles)
                                .withNumRetries(numRetries)
                                .withPersistOptions(persistOptions)
                                .withMapFactory(new FileKeyDocumentSortedMap.Factory())
                                .build();
                // @formatter:on

                // If isMostRecent is true, the document map should be updated whenever we have a value with a more recent timestamp. Otherwise, we only want to
                // retain the first value (when we have a max unique count without isMostRecent).
                FileSortedMap.RewriteStrategy<byte[],Document> rewriteStrategy = transform.uniqueFields.isMostRecent() ? rewriteNewerTimestamp : neverRewrite;

                // @formatter:off
                // noinspection unchecked
                transform.documentMap = (HdfsBackedSortedMap<byte[],Document>) HdfsBackedSortedMap.builder()
                                .withComparator(keyComparator)
                                .withRewriteStrategy(rewriteStrategy)
                                .withBufferPersistThreshold(bufferPersistThreshold)
                                .withIvaratorCacheDirs(getIvaratorCacheDirs(ivaratorCacheDirConfigs, hdfsSiteConfigURLs, subDirectory))
                                .withUniqueSubPath("byUniqueKey")
                                .withMaxOpenFiles(maxOpenFiles)
                                .withNumRetries(numRetries)
                                .withPersistOptions(persistOptions)
                                .withMapFactory(new FileByteDocumentSortedMap.Factory())
                                .build();
                // @formatter:on

                if (transform.uniqueFields.getMaxCount() > 0) {
                    // @formatter:off
                    //noinspection unchecked
                    transform.countMap = (HdfsBackedSortedMap<byte[],Integer>) HdfsBackedSortedMap.builder()
                                    .withComparator(keyComparator)
                                    .withBufferPersistThreshold(bufferPersistThreshold)
                                    .withIvaratorCacheDirs(getIvaratorCacheDirs(ivaratorCacheDirConfigs, hdfsSiteConfigURLs, subDirectory))
                                    .withUniqueSubPath("byMaxUniqueKey")
                                    .withMaxOpenFiles(maxOpenFiles)
                                    .withNumRetries(numRetries)
                                    .withPersistOptions(persistOptions)
                                    .withMapFactory(new FileByteDocumentSortedMap.Factory()).build();
                    // @formatter:on
                }
            } else {
                transform.bloom = BloomFilter.create(new ByteFunnel(), 500000, 1e-15);
            }

            return transform;
        }
    }

    private static long getTimestamp(Document doc) {
        return getDocKeyAttr(doc).getTimestamp();
    }

    private static DocumentKey getDocKeyAttr(Document doc) {
        return (DocumentKey) (doc.get(Document.DOCKEY_FIELD_NAME));
    }

    private static Key getDocKey(Document doc) {
        return getDocKeyAttr(doc).getDocKey();
    }

}
