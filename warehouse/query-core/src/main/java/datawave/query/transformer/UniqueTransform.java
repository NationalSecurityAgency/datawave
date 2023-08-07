package datawave.query.transformer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import javax.annotation.Nullable;

import org.apache.accumulo.core.data.Key;
import org.apache.commons.collections4.keyvalue.UnmodifiableMapEntry;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
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
import datawave.query.iterator.QueryIterator;
import datawave.query.iterator.ivarator.IvaratorCacheDir;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.iterator.profile.FinalDocumentTrackingIterator;
import datawave.query.model.QueryModel;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.sortedset.ByteArrayComparator;
import datawave.query.util.sortedset.FileByteDocumentSortedSet;
import datawave.query.util.sortedset.FileSortedSet;
import datawave.query.util.sortedset.HdfsBackedSortedSet;
import datawave.query.util.sortedset.RewritableSortedSet;
import datawave.query.util.sortedset.RewritableSortedSetImpl;

/**
 * This iterator will filter documents based on uniqueness across a set of configured fields. Only the first instance of an event with a unique set of those
 * fields will be returned unless mostRecentUnique is specified in which case the most recent instance of an event will be returned. This transform is thread
 * safe.
 *
 * 1) FileByteKeySortedSet: ability to keep the most recent key for the same byte array done: setRewriteStrategy(rewriteStrategy) 2) MultiSetBackedSortedSet:
 * ability to keep the mote recent key for the same value when doing a merge sort done 3) Buffer all values in the file backed sorted set until we flush done 4)
 * Update the ShardQueryLogic to use the MostRecentUniqueTransform done 5) Create a MostRecentUniqueIterator akin to the GroupingIterator done 6) Update the
 * QueryIterator to apply the MostRecentUniqueIterator done 7) Create most recent test cases
 *
 */
public class UniqueTransform extends DocumentTransform.DefaultDocumentTransform {

    private static final Logger log = Logger.getLogger(UniqueTransform.class);

    private BloomFilter<byte[]> bloom;
    private final UniqueFields uniqueFields = new UniqueFields();
    private Multimap<String,String> modelMapping;
    private HdfsBackedSortedSet<Entry<byte[],Document>> set;
    private Iterator<Entry<byte[],Document>> setIterator;

    private UniqueTransform(UniqueFields uniqueFields) {
        this.uniqueFields.set(uniqueFields);
        this.uniqueFields.deconstructIdentifierFields();
        this.bloom = BloomFilter.create(new ByteFunnel(), 500000, 1e-15);
        if (log.isTraceEnabled()) {
            log.trace("unique fields: " + this.uniqueFields.getFields());
        }
    }

    public void updateConfig(UniqueFields uniqueFields, QueryModel model) {
        UniqueFields fields = new UniqueFields();
        fields.set(uniqueFields);
        fields.deconstructIdentifierFields();
        if (!this.uniqueFields.equals(fields)) {
            this.uniqueFields.set(fields);
            log.info("Resetting unique fields on the unique transform");
            this.bloom = BloomFilter.create(new ByteFunnel(), 500000, 1e-15);
            if (log.isTraceEnabled()) {
                log.trace("unique fields: " + this.uniqueFields.getFields());
            }
        }
        setModelMappings(model);
    }

    private void setModelMappings(QueryModel model) {
        if (model != null) {
            modelMapping = HashMultimap.create();
            // reverse the reverse query mapping which will give us a mapping from the final field name to the original field name(s)
            for (Map.Entry<String,String> entry : model.getReverseQueryMapping().entrySet()) {
                modelMapping.put(entry.getValue(), entry.getKey());
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
                    this.set.add(new UnmodifiableMapEntry(signature, keyDocumentEntry.getValue()));
                    keyDocumentEntry = null;
                } else if (isDuplicate(keyDocumentEntry.getValue())) {
                    keyDocumentEntry = null;
                }
            } catch (IOException ioe) {
                log.error("Failed to convert document to bytes.  Returning document as unique.", ioe);
            }
        }
        return keyDocumentEntry;
    }

    @Override
    public Map.Entry<Key,Document> flush() {
        if (set != null) {
            if (setIterator == null) {
                setIterator = set.iterator();
            }
            if (setIterator.hasNext()) {
                Map.Entry<byte[],Document> next = setIterator.next();
                Document document = next.getValue();
                return new UnmodifiableMapEntry(getDocKey(document), document);
            }
        }
        return null;
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
        output.flush();
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
     *            the output stream
     * @return The next field count
     * @throws IOException
     *             for issues with read/write
     */
    private int dumpValues(int count, String field, List<String> values, DataOutputStream output) throws IOException {
        if (!values.isEmpty()) {
            Collections.sort(values);
            String separator = "f-" + field + '/' + (count++) + ":";
            for (String value : values) {
                output.writeUTF(separator);
                output.writeUTF(value);
                separator = ",";
            }
            values.clear();
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
            values.add(uniqueFields.transformValue(field, String.valueOf(attribute.getData())));
        }
    }

    // Return the query-specified field that the provided document matches, if one exists, or otherwise return null.
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

        private final UniqueTransform transform;

        public Builder(UniqueFields fields) {
            transform = new UniqueTransform(fields);
        }

        private Comparator<Entry<byte[],Document>> keyComparator = new Comparator<>() {
            private Comparator<byte[]> comparator = new ByteArrayComparator();

            @Override
            public int compare(Map.Entry<byte[],Document> o1, Map.Entry<byte[],Document> o2) {
                return comparator.compare(o1.getKey(), o2.getKey());
            }
        };

        private RewritableSortedSetImpl.RewriteStrategy<Map.Entry<byte[],Document>> keyValueComparator = new RewritableSortedSetImpl.RewriteStrategy<>() {
            @Override
            public boolean rewrite(Map.Entry<byte[],Document> original, Map.Entry<byte[],Document> update) {
                int comparison = keyComparator.compare(original, update);
                if (comparison == 0) {
                    long ts1 = getTimestamp(original.getValue());
                    long ts2 = getTimestamp(update.getValue());
                    return (ts2 > ts1);
                }
                return comparison < 0;
            }
        };

        /**
         * Setup an hdfs backed sorted set if "most recent".
         *
         * @param queryIterator
         *            the queryIterator from which to get hdfs backed sorted set info
         * @throws IOException
         *             when we fail to create the ivarator cache dirs
         **/
        public Builder withQueryIterator(QueryIterator queryIterator) throws IOException {
            if (transform.uniqueFields.isMostRecent()) {
                transform.set = new HdfsBackedSortedSet.Builder().withComparator(keyComparator).withRewriteStrategy(keyValueComparator)
                                .withBufferPersistThreshold(queryIterator.getUniqueCacheBufferSize()).withIvaratorCacheDirs(getIvaratorCacheDirs(queryIterator))
                                .withUniqueSubPath("MostRecentUniqueSet").withMaxOpenFiles(queryIterator.getIvaratorMaxOpenFiles())
                                .withNumRetries(queryIterator.getIvaratorNumRetries()).withPersistOptions(queryIterator.getIvaratorPersistOptions())
                                .withSetFactory(new FileByteDocumentSortedSet.Factory()).build();
            }
            return this;
        }

        /**
         * Capture the reverse field mapping defined within the model being used by the logic (if present). Also setup an hdfs backed sorted set if "most
         * recent".
         *
         * @param logic
         *            the locic from which to get model mappings and hdfs backed sorted set info
         * @throws IOException
         *             when we fail to create the ivarator cache dirs
         **/
        public Builder withLogic(ShardQueryLogic logic) throws IOException {
            transform.setModelMappings(logic.getQueryModel());
            if (transform.uniqueFields.isMostRecent()) {
                transform.set = new HdfsBackedSortedSet.Builder().withComparator(keyComparator).withRewriteStrategy(keyValueComparator)
                                .withBufferPersistThreshold(logic.getUniqueCacheBufferSize()).withIvaratorCacheDirs(getIvaratorCacheDirs(logic))
                                .withUniqueSubPath("FinalMostRecentUniqueSet").withMaxOpenFiles(logic.getIvaratorMaxOpenFiles())
                                .withNumRetries(logic.getIvaratorNumRetries())
                                .withPersistOptions(new FileSortedSet.PersistOptions(logic.isIvaratorPersistVerify(), logic.isIvaratorPersistVerify(),
                                                logic.getIvaratorPersistVerifyCount()))
                                .withSetFactory(new FileByteDocumentSortedSet.Factory()).build();
            }
            return this;
        }

        public UniqueTransform build() {
            return transform;
        }

        private static List<IvaratorCacheDir> getIvaratorCacheDirs(ShardQueryLogic logic) throws IOException {
            return getIvaratorCacheDirs(logic.getIvaratorCacheDirConfigs(), logic.getHdfsSiteConfigURLs(), logic.getConfig().getQuery().getId().toString());
        }

        private static List<IvaratorCacheDir> getIvaratorCacheDirs(QueryIterator queryIterator) throws IOException {
            return getIvaratorCacheDirs(queryIterator.getIvaratorCacheDirConfigs(), queryIterator.getHdfsSiteConfigURLs(),
                            queryIterator.getQueryId() + '-' + queryIterator.getScanId());
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
