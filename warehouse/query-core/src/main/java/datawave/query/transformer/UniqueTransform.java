package datawave.query.transformer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
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

import datawave.core.query.logic.BaseQueryLogic;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.attributes.UniqueFields;
import datawave.query.iterator.profile.FinalDocumentTrackingIterator;
import datawave.query.model.QueryModel;
import datawave.query.tables.ShardQueryLogic;

/**
 * This iterator will filter documents based on uniqueness across a set of configured fields. Only the first instance of an event with a unique set of those
 * fields will be returned. This transform is thread safe.
 */
public class UniqueTransform extends DocumentTransform.DefaultDocumentTransform {

    private static final Logger log = Logger.getLogger(UniqueTransform.class);

    private BloomFilter<byte[]> bloom;
    private UniqueFields uniqueFields;
    private Multimap<String,String> modelMapping;

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
     * Create a new {@link UniqueTransform} that will use a bloom filter to return on those results that are unique per the uniqueFields. Special uniqueness can
     * be requested for date/time fields (@see UniqueFields). The logic will be used to get a query model to include the reverse mappings in the unique field
     * set
     *
     * @param logic
     *            The query logic from whih to pull the query model
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
}
