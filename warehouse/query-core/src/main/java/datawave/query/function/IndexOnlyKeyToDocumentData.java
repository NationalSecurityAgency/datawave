package datawave.query.function;

import static com.google.common.base.Preconditions.checkNotNull;
import static datawave.query.Constants.EMPTY_VALUE;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import datawave.query.Constants;
import datawave.query.attributes.Document;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.iterator.aggregation.DocumentData;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

/**
 * Fetches index-only tf key/values and outputs them as "standard" field key/value pairs
 */
public class IndexOnlyKeyToDocumentData extends KeyToDocumentData implements Iterator<Entry<DocumentData,Document>> {
    private static final Collection<ByteSequence> COLUMN_FAMILIES = Lists.newArrayList(new ArrayByteSequence("d"));

    private static final Logger LOG = Logger.getLogger(IndexOnlyKeyToDocumentData.class);

    private static final Entry<Key,Value> INVALID_COLUMNQUALIFIER_FORMAT_KEY = Maps.immutableEntry(new Key("INVALID_COLUMNQUALIFIER_FORMAT_KEY"), EMPTY_VALUE);

    private static final Entry<Key,Value> ITERATOR_COMPLETE_KEY = Maps.immutableEntry(new Key("ITERATOR_COMPLETE_KEY"), EMPTY_VALUE);

    private static final Entry<Key,Value> NULL_COLUMNFAMILY_KEY = Maps.immutableEntry(new Key("NULL_COLUMNFAMILY_KEY"), EMPTY_VALUE);

    private static final Entry<Key,Value> NULL_COLUMNQUALIFIER_KEY = Maps.immutableEntry(new Key("NULL_COLUMNQUALIFIER_KEY"), EMPTY_VALUE);

    private static final Entry<Key,Value> TOP_RECORD_KEY = Maps.immutableEntry(new Key("TOP_RECORD_KEY"), EMPTY_VALUE);

    private static final Entry<Key,Value> UNINITIALIZED_KEY = Maps.immutableEntry(new Key("UNINITIALIZED_KEY"), EMPTY_VALUE);

    private static final Entry<Key,Value> WRONG_COLUMNFAMILY_KEY = Maps.immutableEntry(new Key("WRONG_COLUMNFAMILY_KEY"), EMPTY_VALUE);

    private static final Entry<Key,Value> WRONG_DOCUMENT_KEY = Maps.immutableEntry(new Key("WRONG_DOCUMENT_KEY"), EMPTY_VALUE);

    private static final Entry<Key,Value> WRONG_FIELD_KEY = Maps.immutableEntry(new Key("WRONG_FIELD_KEY"), EMPTY_VALUE);

    private final String delimiter;

    private final Equality equality;

    private final String fieldName;

    private final boolean seekOnApply;

    private KeyConversionAttributes iteratorConversionAttributes;

    private Document iteratorDocument;

    private Key iteratorDocumentKey;

    private Key iteratorStartKey;

    private Entry<Key,Value> nextSeek = UNINITIALIZED_KEY;

    private final Range parent;

    private final SortedKeyValueIterator<Key,Value> source;

    /**
     * Constructor
     *
     * @param parentRange
     *            The range of the originally desired parent document
     * @param fieldName
     *            The name of the relevant field
     * @param source
     *            Iterator that performs the yeoman's work of metadata retrieval, normalization, etc.
     */
    public IndexOnlyKeyToDocumentData(final Range parentRange, final String fieldName, final SortedKeyValueIterator<Key,Value> source) {
        this(parentRange, fieldName, source, true);
    }

    /**
     * Constructor
     *
     * @param parentRange
     *            The range of the originally desired parent document
     * @param fieldName
     *            The name of the relevant field
     * @param source
     *            Iterator that performs the yeoman's work of metadata retrieval, normalization, etc.
     * @param fullSeekOnApply
     *            If true, seek all applicable records when apply is called. Otherwise, initialize the instance for incremental seeks via the Iterator methods.
     */
    public IndexOnlyKeyToDocumentData(final Range parentRange, final String fieldName, final SortedKeyValueIterator<Key,Value> source,
                    boolean fullSeekOnApply) {
        this(parentRange, fieldName, source, new PrefixEquality(PartialKey.ROW_COLFAM), null, fullSeekOnApply);
    }

    /*
     * Test constructor
     *
     * @param range The range of the originally desired parent document
     *
     * @param fieldName The name of the relevant field
     *
     * @param source Iterator that performs the yeoman's work of metadata retrieval, normalization, etc.
     *
     * @param equality the type of equality to match scanned records
     *
     * @param delimiter delimits pertinent segments of column family and column qualifier strings
     *
     * @param fullSeekOnApply If true, seek all applicable records when apply is called. Otherwise, initialize the instance for incremental seeks via the
     * Iterator methods.
     */
    protected IndexOnlyKeyToDocumentData(final Range range, final String fieldName, final SortedKeyValueIterator<Key,Value> source, final Equality equality,
                    final String delimiter, boolean fullSeekOnApply) {
        super(source, equality, false, false);
        checkNotNull(range, this.getClass().getSimpleName() + " cannot be initialized with a null parent Range");
        checkNotNull(fieldName, this.getClass().getSimpleName() + " cannot be initialized with a null, index-only field name");
        checkNotNull(source, this.getClass().getSimpleName() + " cannot be initialized with a null source SortedKeyValueIterator");
        checkNotNull(equality, this.getClass().getSimpleName() + " cannot be initialized with a null Equality");
        this.parent = range;
        this.fieldName = fieldName;
        this.source = source;
        this.equality = equality;
        if (null != delimiter) {
            this.delimiter = delimiter;
        } else {
            this.delimiter = Constants.NULL_BYTE_STRING;
        }
        this.seekOnApply = fullSeekOnApply;
    }

    @Override
    public Entry<DocumentData,Document> apply(final Entry<Key,Document> from) {
        try {
            // Validate the starting entry
            if (null == from) {
                throw new IllegalArgumentException("Starting key cannot be null");
            }

            // get the document key
            Key docKey = getDocKey(from.getKey());

            // Ensure that we have a non-empty column qualifier
            final Key stopKey = new Key(from.getKey().getRow().toString(), from.getKey().getColumnFamily().toString(),
                            from.getKey().getColumnQualifier().toString() + '\u0000' + '\uffff');

            // Create the primary range
            final Range keyRange = new Range(from.getKey(), true, stopKey, true);
            try {
                // Trigger the initial seek
                this.source.seek(keyRange, COLUMN_FAMILIES, false);

                // Assign the start key
                this.iteratorStartKey = from.getKey();

                // Assign the conversion attributes
                this.iteratorConversionAttributes = this.newKeyConversionAttributes();
            } catch (IOException e) {
                QueryException qe = new QueryException(DatawaveErrorCode.PRIMARY_RANGE_CREATE_ERROR, e);
                throw new DatawaveFatalQueryException(qe);
            }

            // Create a result key
            final Key resultKey = this.newResultKey(from);

            // Set the default iterator document key using the result key
            this.iteratorDocumentKey = resultKey;

            // Iterate through the range of tf entries, converting relevant ones into standard field entries
            final List<Entry<Key,Value>> attrs;
            if (this.seekOnApply) {
                attrs = this.newFieldEntriesFromTfEntries(from.getKey());
            }
            // Otherwise, expect that seeks will be performed incrementally via the Iterator
            // hasNext() and next() implementations.
            else {
                attrs = Collections.emptyList();
            }

            // Try to construct a new document key based on the first converted tf record
            if (!attrs.isEmpty()) {
                final Entry<Key,Value> firstEntry = attrs.get(0);
                final Key fieldKey = firstEntry.getKey();
                long timestamp = resultKey.getTimestamp();
                this.iteratorDocumentKey = this.newDocumentKey(fieldKey, timestamp);
            } else {
                this.iteratorDocument = from.getValue();
            }

            // Set the parent document
            this.iteratorDocument = from.getValue();

            // Create an entry for the initialized Document
            final DocumentData documentData = new DocumentData(this.iteratorDocumentKey, Collections.singleton(docKey), attrs, true);
            return Maps.immutableEntry(documentData, this.iteratorDocument);
        } catch (DatawaveFatalQueryException e) {
            throw e;
        } catch (IOException e) {
            QueryException qe = new QueryException(DatawaveErrorCode.APPLY_FUNCTION_ERROR, e);
            throw new DatawaveFatalQueryException(qe);
        }
    }

    @Override
    public boolean hasNext() {
        // Assign the next key. Ideally, the first iteration has already been performed.
        final Entry<Key,Value> next = this.nextSeek;

        // If not, however, perform the first iteration to find out if there's at least one applicable key
        if (next == UNINITIALIZED_KEY) {
            try {
                this.seekNext(true);
            } catch (IOException e) {
                QueryException qe = new QueryException(DatawaveErrorCode.HAS_NEXT_ELEMENT_ERROR, e);
                throw new DatawaveFatalQueryException(qe);
            }
        }

        return (this.nextSeek != ITERATOR_COMPLETE_KEY);
    }

    /*
     * Generate a column family string to use for tf column qualifier matching and creating new keys.
     *
     * @return a column family
     */
    private Text newColumnFamily() {
        // Declare return value
        final Text newCf;

        // Get the parent range's start key, if possible
        final Key key;
        if (null != this.parent) {
            key = this.parent.getStartKey(); // Highly unlikely to return null since a tf seek wouldn't use an infinite start key
        } else {
            key = null;
        }

        // Get the parent's column family, which is expected to be in the form
        // data_type\u0000data_type_id
        Text columnFamily;
        if (null != key) {
            columnFamily = key.getColumnFamily();
        } else {
            columnFamily = null;
        }

        // If not otherwise defined, try using the CQ of the iterator's start key for the CF,
        // which is alternatively expected to be in the form data_type\u0000data_type_id
        if (((null == columnFamily) || (columnFamily.getLength() == 0)) && (null != this.iteratorStartKey)) {
            columnFamily = this.iteratorStartKey.getColumnQualifier();
        }

        // Extract the tf column qualifier's prefix from the parent range's column family
        if ((null != columnFamily) && (columnFamily.getLength() > 0)) {
            newCf = new Text(columnFamily);
        } else {
            newCf = null;
        }

        // Gently complain if the column family is null
        if (null == newCf) {
            final String message = "Unable to perform tf shard table seek for index-only " + this.fieldName + " field. Could not "
                            + "determine the column family for range " + this.parent;
            LOG.trace(message);
        }

        return newCf;
    }

    /*
     * Create a document key based on a standard field key
     *
     * @param fieldKey A standard record field key
     *
     * @param timestamp The timestamp of the new document key
     *
     * @return a new document key
     */
    private Key newDocumentKey(final Key fieldKey, long timestamp) {
        final Text row = fieldKey.getRow();
        final Text cf = fieldKey.getColumnFamily();
        final Text cq = new Text();
        final Text visibility = new Text();
        return new Key(row, cf, cq, visibility, timestamp);
    }

    private KeyConversionAttributes newKeyConversionAttributes() {
        // Generate and validate the expected document's new column family, which will
        // be used for pattern matching the tf column qualifier and creating the column
        // family for converted keys.
        final Text columnFamily = this.newColumnFamily();
        final String tfCqPrefix;
        final String tfCqSuffix;
        if (null == columnFamily) {
            return null;
        } else {
            tfCqPrefix = columnFamily + this.delimiter;
            tfCqSuffix = this.delimiter + this.fieldName;
        }

        return new KeyConversionAttributes(tfCqPrefix, tfCqSuffix, columnFamily);
    }

    /*
     * Create a result key based on the initial entry
     *
     * @param from the initial entry
     *
     * @return a new document key
     */
    private Key newResultKey(final Entry<Key,Document> from) {
        final Key key = from.getKey();
        final Text row = key.getRow();
        final Text cf = key.getColumnFamily();
        final Text cq = key.getColumnQualifier();
        final Text visibility = key.getColumnVisibility();
        long timestamp = from.getValue().getTimestamp();
        return new Key(row, cf, cq, visibility, timestamp);
    }

    /*
     * Given a Key pointing to the start of an document to aggregate, construct a Range that should encapsulate the "document" to be aggregated together. Also
     * checks to see if data was found for the constructed Range before returning.
     *
     * @param documentKey A Key of the form "bucket tf:type\x00uid:"
     *
     * @return list of key/value entries where the key is of the form "bucket type\x00uid:fieldName"
     */
    private List<Entry<Key,Value>> newFieldEntriesFromTfEntries(final Key documentStartKey) throws IOException {
        // Initialize the return value with an immutable, empty list. A mutable list
        // will be instantiated only after relevant records are scanned.
        List<Entry<Key,Value>> documentAttributes = Collections.emptyList();

        // Loop until we've reached the end of the range, or found records scanned beyond the ones
        // having column qualifiers prefixed with our relevant data type and data type id (i.e., uid).
        while (this.hasNext()) {
            final Entry<Key,Value> docAttrKey = this.seekNext(false);
            if ((null != docAttrKey) && (docAttrKey != ITERATOR_COMPLETE_KEY)) {
                // Create an immutable entry for the document's newly created/converted field key
                final Entry<Key,Value> entry = Maps.immutableEntry(docAttrKey.getKey(), docAttrKey.getValue());

                // Re-initialize the return value with a mutable, non-empty list
                if (documentAttributes.isEmpty()) {
                    documentAttributes = new ArrayList<>(50);
                }
                documentAttributes.add(entry);
            }
        }

        return documentAttributes;
    }

    /*
     * Create a standard record field key based on a relevant tf key
     *
     * @param tfKey The tf key to convert
     *
     * @param tfCqPrefix The expected leading characters of the tf record's column qualifier
     *
     * @param tfCqSuffix The expected trailing characters of the tf record's column qualifier
     *
     * @param newCf The new key's column family
     *
     * @return a standard record field key, or null if the tf key's does not contain the necessary structure and information
     */
    private Entry<Key,Value> newFieldKeyFromTfKey(final Key tfKey, final KeyConversionAttributes attributes) {
        // Begin extracting information from the tf key that may, if relevant,
        // be used to create a standard record field key
        final Text row = tfKey.getRow();
        final Text cf = tfKey.getColumnFamily();
        final Text cq = tfKey.getColumnQualifier();
        final String tfCqPrefix = attributes.getTfCqPrefix();
        final String tfCqSuffix = attributes.getTfCqSuffix();
        final Text newCf = attributes.getNewCf();

        // Declare the return value
        final Entry<Key,Value> holder;

        // Validate the tf column family for null
        if (null == cf) {
            holder = NULL_COLUMNFAMILY_KEY;
        }
        // Validate the tf column qualifier for null
        else if (null == cq) {
            holder = NULL_COLUMNQUALIFIER_KEY;
        }
        // Otherwise, examine the column qualifier more closely. If it's still relevant,
        // construct a standard record field key.
        else {
            // Extract a string version of the column qualifier
            final String cqAsString = cq.toString();

            // Verify a non-null string
            if (null == cqAsString) {
                holder = NULL_COLUMNQUALIFIER_KEY;
            }
            // Ignore a top-level tf key. Although the range used via the IndexOnlyFunctionIterator
            // should prevent such a key from being scanned/created, this validation safeguards
            // against the return of invalid keys.
            else if (cqAsString.isEmpty()) {
                holder = TOP_RECORD_KEY;
            }
            // Verify the prefix matches the document's desired column family. Although the range
            // constructed via the IndexOnlyFunctionIterator should prevent an invalid cf from reaching
            // this point, this validation safeguards against its processing.
            else if (!cqAsString.startsWith(tfCqPrefix)) {
                holder = WRONG_DOCUMENT_KEY;
            }
            // Verify the suffix matches the field name. Although the range constructed via the
            // IndexOnlyFunctionIterator should prevent an invalid cq prefix from reaching this point,
            // this validation safeguards against its processing.
            else if (!cqAsString.endsWith(tfCqSuffix)) {
                holder = WRONG_FIELD_KEY;
            }
            // Extract the tf record's value without the use of String.split() or instantiating more than
            // a single String. Although splitting on the null character may seem quick and easy, unexpected
            // results may occur if the value contains one or more null characters.
            else {
                // Declare the value, which will be assigned only once for sake of efficiency
                final String value;

                // Get the lengths of the relevant strings
                int cqLength = cqAsString.length();
                int prefixLength = tfCqPrefix.length();
                int suffixLength = tfCqSuffix.length();

                // Verify that the cq string's length is at least as big as the combined
                // prefix and suffix
                if (cqLength >= (prefixLength + suffixLength)) {
                    // Extract and assign the value
                    value = cqAsString.substring(prefixLength, (cqLength - suffixLength));
                } else {
                    value = null;
                }

                // If a value is defined, even if it is an empty string, use it to construct a new
                // field key
                if (null != value) {
                    final Text newFieldCq = new Text(this.fieldName + this.delimiter + value);
                    final Key newKey = new Key(row, newCf, newFieldCq, tfKey.getColumnVisibility(), tfKey.getTimestamp());
                    holder = Maps.immutableEntry(newKey, EMPTY_VALUE);
                } else {
                    holder = INVALID_COLUMNQUALIFIER_FORMAT_KEY;
                }
            }
        }

        return holder;
    }

    @Override
    public Entry<DocumentData,Document> next() {
        final Entry<Key,Value> next;
        try {
            next = this.seekNext(false);
        } catch (IOException e) {
            QueryException qe = new QueryException(DatawaveErrorCode.SEEK_NEXT_ELEMENT_ERROR, e);
            throw new DatawaveFatalQueryException(qe);
        }

        final Entry<DocumentData,Document> entry;
        if (null != next) {
            final List<Entry<Key,Value>> keyValues = new LinkedList<>();
            keyValues.add(next);
            Key docKey = getDocKey(next.getKey());
            final DocumentData documentData = new DocumentData(this.iteratorDocumentKey, Collections.singleton(docKey), keyValues, true);
            entry = Maps.immutableEntry(documentData, this.iteratorDocument);
        } else if (next == ITERATOR_COMPLETE_KEY) {
            QueryException qe = new QueryException(DatawaveErrorCode.FETCH_NEXT_ELEMENT_ERROR,
                            MessageFormat.format("Fieldname: {0}, Range: {1}", this.fieldName, this.parent));
            throw (NoSuchElementException) (new NoSuchElementException().initCause(qe));
        } else {
            entry = null;
        }

        return entry;
    }

    private Entry<Key,Value> seekNext() throws IOException {
        // Assign the return value
        final Entry<Key,Value> next;
        boolean reassignIteratorDocKey;
        if (this.nextSeek != UNINITIALIZED_KEY) {
            next = this.nextSeek;
            reassignIteratorDocKey = false;
        } else {
            next = null;
            reassignIteratorDocKey = true;
        }

        // Fetch the next value, if any
        final Entry<Key,Value> fetched;
        if (this.source.hasTop()) {
            final KeyConversionAttributes attributes = this.iteratorConversionAttributes;
            fetched = this.nextFieldEntryFromTfEntry(attributes, false);
        } else {
            fetched = null;
        }

        // Assign the next value
        if (null != fetched) {
            this.nextSeek = fetched;
            if (reassignIteratorDocKey) {
                long timestamp = this.iteratorDocumentKey.getTimestamp();
                final Key fieldKey = fetched.getKey();
                this.iteratorDocumentKey = this.newDocumentKey(fieldKey, timestamp);
            }
        } else {
            this.nextSeek = ITERATOR_COMPLETE_KEY;
        }

        return next;
    }

    private Entry<Key,Value> seekNext(boolean hasNext) throws IOException {
        // Declare the return value
        final Entry<Key,Value> next;

        // Validate the iterator's state
        if ((null == this.iteratorStartKey) || (null == this.iteratorConversionAttributes)) {
            final String message = "Unable to seek tf section of shard table for index-only " + this.fieldName + " records,"
                            + "possibly due to an invalid Range " + this.parent;
            LOG.error(message, new IllegalStateException(this.getClass().getSimpleName() + " has not been initialized"));
            next = ITERATOR_COMPLETE_KEY;
            this.nextSeek = ITERATOR_COMPLETE_KEY;
        }
        // Otherwise, perform seek(s)
        else {
            // If not initialized and next() is called vs. hasNext(), perform a seek to pre-populate
            // the nextSeek instance variable
            if ((this.nextSeek == UNINITIALIZED_KEY) && !hasNext) {
                this.seekNext();
            }

            // Get the next value
            next = this.seekNext();
        }

        // Return the next sought key, if any
        return next;
    }

    /*
     * Given a Key pointing to the start of an document to aggregate, construct a Range that should encapsulate the "document" to be aggregated together. Also
     * checks to see if data was found for the constructed Range before returning.
     *
     * @param documentKey A Key of the form "bucket tf:type\x00uid:"
     *
     * @return list of key/value entries where the key is of the form "bucket type\x00uid:fieldName"
     */
    private Entry<Key,Value> nextFieldEntryFromTfEntry(final KeyConversionAttributes attributes, boolean exitOnWrongDoc) throws IOException {
        // Initialize the return value
        Entry<Key,Value> validHolder = null;

        // Loop until we've found a relevant record, reached the end of the range, or found records
        // scanned beyond the ones having column qualifiers prefixed with our relevant data type and
        // data type id (i.e., uid).
        Key tfKey = this.source.getTopKey();
        while ((null == validHolder) && this.equality.partOf(this.iteratorStartKey, tfKey)) {
            // Try to generate a new field key from the
            final Entry<Key,Value> holder = this.newFieldKeyFromTfKey(tfKey, attributes);
            if (null != holder) {
                // Create and add an entry for a relevant, successfully converted tf key. Although many
                // scanned keys may contain irrelevant field names, they all should belong to the correct
                // document/event due to the Range constructed by the IndexOnlyFunctionIterator. That
                // said, much of the following validation provides an additional layer of protection
                // against unexpected and otherwise irrelevant records.
                if ((WRONG_FIELD_KEY != holder) && (WRONG_DOCUMENT_KEY != holder) && (INVALID_COLUMNQUALIFIER_FORMAT_KEY != holder)
                                && (NULL_COLUMNFAMILY_KEY != holder) && (NULL_COLUMNQUALIFIER_KEY != holder) && (TOP_RECORD_KEY != holder)
                                && (WRONG_COLUMNFAMILY_KEY != holder)) {
                    validHolder = holder;
                }
                // Exit the loop if we've converted the document's last tf record into a field record.
                // We will know that we have scanned far enough because we have collected at least one relevant
                // tf record, but fetched a tf record related to a different document.
                else if (WRONG_DOCUMENT_KEY == holder && exitOnWrongDoc) {
                    break;
                }
            }

            // Try to advance to the next tf record
            this.source.next();

            // Check to see if there is a next record
            if ((null == validHolder) && this.source.hasTop()) {
                tfKey = this.source.getTopKey();
            }
            // Otherwise, stop looping
            else {
                break;
            }
        }

        return validHolder;
    }

    @Override
    public void remove() {
        // No op
    }

    /*
     * Internally generated container of information used to convert tf record keys into standard document keys
     */
    private class KeyConversionAttributes {
        private final String tfCqPrefix;
        private final String tfCqSuffix;
        private final Text newCf;

        public KeyConversionAttributes(final String tfCqPrefix, final String tfCqSuffix, final Text newCf) {
            this.tfCqPrefix = tfCqPrefix;
            this.tfCqSuffix = tfCqSuffix;
            this.newCf = newCf;
        }

        public String getTfCqPrefix() {
            return tfCqPrefix;
        }

        public String getTfCqSuffix() {
            return tfCqSuffix;
        }

        public Text getNewCf() {
            return newCf;
        }
    }
}
