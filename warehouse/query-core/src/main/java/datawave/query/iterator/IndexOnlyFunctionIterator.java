package datawave.query.iterator;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.jexl2.MapContext;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.Iterators;

import datawave.query.attributes.Document;
import datawave.query.composite.CompositeMetadata;
import datawave.query.function.Aggregation;
import datawave.query.function.IndexOnlyContextCreator;
import datawave.query.function.IndexOnlyKeyToDocumentData;
import datawave.query.function.JexlContextCreator;
import datawave.query.jexl.DatawaveJexlContext;
import datawave.query.postprocessing.tf.EmptyTermFrequencyFunction;
import datawave.query.predicate.TimeFilter;
import datawave.query.util.EntryToTuple;
import datawave.query.util.TraceIterators;
import datawave.query.util.Tuple2;
import datawave.query.util.Tuple3;
import datawave.query.util.TypeMetadata;

/**
 * Inserted into the iterator and JEXL evaluation "stacks" to allow functions to operate on index-only fields, such as HEAD, FOOT, BODY, and META.
 */
public class IndexOnlyFunctionIterator<T> extends WrappingIterator<T> {
    /**
     * Column family for term frequency records in the shard table
     */
    public static final String TF_COLUMN_FAMILY = "tf";

    private static Logger LOG = Logger.getLogger(IndexOnlyFunctionIterator.class);

    private final IndexOnlyContextCreator contextCreator;

    private final SortedKeyValueIterator<Key,Value> indexOnlySeeker;

    private final Collection<String> indexOnlyFields;

    private Iterator<T> parentIterator;

    private Range parentRange;

    private final Key documentKey;

    /**
     * Constructor
     *
     * @param documentIterator
     *            The iterator that does the yeoman's work of fetching shard records from the data store, applying normalizers, and performing similar
     *            operations.
     * @param contextCreator
     *            Creates JexlContexts and contains the information needed to seek and retrieve index-only fields in a manner similar to standard fields, albeit
     *            from different locations in the shard table.
     */
    public IndexOnlyFunctionIterator(final SortedKeyValueIterator<Key,Value> documentIterator, final IndexOnlyContextCreator contextCreator) {
        this(documentIterator, contextCreator, null);
    }

    /**
     * Constructor
     *
     * @param documentIterator
     *            The iterator that does the yeoman's work of fetching shard records from the data store, applying normalizers, and performing similar
     *            operations.
     * @param contextCreator
     *            Creates JexlContexts and contains the information needed to seek and retrieve index-only fields in a manner similar to standard fields, albeit
     *            from different locations in the shard table.
     * @param documentKey
     *            Key used for building a document in conjunction with the JexlContext
     */
    public IndexOnlyFunctionIterator(final SortedKeyValueIterator<Key,Value> documentIterator, final IndexOnlyContextCreator contextCreator,
                    final Key documentKey) {
        // Validate and assign instance variables. Collections provided by the IndexOnlyIteratorContext are guaranteed
        // to be non-null.
        checkNotNull(documentIterator, this.getClass().getSimpleName() + " cannot be initialized with a null document iterator");
        checkNotNull(contextCreator, this.getClass().getSimpleName() + " cannot be initialized with a null " + IndexOnlyContextCreator.class.getSimpleName());
        this.parentRange = contextCreator.getRange();
        checkNotNull(this.parentRange, this.getClass().getSimpleName() + " cannot be initialized with a null Range");
        Function<Range,Key> getDocumentKey = contextCreator.getGetDocumentKey();
        if (null == getDocumentKey) {
            final String message = this.getClass().getSimpleName() + " cannot function properly with a null getDocumentKey value";
            LOG.warn(message, new IllegalArgumentException());
        }
        this.indexOnlySeeker = documentIterator;
        this.contextCreator = contextCreator;
        this.indexOnlyFields = contextCreator.getIndexOnlyFields();
        checkNotNull(this.indexOnlyFields, this.getClass().getSimpleName() + " cannot be initialized with a null collection of index-only field names");
        this.documentKey = documentKey;
    }

    /*
     * Trigger the fetch by creating a stack of iterators based on a specialized, index-only, KeyToDocumentData implementation.
     *
     * @param fieldName The field to be fetched
     *
     * @param fetchAllRecords If true, fetch all relevant records, fully populating the Document and its DocumentData with all relevant name/value pairs.
     *
     * @return an iterator of Key/Document pairs
     */
    private <E> Iterator<Entry<Key,Document>> initializeFetch(final String fieldName, final IndexOnlyKeyToDocumentData keyToDocumentData) {
        Collection<Entry<Key,Document>> collection = Collections.emptySet();
        Iterator<Entry<Key,Document>> documents = collection.iterator();
        try {
            // Create a range to load a document with index-only information
            final Range parent = this.parentRange;
            final Key startKey = parent.getStartKey();
            final Text tfRow = startKey.getRow();
            final Text tfCf = new Text(TF_COLUMN_FAMILY);
            Text tfPartialCq = startKey.getColumnFamily();
            if ((tfPartialCq.getLength() == 0) && (null != this.documentKey)) {
                tfPartialCq = this.documentKey.getColumnFamily();
            }
            final ColumnVisibility cv = new ColumnVisibility(startKey.getColumnVisibility());
            long timeStamp = startKey.getTimestamp();
            final Key start = new Key(tfRow, tfCf, tfPartialCq, cv, timeStamp);
            final Key stop = new Key(tfRow, tfCf, tfPartialCq, cv, timeStamp);
            final Range indexOnlyRange = new Range(start, stop);

            // Take the document Keys and transform it into Entry<Key,Document>, which will remove attributes for this document
            // not falling within the expected time range
            final TypeMetadata typeMetadata = this.contextCreator.getTypeMetadata();
            final CompositeMetadata compositeMetadata = this.contextCreator.getCompositeMetadata();
            boolean includeGroupingContext = this.contextCreator.isIncludeGroupingContext();
            final TimeFilter timeFilter = this.contextCreator.getTimeFilter();
            boolean includeRecordId = this.contextCreator.isIncludeRecordId();
            final Aggregation aggregation = new Aggregation(timeFilter, typeMetadata, compositeMetadata, includeGroupingContext, includeRecordId, false, null);

            // Construct an iterator to build the document. Although the DocumentData will be retrieved from the tf section
            // of the shard table, the IndexOnlyKeyToDocumentData will reformat the entries to "look" like records from standard
            // columns.
            final Key documentKey = this.contextCreator.getGetDocumentKey().apply(indexOnlyRange);
            final DocumentSpecificTreeIterable source = new DocumentSpecificTreeIterable(documentKey, keyToDocumentData);

            // Initialize the seek
            source.iterator();

            // Initialize the fetch
            documents = Iterators.transform(keyToDocumentData, aggregation);
        } catch (final Exception e) {
            final String message = "Could not perform function on index-only field '" + fieldName + "\' for range " + this.parentRange;
            LOG.error(message, e);
        }

        return documents;
    }

    public <E> Collection<E> fetchAllIndexOnlyValues(final String fieldName) {
        Collection<E> values = Collections.emptySet();
        try {
            // Create a lazy-fetching iterator
            final Iterator<E> iterator = this.newLazyFetchingIterator(fieldName);

            // Get the values from the context and assign to the return variable
            while (iterator.hasNext()) {
                final E value = iterator.next();
                if (values.isEmpty()) {
                    values = new HashSet<>();
                }
                values.add(value);
            }
        } catch (final Exception e) {
            final String message = "Could not perform function on index-only field '" + fieldName + "\' for range " + this.parentRange;
            LOG.error(message, e);
        }

        return values;
    }

    /**
     * Returns a collection of index-only field names
     *
     * @return a collection of index-only field names
     */
    public Collection<String> getIndexOnlyFields() {
        return this.indexOnlyFields;
    }

    @Override
    public boolean hasNext() {
        boolean hasNext = false;
        if (null != this.parentIterator) {
            hasNext = this.parentIterator.hasNext();
        }

        return hasNext;
    }

    @SuppressWarnings("unchecked")
    private <E> E tupleToGeneric(final String fieldName, final Tuple3<Key,Document,DatawaveJexlContext> tuple) {
        // Get the JEXL context
        MapContext context = null;
        if (null != tuple) {
            context = tuple.third();
        }

        // Declare the return value
        final E value;

        // Get the value(s) from the context and assign to the return variable
        if (context instanceof SingleValueContext) {
            value = (E) context.get(context.toString());
        } else if (null != context) {
            value = (E) context.get(fieldName);
        } else {
            value = null;
        }

        return value;
    }

    /**
     * Create an iterator that will allow for lazy, incremental fetching of index-only values
     *
     * @param fieldName
     *            The field to fetch
     * @param <E>
     *            type for the iterator
     * @return An iterator that will allow for lazy, incremental fetching of index-only values
     */
    @SuppressWarnings("unchecked")
    public <E> Iterator<E> newLazyFetchingIterator(final String fieldName) {
        Collection<E> values = Collections.emptySet();
        Iterator<E> iterator = values.iterator();
        try {
            // Create a specialized, index-only KeyToDocumentData
            final IndexOnlyKeyToDocumentData keyToDocumentData = new IndexOnlyKeyToDocumentData(this.parentRange, fieldName, this.indexOnlySeeker, false);

            // Initialize the fetch
            final Iterator<Entry<Key,Document>> documents = this.initializeFetch(fieldName, keyToDocumentData);

            // Populate the document
            final Iterator<Tuple2<Key,Document>> tupleItr = Iterators.transform(documents, new EntryToTuple<>());
            final EmptyTermFrequencyFunction tfFunction = new EmptyTermFrequencyFunction();
            final Iterator<Tuple3<Key,Document,Map<String,Object>>> itrWithContext = TraceIterators.transform(tupleItr, tfFunction, "Term Frequency Lookup");
            final Iterator<Tuple3<Key,Document,DatawaveJexlContext>> itrWithDatawaveJexlContext = Iterators.transform(itrWithContext,
                            new SingleValueContextCreator(fieldName));

            // Replace the return value with a lazy-fetching iterator
            iterator = (Iterator<E>) Iterators.transform(itrWithDatawaveJexlContext, new TupleToGenericFunction(fieldName));
        } catch (final Exception e) {
            final String message = "Could not perform function on index-only field '" + fieldName + "\' for range " + this.parentRange;
            LOG.error(message, e);
        }

        return iterator;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T next() {
        final Object object;
        if (null != this.parentIterator) {
            object = this.parentIterator.next();
        } else {
            object = null;
        }

        return (T) object;
    }

    @Override
    public void remove() {
        if (null != this.parentIterator) {
            this.parentIterator.remove();
        }
    }

    @Override
    public void setDelegate(final Iterator<T> delegate) {
        this.parentIterator = delegate;
        super.setDelegate(delegate);
    }

    /*
     * Transforms an index-oonly tuple to a Generic class instance
     */
    private class TupleToGenericFunction implements Function<Tuple3<Key,Document,DatawaveJexlContext>,T> {
        private final String fieldName;

        public TupleToGenericFunction(final String fieldName) {
            this.fieldName = fieldName;
        }

        @Override
        public T apply(final Tuple3<Key,Document,DatawaveJexlContext> tuple) {
            return IndexOnlyFunctionIterator.this.tupleToGeneric(this.fieldName, tuple);
        }
    }

    /*
     * Normally, a DatawaveJexlContext stores up all values fetched for a given field. In order to reduce the large memory footprint of index-only fields,
     * however, such values are fetched incrementally and allowed [potentially] to be "dropped on the floor" immediately for garbage-collection.
     *
     * This JexlContext helps in the reduction of the memory footprint by "remembering" only the last known name/value pair added via the set(..) method. In
     * order to prevent an underlying Document from creating a Collection instance to accumulate JexlContext field values, this context always returns null when
     * the get(..) method is called using the normal field name.
     *
     * In order to retrieve the last-fetched, actual value from the get(..) method, index-only logic can use the SingleValueContext's "contextKey" as a
     * specially recognized field name. Although the contextKey can be obtained via a getter method after casting to SingleValueContext class, a less expensive
     * means is provided by calling the context's toString() method.
     */
    private static class SingleValueContext extends DatawaveJexlContext {

        private final String contextKey;
        private final String fieldName;
        private Object value;

        public SingleValueContext(final String fieldName) {
            this.contextKey = this.getClass().getName() + '.' + fieldName;
            this.fieldName = fieldName;
        }

        @Override
        public boolean has(final String name) {
            return (null != this.get(name));
        }

        @Override
        public Object get(final String name) {
            final Object value;
            if (this.contextKey.equals(name)) {
                value = this.value;
            } else {
                value = null;
            }

            return value;
        }

        public String getContextKey() {
            return this.contextKey;
        }

        @Override
        public void set(final String name, final Object value) {
            if ((null != this.fieldName) && this.fieldName.equals(name)) {
                // Prevent an underlying Document from trying to set a collection
                // having only a single value. Such a case shouldn't happen given the
                // Document's current implementation, but this logic safeguards against
                // a potential change.
                final Object nonSingleValueCollectionValue;
                if (value instanceof Collection<?>) {
                    final Collection<?> collection = (Collection<?>) value;
                    if (collection.size() == 1) {
                        nonSingleValueCollectionValue = collection.iterator().next();
                    } else {
                        nonSingleValueCollectionValue = collection;
                    }
                } else {
                    nonSingleValueCollectionValue = value;
                }

                // Set the value
                this.value = nonSingleValueCollectionValue;
            }
        }

        @Override
        public String toString() {
            return this.getContextKey();
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof SingleValueContext)) {
                return false;
            }
            SingleValueContext other = (SingleValueContext) o;
            return super.equals(other) && Objects.equal(contextKey, other.contextKey) && Objects.equal(value, other.value)
                            && Objects.equal(fieldName, other.fieldName);
        }

        @Override
        public int hashCode() {
            return super.hashCode() + 31 * Objects.hashCode(fieldName, contextKey, value);
        }
    }

    /*
     * Allows for the re-use of specialized, single-value JEXL contexts
     */
    private static class SingleValueContextCreator extends JexlContextCreator {
        private final DatawaveJexlContext context;

        public SingleValueContextCreator(final String fieldName) {
            this.context = new SingleValueContext(fieldName);
        }

        @Override
        protected DatawaveJexlContext newDatawaveJexlContext(final Tuple3<Key,Document,Map<String,Object>> from) {
            return this.context;
        }

    }
}
