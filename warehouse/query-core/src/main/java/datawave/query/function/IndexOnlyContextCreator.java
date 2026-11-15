package datawave.query.function;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl3.parser.JexlNode;

import com.google.common.base.Function;
import com.google.common.collect.Multimap;

import datawave.query.attributes.Document;
import datawave.query.collections.FunctionalSet;
import datawave.query.composite.CompositeMetadata;
import datawave.query.iterator.QueryOptions;
import datawave.query.jexl.DatawaveJexlContext;
import datawave.query.jexl.DelayedNonEventIndexContext;
import datawave.query.jexl.visitors.IteratorBuildingVisitor;
import datawave.query.predicate.TimeFilter;
import datawave.query.util.Tuple3;
import datawave.query.util.TypeMetadata;
import datawave.table.constants.QueryConstants;

/**
 * Used to create a specialized, lazy-fetching index only context that in practice was quite horrible.
 * <p>
 * Currently used to create a {@link DelayedNonEventIndexContext} which is far more responsible with the delegated resources.
 *
 * @see DelayedNonEventIndexContext
 */
public class IndexOnlyContextCreator extends JexlContextCreator {

    private final Function<Range,Key> getDocumentKey;
    private final boolean includeGroupingContext;
    private final boolean includeRecordId;
    private final Collection<String> indexOnlyFields;
    private final TypeMetadata typeMetadata;
    private final CompositeMetadata compositeMetadata;
    private final Range range;
    private final IteratorBuildingVisitor iteratorBuildingVisitor;
    private final Multimap<String,JexlNode> delayedNonEventFieldMap;
    private final Equality equality;
    private final Collection<ByteSequence> columnFamilies;
    private final boolean inclusive;
    private final TimeFilter timeFilter;

    /**
     * Builder class for {@link IndexOnlyContextCreator} supporting fluent-style building.
     */
    public static class Builder {

        private Collection<String> variables;
        private TypeMetadata typeMetadata;
        private CompositeMetadata compositeMetadata;
        private Range range;
        private IteratorBuildingVisitor iteratorBuildingVisitor;
        private Multimap<String,JexlNode> delayedNonEventFieldMap;
        private Equality equality;
        private Collection<ByteSequence> columnFamilies;
        private boolean inclusive;
        private SortedKeyValueIterator<Key,Value> source;
        private JexlContextCreator.JexlContextValueComparator comparatorFactory;
        private QueryOptions options;

        /**
         * Build and return a new {@link IndexOnlyContextCreator} using the fields of this builder.
         *
         * @return the new creator
         */
        public IndexOnlyContextCreator build() {
            return new IndexOnlyContextCreator(this);
        }

        /**
         * Set the collection of variables.
         *
         * @param variables
         *            the variables
         * @return this builder
         */
        public Builder setVariables(Collection<String> variables) {
            this.variables = variables;
            return this;
        }

        /**
         * Set the comparator factory to use.
         *
         * @param comparatorFactory
         *            the factory
         * @return this builder
         */
        public Builder setComparatorFactory(JexlContextCreator.JexlContextValueComparator comparatorFactory) {
            this.comparatorFactory = comparatorFactory;
            return this;
        }

        /**
         * Set the query options, various query parameters and helper components.
         *
         * @param options
         *            the query options
         * @return this builder
         */
        public Builder setOptions(QueryOptions options) {
            this.options = options;
            return this;
        }

        /**
         * Set the iterator used to fetch index-only records, as applicable, from the shard table
         *
         * @param source
         *            the iterator
         * @return this builder
         */
        public Builder setSource(SortedKeyValueIterator<Key,Value> source) {
            this.source = source;
            return this;
        }

        /**
         * Set the normalizers that are applied to the value(s) of fields fetched for a given document.
         *
         * @param typeMetadata
         *            the type metadata
         * @return this builder
         */
        public Builder setTypeMetadata(TypeMetadata typeMetadata) {
            this.typeMetadata = typeMetadata;
            return this;
        }

        /**
         * Set the composite metadata.
         *
         * @param compositeMetadata
         *            the composite metadata
         * @return this builder
         */
        public Builder setCompositeMetadata(CompositeMetadata compositeMetadata) {
            this.compositeMetadata = compositeMetadata;
            return this;
        }

        /**
         * Set the range for a given document fetched from the shard table.
         *
         * @param range
         *            the range
         * @return this builder
         */
        public Builder setRange(Range range) {
            this.range = range;
            return this;
        }

        /**
         * Set the iterator visitor.
         *
         * @param iteratorBuildingVisitor
         *            the visitor
         * @return this builder
         */
        public Builder setIteratorBuildingVisitor(IteratorBuildingVisitor iteratorBuildingVisitor) {
            this.iteratorBuildingVisitor = iteratorBuildingVisitor;
            return this;
        }

        /**
         * Set the map of delayed non-event fields.
         *
         * @param delayedNonEventFieldMap
         *            the map
         * @return this builder
         */
        public Builder setDelayedNonEventFieldMap(Multimap<String,JexlNode> delayedNonEventFieldMap) {
            this.delayedNonEventFieldMap = delayedNonEventFieldMap;
            return this;
        }

        /**
         * Set the equality object.
         *
         * @param equality
         *            the equality
         * @return this builder
         */
        public Builder setEquality(Equality equality) {
            this.equality = equality;
            return this;
        }

        /**
         * Set the column families.
         *
         * @param columnFamilies
         *            the column families
         * @return this builder
         */
        public Builder setColumnFamilies(Collection<ByteSequence> columnFamilies) {
            this.columnFamilies = columnFamilies;
            return this;
        }

        /**
         * Set whether the range is inclusive.
         *
         * @param inclusive
         *            the range inclusivity
         * @return this builder
         */
        public Builder setInclusive(boolean inclusive) {
            this.inclusive = inclusive;
            return this;
        }
    }

    /**
     * Return a new {@link IndexOnlyContextCreator.Builder} instance.
     *
     * @return the new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    public IndexOnlyContextCreator(Builder builder) {
        super(builder.variables, builder.comparatorFactory);
        checkNotNull(builder.source, "source must not be null");
        checkNotNull(builder.range, "range must not be null");
        checkNotNull(builder.options, "options must not be null");

        this.typeMetadata = builder.typeMetadata;
        this.compositeMetadata = builder.compositeMetadata;
        this.getDocumentKey = builder.options.getDocumentKey();
        this.includeGroupingContext = builder.options.isIncludeGroupingContext();
        this.includeRecordId = builder.options.isIncludeRecordId();
        this.indexOnlyFields = builder.options.getIndexOnlyFields();

        // For delayed index lookup.
        this.iteratorBuildingVisitor = builder.iteratorBuildingVisitor;
        this.delayedNonEventFieldMap = builder.delayedNonEventFieldMap;
        this.equality = builder.equality;
        this.columnFamilies = builder.columnFamilies;
        this.inclusive = builder.inclusive;

        this.range = builder.range;
        this.timeFilter = builder.options.getTimeFilter();
    }

    @Override
    public Tuple3<Key,Document,DatawaveJexlContext> apply(Tuple3<Key,Document,Map<String,Object>> from) {
        final Tuple3<Key,Document,DatawaveJexlContext> tuple;
        if (null != from) {
            tuple = super.apply(from);
            verifyContext(tuple.third());
        } else {
            tuple = null;
        }
        return tuple;
    }

    public Function<Range,Key> getGetDocumentKey() {
        return this.getDocumentKey;
    }

    public Collection<String> getIndexOnlyFields() {
        if (null != this.indexOnlyFields) {
            return new HashSet<>(this.indexOnlyFields);
        } else {
            return Collections.emptySet();
        }
    }

    public TypeMetadata getTypeMetadata() {
        if (null != this.typeMetadata) {
            return new TypeMetadata(this.typeMetadata);
        } else {
            return new TypeMetadata();
        }
    }

    public CompositeMetadata getCompositeMetadata() {
        return compositeMetadata;
    }

    public Range getRange() {
        return this.range;
    }

    public TimeFilter getTimeFilter() {
        return this.timeFilter;
    }

    public boolean isIncludeGroupingContext() {
        return this.includeGroupingContext;
    }

    public boolean isIncludeRecordId() {
        return includeRecordId;
    }

    @Override
    protected DatawaveJexlContext newDatawaveJexlContext(final Tuple3<Key,Document,Map<String,Object>> from) {
        DatawaveJexlContext newContext = super.newDatawaveJexlContext(from);

        // see if there are any delayed nodes that need to be processed
        if (delayedNonEventFieldMap != null && !delayedNonEventFieldMap.isEmpty()) {
            // build the current document range from the document Key to end of the document, even though for some query logics this may be too large a range,
            // it will be narrowed with equality later
            Key startKey = new Key(from.first().getRow(), from.first().getColumnFamily());
            Key endKey = new Key(startKey.getRow().toString(), startKey.getColumnFamily() + QueryConstants.MAX_UNICODE_STRING);
            Range docRange = new Range(startKey, true, endKey, false);

            newContext = new DelayedNonEventIndexContext(newContext, iteratorBuildingVisitor, delayedNonEventFieldMap, docRange, columnFamilies, inclusive,
                            equality);
        }

        return newContext;
    }

    /**
     * Ensures that downstream expectations are met with regard to the state of the given context
     *
     * @param context
     *            jexl context to verify, assumed to be non-null
     */
    private void verifyContext(DatawaveJexlContext context) {
        // Make sure that index-only fields being queried are represented in the context, in
        // order to avoid potential downstream eval misfire (see #1204)
        for (String var : this.variables) {
            if (this.indexOnlyFields.contains(var) && !context.has(var)) {
                context.set(var, FunctionalSet.empty());
            }
        }
    }
}
