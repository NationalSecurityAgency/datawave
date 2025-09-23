package datawave.query.iterator;

import static datawave.query.jexl.visitors.EventDataQueryExpressionVisitor.getExpressionFilters;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nullable;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.composite.CompositeMetadata;
import datawave.query.function.Aggregation;
import datawave.query.function.KeyToDocumentData;
import datawave.query.function.RangeProvider;
import datawave.query.jexl.visitors.EventDataQueryExpressionVisitor.ExpressionFilter;
import datawave.query.predicate.EventDataQueryFilter;
import datawave.query.predicate.ParentEventDataFilter;
import datawave.query.predicate.ParentRangeProvider;
import datawave.query.util.Tuple2;
import datawave.query.util.TupleToEntry;

public class ParentQueryIterator extends QueryIterator {
    private static final Logger log = Logger.getLogger(ParentQueryIterator.class);

    protected boolean parentDisableIndexOnlyDocuments = false;

    public ParentQueryIterator() {}

    public ParentQueryIterator(QueryIterator other, IteratorEnvironment env) {
        super(other, env);
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("ParentQueryIterator init()");
        }

        super.init(source, options, env);

        if (disableIndexOnlyDocuments) {
            parentDisableIndexOnlyDocuments = disableIndexOnlyDocuments;
            disableIndexOnlyDocuments = false;
        }
    }

    @Override
    public EventDataQueryFilter getEvaluationFilter() {
        if (evaluationFilter == null && getScript() != null) {

            AttributeFactory attributeFactory = new AttributeFactory(typeMetadata);
            Map<String,ExpressionFilter> expressionFilters = getExpressionFilters(getScript(), attributeFactory);

            this.evaluationFilter = new ParentEventDataFilter(expressionFilters);
        }
        return evaluationFilter != null ? evaluationFilter.clone() : null;
    }

    /**
     * In the Parent case replace the {@link QueryOptions#eventFilter} with an evaluation filter
     *
     * @return an evaluation filter
     */
    public EventDataQueryFilter getEventFilter() {
        return getEvaluationFilter();
    }

    @Override
    public EventDataQueryFilter getFiEvaluationFilter() {
        if (fiEvaluationFilter == null) {
            fiEvaluationFilter = getEvaluationFilter();
        }
        return fiEvaluationFilter.clone();
    }

    @Override
    public EventDataQueryFilter getEventEvaluationFilter() {
        if (eventEvaluationFilter == null) {
            eventEvaluationFilter = getEvaluationFilter();
        }
        return eventEvaluationFilter.clone();
    }

    @Override
    public Iterator<Entry<Key,Document>> mapDocument(SortedKeyValueIterator<Key,Value> deepSourceCopy, Iterator<Entry<Key,Document>> documents,
                    CompositeMetadata compositeMetadata) {

        // no evaluation filter here as this is post evaluation

        Aggregation aggregation = new Aggregation(this.getTimeFilter(), this.typeMetadataWithNonIndexed, compositeMetadata, this.isIncludeGroupingContext(),
                        this.includeRecordId, this.parentDisableIndexOnlyDocuments, null);

        SortedKeyValueIterator<Key,Value> docMapperSource = getSourceDeepCopy("map document - key to document");
        KeyToDocumentData k2d = new KeyToDocumentData(docMapperSource, this.myEnvironment, this.documentOptions, getEquality(), null,
                        this.includeHierarchyFields, this.includeHierarchyFields);
        k2d.withRangeProvider(getRangeProvider());
        k2d.withAggregationThreshold(getDocAggregationThresholdMs());

        Iterator<Tuple2<Key,Document>> parentDocuments = Iterators.transform(documents, new GetParentDocument(k2d, aggregation));

        Iterator<Entry<Key,Document>> retDocuments = Iterators.transform(parentDocuments, new TupleToEntry<>());
        retDocuments = Iterators.transform(retDocuments, new ParentQueryIterator.KeepAllFlagSetter());
        return retDocuments;
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new ParentQueryIterator(this, env);
    }

    public class KeepAllFlagSetter implements Function<Entry<Key,Document>,Entry<Key,Document>> {

        @Nullable
        @Override
        public Entry<Key,Document> apply(@Nullable Entry<Key,Document> input) {
            for (Attribute attr : input.getValue().getDictionary().values()) {
                attr.setToKeep(true);
            }
            return input;
        }

        @Override
        public boolean equals(@Nullable Object object) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Get a {@link ParentRangeProvider}
     *
     * @return a {@link ParentRangeProvider}
     */
    @Override
    public RangeProvider getRangeProvider() {
        if (rangeProvider == null) {
            rangeProvider = new ParentRangeProvider();
        }
        return rangeProvider;
    }
}
