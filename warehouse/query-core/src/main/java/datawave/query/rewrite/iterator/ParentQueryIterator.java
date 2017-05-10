package datawave.query.rewrite.iterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import datawave.query.rewrite.attributes.Attribute;
import datawave.query.rewrite.attributes.Document;
import datawave.query.rewrite.function.Aggregation;
import datawave.query.rewrite.function.KeyToDocumentData;
import datawave.query.rewrite.predicate.EventDataQueryFilter;
import datawave.query.rewrite.predicate.ParentEventDataFilter;
import datawave.query.util.CompositeMetadata;
import datawave.query.util.Tuple2;
import datawave.query.util.TupleToEntry;

import com.google.common.base.Function;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;

import javax.annotation.Nullable;

public class ParentQueryIterator extends QueryIterator {
    private static final Logger log = Logger.getLogger(ParentQueryIterator.class);
    
    protected boolean parentDisableIndexOnlyDocuments = false;
    
    protected EventDataQueryFilter evaluationFilter = null;
    
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
        if (evaluationFilter == null) {
            this.evaluationFilter = new ParentEventDataFilter(script);
        }
        return evaluationFilter;
    }
    
    @Override
    public Iterator<Entry<Key,Document>> mapDocument(SortedKeyValueIterator<Key,Value> deepSourceCopy, Iterator<Entry<Key,Document>> documents,
                    CompositeMetadata compositeMetadata) {
        Iterator<Tuple2<Key,Document>> parentDocuments = Iterators.transform(documents, new GetParentDocument(
        // no evaluation filter here as this is post evaluation
                        new KeyToDocumentData(deepSourceCopy, this.myEnvironment, this.documentOptions, this.equality, null, this.includeHierarchyFields,
                                        this.includeHierarchyFields), new Aggregation(this.getTimeFilter(), this.typeMetadataWithNonIndexed, compositeMetadata,
                                        this.isIncludeGroupingContext(), this.parentDisableIndexOnlyDocuments, null)));
        
        Iterator<Entry<Key,Document>> retDocuments = Iterators.transform(parentDocuments, new TupleToEntry<Key,Document>());
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
}
