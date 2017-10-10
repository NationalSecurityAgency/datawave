package nsa.datawave.query.rewrite.iterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import nsa.datawave.query.rewrite.attributes.Attribute;
import nsa.datawave.query.rewrite.attributes.Document;
import nsa.datawave.query.rewrite.function.Aggregation;
import nsa.datawave.query.rewrite.function.KeyToDocumentData;
import nsa.datawave.query.rewrite.predicate.EventDataQueryFilter;
import nsa.datawave.query.rewrite.predicate.ParentEventDataFilter;
import nsa.datawave.query.util.CompositeMetadata;
import nsa.datawave.query.util.Tuple2;
import nsa.datawave.query.util.TupleToEntry;

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
        if (evaluationFilter == null && script != null) {
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
                                        this.isIncludeGroupingContext(), this.includeRecordId, this.parentDisableIndexOnlyDocuments, null)));
        
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
