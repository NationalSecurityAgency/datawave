package datawave.query.tld;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import datawave.data.type.NoOpType;
import datawave.query.Constants;
import datawave.query.attributes.Document;
import datawave.query.data.parsers.DatawaveKey;
import datawave.query.iterator.builder.AbstractIteratorBuilder;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlASTHelper.IdentifierOpLiteral;
import datawave.query.jexl.LiteralRange;
import datawave.query.jexl.functions.EventFieldAggregator;
import datawave.query.jexl.functions.TLDEventFieldAggregator;
import datawave.query.jexl.functions.TermFrequencyAggregator;
import datawave.query.jexl.visitors.IteratorBuildingVisitor;
import datawave.query.predicate.ChainableEventDataQueryFilter;
import datawave.query.predicate.EventDataQueryFilter;
import datawave.query.predicate.TLDEventDataFilter;
import datawave.query.util.IteratorToSortedKeyValueIterator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

public class TLDIndexBuildingVisitor extends IteratorBuildingVisitor {
    private static final Logger log = Logger.getLogger(TLDIndexBuildingVisitor.class);
    
    @Override
    public Object visit(ASTNENode node, Object data) {
        // We have no parent already defined
        if (data == null) {
            // We don't support querying only on a negation
            throw new IllegalStateException("Root node cannot be a negation");
        }
        TLDIndexIteratorBuilder builder = new TLDIndexIteratorBuilder();
        builder.setSource(source.deepCopy(env));
        builder.setTypeMetadata(typeMetadata);
        builder.setDatatypeFilter(datatypeFilter);
        builder.setFieldsToAggregate(fieldsToAggregate);
        builder.setKeyTransform(fiAggregator);
        builder.setTimeFilter(timeFilter);
        builder.setNode(node);
        node.childrenAccept(this, builder);
        
        // A EQNode may be of the form FIELD == null. The evaluation can
        // handle this, so we should just not build an IndexIterator for it.
        if (null == builder.getValue()) {
            if (isQueryFullySatisfied == true) {
                throw new RuntimeException("Determined that isQueryFullySatisfied should be false, but it was not preset to false by the SatisfactionVisitor");
            }
            return null;
        }
        
        AbstractIteratorBuilder iterators = (AbstractIteratorBuilder) data;
        // Add the negated IndexIteratorBuilder to the parent as an *exclude*
        if (!iterators.hasSeen(builder.getField(), builder.getValue()) && includeReferences.contains(builder.getField())
                        && !excludeReferences.contains(builder.getField())) {
            iterators.addExclude(builder.build());
        } else {
            if (isQueryFullySatisfied == true) {
                throw new RuntimeException("Determined that isQueryFullySatisfied should be false, but it was not preset to false by the SatisfactionVisitor");
            }
        }
        
        return null;
    }
    
    /**
     * @param kvIter
     * @param node
     * @throws IOException
     */
    @Override
    protected void seekIndexOnlyDocument(SortedKeyValueIterator<Key,Value> kvIter, ASTEQNode node) throws IOException {
        if (null != rangeLimiter && limitLookup) {
            
            Key newStartKey = getKey(node);
            
            kvIter.seek(new Range(newStartKey, true, newStartKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL), false), Collections.emptyList(), false);
            
        }
    }
    
    @Override
    protected SortedKeyValueIterator<Key,Value> createIndexOnlyKey(ASTEQNode node) throws IOException {
        Key newStartKey = getKey(node);
        
        IdentifierOpLiteral op = JexlASTHelper.getIdentifierOpLiteral(node);
        if (null == op || null == op.getLiteralValue()) {
            // deep copy since this is likely a null literal
            return source.deepCopy(env);
        }
        
        String fn = op.deconstructIdentifier();
        String literal = String.valueOf(op.getLiteralValue());
        
        if (log.isTraceEnabled()) {
            log.trace("createIndexOnlyKey for " + fn + " " + literal + " " + newStartKey);
        }
        List<Entry<Key,Value>> kv = Lists.newArrayList();
        if (null != limitedMap.get(Maps.immutableEntry(fn, literal))) {
            kv.add(limitedMap.get(Maps.immutableEntry(fn, literal)));
        } else {
            
            SortedKeyValueIterator<Key,Value> mySource = limitedSource;
            // if source size > 0, we are free to use up to that number for this query
            if (source.getSourceSize() > 0)
                mySource = source.deepCopy(env);
            
            mySource.seek(new Range(newStartKey, true, newStartKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL), false), Collections.emptyList(), false);
            
            if (mySource.hasTop()) {
                kv.add(Maps.immutableEntry(mySource.getTopKey(), Constants.NULL_VALUE));
                
            }
        }
        
        return new IteratorToSortedKeyValueIterator(kv.iterator());
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        TLDIndexIteratorBuilder builder = new TLDIndexIteratorBuilder();
        boolean isNegation = false;
        if (data instanceof AbstractIteratorBuilder) {
            AbstractIteratorBuilder oib = (AbstractIteratorBuilder) data;
            isNegation = oib.isInANot();
        }
        builder.setSource(getSourceIterator(node, isNegation));
        builder.setTimeFilter(getTimeFilter(node));
        builder.setTypeMetadata(typeMetadata);
        builder.setDatatypeFilter(datatypeFilter);
        builder.setFieldsToAggregate(fieldsToAggregate);
        builder.setKeyTransform(fiAggregator);
        builder.forceDocumentBuild(!limitLookup && this.isQueryFullySatisfied);
        builder.setNode(node);
        node.childrenAccept(this, builder);
        
        // A EQNode may be of the form FIELD == null. The evaluation can
        // handle this, so we should just not build an IndexIterator for it.
        if (null == builder.getValue()) {
            if (isQueryFullySatisfied == true) {
                throw new RuntimeException("Determined that isQueryFullySatisfied should be false, but it was not preset to false by the SatisfactionVisitor");
            }
            return null;
        }
        
        // We have no parent already defined
        if (data == null) {
            // Make this EQNode the root
            if (!includeReferences.contains(builder.getField()) && excludeReferences.contains(builder.getField())) {
                throw new IllegalStateException(builder.getField() + " is a blacklisted reference.");
            } else {
                root = builder.build();
                
                if (log.isTraceEnabled()) {
                    log.trace("Build IndexIterator: " + root);
                }
            }
        } else {
            AbstractIteratorBuilder iterators = (AbstractIteratorBuilder) data;
            // Add this IndexIterator to the parent
            if (!iterators.hasSeen(builder.getField(), builder.getValue()) && includeReferences.contains(builder.getField())
                            && !excludeReferences.contains(builder.getField())) {
                iterators.addInclude(builder.build());
            } else {
                if (isQueryFullySatisfied == true) {
                    throw new RuntimeException(
                                    "Determined that isQueryFullySatisfied should be false, but it was not preset to false by the SatisfactionVisitor");
                }
            }
        }
        
        return null;
    }
    
    @Override
    protected EventFieldAggregator getEventFieldAggregator(String field, ChainableEventDataQueryFilter filter) {
        return new TLDEventFieldAggregator(field, filter, attrFilter != null ? attrFilter.getMaxNextCount() : -1, typeMetadata, NoOpType.class.getName());
    }
    
    /**
     * Use fieldsToAggregate instead of indexOnlyFields because this enables TLDs to return non-event tokens as part of the user document
     * 
     * @param filter
     * @param maxNextCount
     * @return
     */
    @Override
    protected TermFrequencyAggregator buildTermFrequencyAggregator(String identifier, ChainableEventDataQueryFilter filter, int maxNextCount) {
        EventDataQueryFilter rootFilter = new EventDataQueryFilter() {
            @Override
            public void startNewDocument(Key documentKey) {
                // no-op
            }
            
            @Override
            public boolean apply(@Nullable Entry<Key,String> var1) {
                // accept all
                return true;
            }
            
            @Override
            public boolean peek(@Nullable Entry<Key,String> var1) {
                // accept all
                return true;
            }
            
            /**
             * Only keep the tf key if it isn't the root pointer or if it is index only and contributes to document evaluation
             * 
             * @param k
             * @return
             */
            @Override
            public boolean keep(Key k) {
                if (TLDEventDataFilter.isRootPointer(k)) {
                    DatawaveKey key = new DatawaveKey(k);
                    if (!indexOnlyFields.contains(key.getFieldName())) {
                        return false;
                    }
                }
                
                return attrFilter.peek(new AbstractMap.SimpleEntry(k, null));
            }
            
            @Override
            public Key getStartKey(Key from) {
                throw new UnsupportedOperationException();
            }
            
            @Override
            public Key getStopKey(Key from) {
                throw new UnsupportedOperationException();
            }
            
            @Override
            public Range getKeyRange(Entry<Key,Document> from) {
                throw new UnsupportedOperationException();
            }
            
            @Override
            public EventDataQueryFilter clone() {
                return this;
            }
            
            @Override
            public Range getSeekRange(Key current, Key endKey, boolean endKeyInclusive) {
                throw new UnsupportedOperationException();
            }
            
            @Override
            public int getMaxNextCount() {
                return -1;
            }
            
            @Override
            public Key transform(Key toTransform) {
                throw new UnsupportedOperationException();
            }
        };
        filter.addFilter(rootFilter);
        
        Set<String> toAggregate = fieldsToAggregate.contains(identifier) ? Collections.singleton(identifier) : Collections.emptySet();
        
        return new TLDTermFrequencyAggregator(toAggregate, filter, filter.getMaxNextCount());
    }
    
    /**
     * Range should be build to encompass the entire TLD
     * 
     * @param range
     *            non-null literal range to generate an FI range from
     * @return
     */
    @Override
    protected Range getFiRangeForTF(LiteralRange<?> range) {
        Key startKey = rangeLimiter.getStartKey();
        
        StringBuilder strBuilder = new StringBuilder("fi");
        strBuilder.append(NULL_DELIMETER).append(range.getFieldName());
        Text cf = new Text(strBuilder.toString());
        
        strBuilder = new StringBuilder(range.getLower().toString());
        
        strBuilder.append(NULL_DELIMETER).append(startKey.getColumnFamily());
        Text cq = new Text(strBuilder.toString());
        
        Key seekBeginKey = new Key(startKey.getRow(), cf, cq, startKey.getTimestamp());
        
        strBuilder = new StringBuilder(range.getUpper().toString());
        
        strBuilder.append(NULL_DELIMETER).append(startKey.getColumnFamily() + Constants.MAX_UNICODE_STRING);
        cq = new Text(strBuilder.toString());
        
        Key seekEndKey = new Key(startKey.getRow(), cf, cq, startKey.getTimestamp());
        
        return new Range(seekBeginKey, true, seekEndKey, true);
    }
}
