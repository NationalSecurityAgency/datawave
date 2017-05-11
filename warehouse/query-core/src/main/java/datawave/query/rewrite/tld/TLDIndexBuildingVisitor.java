package datawave.query.rewrite.tld;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;

import datawave.core.iterators.filesystem.FileSystemCache;
import datawave.core.iterators.querylock.QueryLock;
import datawave.query.rewrite.iterator.builder.NegationBuilder;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import datawave.query.rewrite.Constants;
import datawave.query.rewrite.iterator.SourceFactory;
import datawave.query.rewrite.iterator.builder.AbstractIteratorBuilder;
import datawave.query.rewrite.jexl.JexlASTHelper;
import datawave.query.rewrite.jexl.JexlASTHelper.IdentifierOpLiteral;
import datawave.query.rewrite.jexl.functions.FieldIndexAggregator;
import datawave.query.rewrite.jexl.visitors.IteratorBuildingVisitor;
import datawave.query.rewrite.predicate.EventDataQueryFilter;
import datawave.query.rewrite.predicate.TimeFilter;
import datawave.query.util.IteratorToSortedKeyValueIterator;
import datawave.query.util.TypeMetadata;

public class TLDIndexBuildingVisitor extends IteratorBuildingVisitor {
    private static final Logger log = Logger.getLogger(TLDIndexBuildingVisitor.class);
    
    public TLDIndexBuildingVisitor(SourceFactory<Key,Value> sourceFactory, IteratorEnvironment env, TimeFilter timeFilter, TypeMetadata typeMetadata,
                    Set<String> indexOnlyFields, Predicate<Key> datatypeFilter, FieldIndexAggregator fiAggregator, FileSystemCache fsCache,
                    QueryLock queryLock, List<String> hdfsCacheDirURIAlternatives, String queryId, String hdfsCacheSubDirPrefix,
                    String hdfsFileCompressionCodec, int hdfsCacheBufferSize, long hdfsCacheScanPersistThreshold, long hdfsCacheScanTimeout, int maxRangeSplit,
                    int ivaratorMaxOpenFiles, int maxIvaratorSources, Collection<String> includes, Collection<String> excludes,
                    Set<String> termFrequencyFields, boolean isQueryFullySatisfied, boolean sortedUIDs) {
        super(sourceFactory, env, timeFilter, typeMetadata, indexOnlyFields, datatypeFilter, fiAggregator, fsCache, queryLock, hdfsCacheDirURIAlternatives,
                        queryId, hdfsCacheSubDirPrefix, hdfsFileCompressionCodec, hdfsCacheBufferSize, hdfsCacheScanPersistThreshold, hdfsCacheScanTimeout,
                        maxRangeSplit, ivaratorMaxOpenFiles, maxIvaratorSources, includes, excludes, termFrequencyFields, isQueryFullySatisfied, sortedUIDs);
        setIteratorBuilder(TLDIndexIteratorBuilder.class);
    }
    
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
        builder.setIndexOnlyFields(indexOnlyFields);
        builder.setDatatypeFilter(datatypeFilter);
        builder.setKeyTransform(fiAggregator);
        builder.setTimeFilter(timeFilter);
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
            
            kvIter.seek(new Range(newStartKey, true, newStartKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL), false), Collections.<ByteSequence> emptyList(),
                            false);
            
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
            
            mySource.seek(new Range(newStartKey, true, newStartKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL), false), Collections.<ByteSequence> emptyList(),
                            false);
            
            if (mySource.hasTop()) {
                kv.add(Maps.immutableEntry(mySource.getTopKey(), Constants.NULL_VALUE));
                
            }
        }
        
        SortedKeyValueIterator<Key,Value> kvIter = new IteratorToSortedKeyValueIterator(kv.iterator());
        return kvIter;
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        TLDIndexIteratorBuilder builder = new TLDIndexIteratorBuilder();
        boolean isNegation = (null != data && data instanceof NegationBuilder);
        if (data instanceof AbstractIteratorBuilder) {
            AbstractIteratorBuilder oib = (AbstractIteratorBuilder) data;
            if (oib.isInANot()) {
                isNegation = true;
            }
        }
        builder.setSource(getSourceIterator(node, isNegation));
        builder.setTimeFilter(getTimeFilter(node));
        builder.setIndexOnlyFields(indexOnlyFields);
        builder.setDatatypeFilter(datatypeFilter);
        builder.setKeyTransform(fiAggregator);
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
    
}
