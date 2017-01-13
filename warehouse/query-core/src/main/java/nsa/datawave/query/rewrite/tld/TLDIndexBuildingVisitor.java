package nsa.datawave.query.rewrite.tld;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import nsa.datawave.query.rewrite.iterator.builder.NegationBuilder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import com.google.common.base.Predicate;

import nsa.datawave.query.rewrite.iterator.SourceFactory;
import nsa.datawave.query.rewrite.iterator.builder.AbstractIteratorBuilder;
import nsa.datawave.query.rewrite.jexl.functions.FieldIndexAggregator;
import nsa.datawave.query.rewrite.jexl.visitors.IteratorBuildingVisitor;
import nsa.datawave.query.rewrite.predicate.EventDataQueryFilter;
import nsa.datawave.query.rewrite.predicate.TimeFilter;
import nsa.datawave.query.util.TypeMetadata;

public class TLDIndexBuildingVisitor extends IteratorBuildingVisitor {
    private static final Logger log = Logger.getLogger(TLDIndexBuildingVisitor.class);
    
    public TLDIndexBuildingVisitor(SourceFactory<Key,Value> sourceFactory, IteratorEnvironment env, TimeFilter timeFilter, TypeMetadata typeMetadata,
                    Set<String> indexOnlyFields, Predicate<Key> datatypeFilter, FieldIndexAggregator fiAggregator, FileSystem hdfsFileSystem,
                    String hdfsCacheDir, List<String> hdfsCacheDirURIAlternatives, String hdfsCacheSubDirPrefix, String hdfsFileCompressionCodec,
                    boolean hdfsCacheDirReused, int hdfsCacheBufferSize, long hdfsCacheScanPersistThreshold, long hdfsCacheScanTimeout, int maxRangeSplit,
                    int maxIvaratorSources, Collection<String> includes, Collection<String> excludes, Set<String> termFrequencyFields,
                    boolean isQueryFullySatisfied, boolean sortedUIDs) {
        super(sourceFactory, env, timeFilter, typeMetadata, indexOnlyFields, datatypeFilter, fiAggregator, hdfsFileSystem, hdfsCacheDir,
                        hdfsCacheDirURIAlternatives, hdfsCacheSubDirPrefix, hdfsFileCompressionCodec, hdfsCacheDirReused, hdfsCacheBufferSize,
                        hdfsCacheScanPersistThreshold, hdfsCacheScanTimeout, maxRangeSplit, maxIvaratorSources, includes, excludes, termFrequencyFields,
                        isQueryFullySatisfied, sortedUIDs);
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
