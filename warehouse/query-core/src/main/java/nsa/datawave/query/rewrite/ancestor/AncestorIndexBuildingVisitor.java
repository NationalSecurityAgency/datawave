package nsa.datawave.query.rewrite.ancestor;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import nsa.datawave.query.rewrite.iterator.SourceFactory;
import nsa.datawave.query.rewrite.jexl.functions.FieldIndexAggregator;
import nsa.datawave.query.rewrite.jexl.visitors.IteratorBuildingVisitor;
import nsa.datawave.query.rewrite.predicate.EventDataQueryFilter;
import nsa.datawave.query.rewrite.predicate.TimeFilter;
import nsa.datawave.query.util.TypeMetadata;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import com.google.common.base.Predicate;

public class AncestorIndexBuildingVisitor extends IteratorBuildingVisitor {
    private static final Logger log = Logger.getLogger(AncestorIndexBuildingVisitor.class);
    
    public AncestorIndexBuildingVisitor(SourceFactory<Key,Value> sourceFactory, IteratorEnvironment env,
                    TimeFilter timeFilter,
                    TypeMetadata typeMetadata,
                    Set<String> indexOnlyFields,
                    // EventDataQueryFilter attrFilter,
                    Predicate<Key> datatypeFilter, FieldIndexAggregator fiAggregator, FileSystem hadoopConfigUrl, String hdfsCacheDir,
                    List<String> hdfsCacheDirURIAlternatives, String hdfsCacheSubDirPrefix, String hdfsFileCompressionCodec, boolean hdfsCacheDirReused,
                    int hdfsCacheBufferSize, long hdfsCacheScanPersistThreshold, long hdfsCacheScanTimeout, int maxRangeSplit, int maxIvaratorSources,
                    Collection<String> includes, Collection<String> excludes, Set<String> termFrequencyFields, boolean isQueryFullySatisfied, boolean sortedUIDs) {
        super(sourceFactory, env, timeFilter, typeMetadata, indexOnlyFields, datatypeFilter, fiAggregator, hadoopConfigUrl, hdfsCacheDir,
                        hdfsCacheDirURIAlternatives, hdfsCacheSubDirPrefix, hdfsFileCompressionCodec, hdfsCacheDirReused, hdfsCacheBufferSize,
                        hdfsCacheScanPersistThreshold, hdfsCacheScanTimeout, maxRangeSplit, maxIvaratorSources, includes, excludes, termFrequencyFields,
                        isQueryFullySatisfied, sortedUIDs);
        setIteratorBuilder(AncestorIndexIteratorBuilder.class);
    }
}
