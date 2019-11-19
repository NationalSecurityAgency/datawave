package datawave.query.iterator.builder;

import datawave.core.iterators.DatawaveFieldIndexFilterIteratorJexl;
import datawave.query.iterator.DocumentIterator;
import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.logic.DocumentAggregatingIterator;
import datawave.query.iterator.logic.IndexIteratorBridge;
import datawave.query.jexl.LiteralRange;
import datawave.query.predicate.Filter;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * A convenience class that aggregates a field, filter, range, source iterator, normalizer mappings, index only fields, data type filter and key transformer
 * when traversing a subtree in a query. This allows arbitrary ordering of the arguments.
 * 
 */
public class IndexFilterIteratorBuilder extends IvaratorBuilder implements IteratorBuilder {
    private static Logger log = Logger.getLogger(IndexFilterIteratorBuilder.class);
    
    protected LiteralRange range;
    protected Filter filter;
    
    public LiteralRange getRange() {
        return range;
    }
    
    public Filter getFilter() {
        return filter;
    }
    
    public void setRangeAndFunction(LiteralRange range, Filter filter) {
        this.range = range;
        this.filter = filter;
        setField(range.getFieldName());
        StringBuilder builder = new StringBuilder();
        builder.append(filter).append(":");
        builder.append(range.getLower()).append("-").append(range.getUpper());
        setValue(builder.toString());
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public NestedIterator<Key> build() {
        if (notNull(range, filter, source, datatypeFilter, keyTform, timeFilter, ivaratorCacheDirURI, hdfsFileSystem)) {
            if (log.isTraceEnabled()) {
                log.trace("Generating ivarator (caching field index iterator) for " + filter + " over " + range);
            }
            // get the hadoop file system and a temporary directory
            URI hdfsCacheURI = null;
            try {
                hdfsCacheURI = new URI(ivaratorCacheDirURI);
                hdfsFileSystem.mkdirs(new Path(hdfsCacheURI));
            } catch (MalformedURLException e) {
                throw new IllegalStateException("Unable to load hadoop configuration", e);
            } catch (IOException e) {
                throw new IllegalStateException("Unable to create hadoop file system", e);
            } catch (URISyntaxException e) {
                throw new IllegalStateException("Invalid hdfs cache dir URI: " + ivaratorCacheDirURI, e);
            }
            
            DocumentIterator docIterator = null;
            try {
                // create a field index caching ivarator
                // @formatter:off
                DatawaveFieldIndexFilterIteratorJexl rangeIterator = DatawaveFieldIndexFilterIteratorJexl.builder()
                        .withFieldName(new Text(range.getFieldName()))
                        .withFilter(filter)
                        .withLowerBound(range.getLower().toString())
                        .lowerInclusive(range.isLowerInclusive())
                        .withUpperBound(range.getUpper().toString())
                        .upperInclusive(range.isUpperInclusive())
                        .withTimeFilter(timeFilter)
                        .withDatatypeFilter(datatypeFilter)
                        .negated(false)
                        .withScanThreshold(ivaratorCacheScanPersistThreshold)
                        .withScanTimeout(ivaratorCacheScanTimeout)
                        .withHdfsBackedSetBufferSize(ivaratorCacheBufferSize)
                        .withMaxRangeSplit(maxRangeSplit)
                        .withMaxOpenFiles(ivaratorMaxOpenFiles)
                        .withMaxResults(maxIvaratorResults)
                        .withFileSystem(hdfsFileSystem)
                        .withUniqueDir(new Path(hdfsCacheURI))
                        .withQueryLock(queryLock)
                        .allowDirResuse(true)
                        .withReturnKeyType(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME)
                        .withSortedUUIDs(sortedUIDs)
                        .withCompositeMetadata(compositeMetadata)
                        .withCompositeSeekThreshold(compositeSeekThreshold)
                        .withTypeMetadata(typeMetadata)
                        .withIteratorEnv(env)
                        .build();
                // @formatter:on
                
                if (collectTimingDetails) {
                    rangeIterator.setCollectTimingDetails(true);
                    rangeIterator.setQuerySpanCollector(this.querySpanCollector);
                }
                rangeIterator.init(source, null, env);
                log.debug("Created a DatawaveFieldIndexFilterIteratorJexl: " + rangeIterator);
                
                // Add an interator to aggregate documents. This is needed for index only fields.
                DocumentAggregatingIterator aggregatingIterator = new DocumentAggregatingIterator(true, // this.fieldsToKeep == null ? false
                                                                                                        // :this.fieldsToKeep.contains(field),
                                this.typeMetadata, keyTform);
                aggregatingIterator.init(rangeIterator, null, null);
                
                docIterator = aggregatingIterator;
                
            } catch (IOException e) {
                throw new IllegalStateException("Unable to initialize regex iterator stack", e);
            }
            
            IndexIteratorBridge itr = new IndexIteratorBridge(docIterator);
            range = null;
            filter = null;
            source = null;
            timeFilter = null;
            datatypeFilter = null;
            keyTform = null;
            timeFilter = null;
            hdfsFileSystem = null;
            ivaratorCacheDirURI = null;
            return itr;
        } else {
            StringBuilder msg = new StringBuilder(256);
            msg.append("Cannot build iterator-- a field was null!\n");
            if (filter == null) {
                msg.append("\tFilter was null!\n");
            }
            if (range == null) {
                msg.append("\tRange was null!\n");
            }
            if (source == null) {
                msg.append("\tSource was null!\n");
            }
            msg.setLength(msg.length() - 1);
            throw new IllegalStateException(msg.toString());
        }
    }
}
