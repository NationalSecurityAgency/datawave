package datawave.query.iterator.builder;

import datawave.core.iterators.DatawaveFieldIndexRangeIteratorJexl;
import datawave.query.iterator.DocumentIterator;
import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.logic.DocumentAggregatingIterator;
import datawave.query.iterator.logic.IndexIteratorBridge;
import datawave.query.jexl.LiteralRange;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.SortedSet;

/**
 * A convenience class that aggregates a field, range, source iterator, normalizer mappings, index only fields, data type filter and key transformer when
 * traversing a subtree in a query. This allows arbitrary ordering of the arguments.
 * 
 */
public class IndexRangeIteratorBuilder extends IvaratorBuilder implements IteratorBuilder {
    private static Logger log = Logger.getLogger(IndexRangeIteratorBuilder.class);
    
    protected LiteralRange range;
    protected SortedSet<Range> subRanges;
    
    public LiteralRange getRange() {
        return range;
    }
    
    public void setRange(LiteralRange range) {
        this.range = range;
        setField(range.getFieldName());
        StringBuilder builder = new StringBuilder();
        builder.append(range.getLower()).append("-").append(range.getUpper());
        setValue(builder.toString());
    }
    
    public SortedSet<Range> getSubRanges() {
        return subRanges;
    }
    
    public void setSubRanges(SortedSet<Range> subRanges) {
        this.subRanges = subRanges;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public NestedIterator<Key> build() {
        if (notNull(range, source, datatypeFilter, keyTform, timeFilter, ivaratorCacheDirURI, hdfsFileSystem)) {
            if (log.isTraceEnabled()) {
                log.trace("Generating ivarator (caching field index iterator) for " + range);
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
                DatawaveFieldIndexRangeIteratorJexl rangeIterator = DatawaveFieldIndexRangeIteratorJexl.builder().withFieldName(new Text(range.getFieldName()))
                                .withLowerBound(range.getLower().toString()).lowerInclusive(range.isLowerInclusive())
                                .withUpperBound(range.getUpper().toString()).upperInclusive(range.isUpperInclusive()).withTimeFilter(this.timeFilter)
                                .withDatatypeFilter(this.datatypeFilter).negated(false).withScanThreshold(ivaratorCacheScanPersistThreshold)
                                .withScanTimeout(ivaratorCacheScanTimeout).withHdfsBackedSetBufferSize(ivaratorCacheBufferSize)
                                .withMaxRangeSplit(maxRangeSplit).withMaxOpenFiles(ivaratorMaxOpenFiles).withFileSystem(hdfsFileSystem)
                                .withUniqueDir(new Path(hdfsCacheURI)).withQueryLock(queryLock).allowDirResuse(true)
                                .withReturnKeyType(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME).withSortedUUIDs(sortedUIDs)
                                .withCompositeMetadata(compositeMetadata).withCompositeSeekThreshold(compositeSeekThreshold).withTypeMetadata(typeMetadata)
                                .withSubRanges(subRanges).build();
                
                if (collectTimingDetails) {
                    rangeIterator.setCollectTimingDetails(true);
                    rangeIterator.setQuerySpanCollector(this.querySpanCollector);
                }
                rangeIterator.init(source, null, null);
                log.debug("Created a DatawaveFieldIndexRangeIteratorJexl: " + rangeIterator);
                
                boolean canBuildDocument = this.fieldsToAggregate == null ? false : this.fieldsToAggregate.contains(field);
                if (forceDocumentBuild) {
                    canBuildDocument = true;
                }
                
                // Add an interator to aggregate documents. This is needed for index only fields.
                DocumentAggregatingIterator aggregatingIterator = new DocumentAggregatingIterator(canBuildDocument, this.typeMetadata, keyTform);
                aggregatingIterator.init(rangeIterator, null, null);
                
                docIterator = aggregatingIterator;
                
            } catch (IOException e) {
                throw new IllegalStateException("Unable to initialize regex iterator stack", e);
            }
            
            IndexIteratorBridge itr = new IndexIteratorBridge(docIterator);
            range = null;
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
